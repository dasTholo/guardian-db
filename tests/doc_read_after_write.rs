//! `AS P0` — die Weiche des Zyklus: sieht ein Präfix-Query der Doc einen
//! Schlüssel, den `set_bytes` unmittelbar davor geschrieben hat?
//!
//! No store and no index anywhere in here. `GuardianDBDocumentStore` keeps an
//! in-memory index over this document and rebuilds it on every remote event;
//! the question this file answers is what the DOCUMENT knows, because that is
//! the thing that has no rebuild.

use guardian_db::p2p::network::client::IrohClient;
use guardian_db::p2p::network::config::ClientConfig;
use guardian_db::p2p::network::core::docs::WillowDocs;
use iroh_docs::store::Query;
use tempfile::TempDir;
mod common;

use common::TestNode;
use guardian_db::traits::AsyncDocumentFilter;
use serde_json::json;

#[tokio::test]
async fn a_key_written_to_the_doc_is_visible_to_a_prefix_query_at_once() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ClientConfig::testing();
    cfg.data_store_path = Some(dir.path().join("iroh"));
    cfg.port = 0;
    let client = IrohClient::new(cfg).await.unwrap();
    let mut docs = WillowDocs::new(client.backend().clone()).await.unwrap();
    let author = docs.get_or_init_author().await.unwrap();
    let doc = docs.create_doc().await.unwrap();

    // TWO keys, and the second one is the point of `R1`: it shares no
    // prefix with the first, so a filter that does not filter is visible
    // as a count of two.
    docs.set_bytes(&doc, author, b"rev/de/aaa".to_vec(), b"A".to_vec())
        .await
        .unwrap();
    docs.set_bytes(&doc, author, b"payload/de/aaa".to_vec(), b"P".to_vec())
        .await
        .unwrap();

    // NO sleep, no `eventually`: "immediately after the write" IS the
    // question. A poll loop here would answer a different one.
    let entries = docs
        .get_many(
            &doc,
            Query::single_latest_per_key().key_prefix("rev/de/").build(),
        )
        .await
        .unwrap();

    assert_eq!(
        entries.len(),
        1,
        "the write is visible to the doc at once, and the prefix keeps the \
         other key out — got {:?}",
        entries
            .iter()
            .map(|e| String::from_utf8_lossy(e.key()).to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        String::from_utf8_lossy(entries[0].key()),
        "rev/de/aaa",
        "and it is the key that was written under the prefix"
    );
    assert!(
        entries[0].content_len() > 0,
        "with content behind it, so a reader has a hash to fetch"
    );
}

#[tokio::test]
async fn scan_docs_reads_the_doc_where_query_reads_an_index_that_was_just_cleared() {
    // THE TWO PATHS SIDE BY SIDE, over one store and one write, so no
    // fixture difference carries the statement: only WHICH method is asked
    // moves. The cleared index is not a contrivance — it is the state
    // `refresh_doc_index` puts this very index into on every remote event,
    // built by hand here so it is a state and not a race.
    let node = TestNode::new("scan-docs-node").await.unwrap();
    let docs = node.db.docs("scan-docs", None).await.unwrap();

    docs.put(Box::new(json!({ "_id": "rev/de/aaa", "name": "Alice" })))
        .await
        .unwrap();
    docs.put(Box::new(json!({ "_id": "payload/de/aaa", "body": "P" })))
        .await
        .unwrap();

    // Green first: with the index intact both paths answer, so every zero
    // below is a change and not the state the fixture started in.
    assert_eq!(
        docs.scan_docs("rev/de/").await.unwrap().len(),
        1,
        "the prefix selects the one header and leaves the payload key out"
    );

    // THE WINDOW, by hand.
    let mut index = docs.index();
    index.clear().unwrap();

    // The long form the fork's own suite uses
    // (`tests/integration_persistence.rs:235-244`): the closure's return
    // type does not infer through `Pin<Box<dyn Future<…>>>` on its own.
    let filter: AsyncDocumentFilter = Box::pin(|_doc| {
        Box::pin(async move { Ok(true) })
            as std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<bool, Box<dyn std::error::Error + Send + Sync>>,
                        > + Send,
                >,
            >
    });
    assert_eq!(
        docs.query(filter).await.unwrap().len(),
        0,
        "the index path answers NOTHING while the index is empty — and this \
         zero is indistinguishable from an empty namespace, which is the \
         whole defect"
    );
    assert_eq!(
        docs.scan_docs("rev/de/").await.unwrap().len(),
        1,
        "the doc path answers the SAME document, because the document never \
         lost it"
    );
}
