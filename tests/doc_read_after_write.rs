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
use iroh_blobs::Hash;
use iroh_docs::DocTicket;
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
    //
    // `L7`: "puts this very index into" held until `LN` Task 25 (8); the
    // refresh no longer clears (`tests/doc_index_refresh.rs`). The contrast
    // still holds for an index that IS empty — a fresh store before its
    // first refresh, or `StoreIndex::clear` as below.
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

/// `AS L4` — the gate the constraint went without: `scan_docs` ABORTS on an
/// entry that the index build only warns about and steps over.
///
/// WHICH BRANCH THIS HOLDS, and which it does not. `scan_docs` can fail on a
/// single entry in three places, in this order: the blob fetch (`cat_bytes`),
/// the payload codec (`decode_value`), and the JSON parse. This test holds the
/// FIRST, and it holds that one because it is the branch the constraint is
/// worded around: `refresh_doc_index` has exactly ONE `warn!`-and-skip and it
/// sits on the very same `cat_bytes`, so both paths can be pointed at ONE
/// entry in ONE document and nothing moves between them but the method asked.
///
/// THE OTHER TWO STAY UNGATED, and this says so rather than letting the name
/// imply otherwise. `decode_value` cannot fail under the identity codec every
/// store in this file runs on, and handing this store a real codec would take
/// the contrast away with it: the wrapper's `query` feeds index bytes straight
/// to `serde_json` without ever asking the codec, so over an encoded index it
/// answers nothing at all and the "and it still answers" half of the statement
/// collapses. An honest hole is worth more than a test whose name outruns what
/// it measures.
#[tokio::test]
async fn scan_docs_fails_on_the_entry_the_index_build_only_warns_about() {
    let node = TestNode::new("scan-docs-unfetchable").await.unwrap();
    let docs = node.db.docs("scan-docs-unfetchable", None).await.unwrap();

    docs.put(Box::new(json!({ "_id": "rev/de/aaa", "name": "Alice" })))
        .await
        .unwrap();

    // Green first, so every failure below is a change and not the state the
    // fixture started in.
    assert_eq!(
        docs.scan_docs("rev/de/").await.unwrap().len(),
        1,
        "the prefix selects the one header that is there and readable"
    );

    // THE UNREADABLE ENTRY, by hand. The store's own iroh-docs document is
    // reachable through the ticket it hands out, and a `WillowDocs` over the
    // same backend is the same engine — so this writes into the very document
    // the store reads, and not into a lookalike.
    let ticket = docs
        .share_ticket()
        .await
        .unwrap()
        .parse::<DocTicket>()
        .unwrap();
    let mut willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();
    let author = willow.get_or_init_author().await.unwrap();
    let doc = willow
        .open_doc(ticket.capability.id())
        .await
        .unwrap()
        .expect("the store's own document, opened a second time");

    // `set_hash` and not `set_bytes`: it writes the ENTRY and leaves the blob
    // behind it absent, which is the state a peer's entry is in between the
    // sync that carried the key and the transfer that carries its content.
    // That is the case `refresh_doc_index` keeps its `warn!` for, built here
    // as a state and not as a race. The size is non-zero on purpose: a
    // zero-length entry is a deletion marker, which both paths skip by design.
    let never_stored = b"a payload this node was never sent";
    doc.set_hash(
        author,
        b"rev/de/ghost".to_vec(),
        Hash::new(never_stored),
        never_stored.len() as u64,
    )
    .await
    .unwrap();

    // THE TWO PATHS SIDE BY SIDE over that one entry.
    assert!(
        docs.scan_docs("rev/de/").await.is_err(),
        "scan_docs must FAIL on an entry it cannot fetch: a skipped entry \
         would be an invisible ancestor, and that is the same fork this read \
         exists to prevent, only quieter (`AS L4`)"
    );

    // `Store::load` IS `sync_index_from_docs`, which is `refresh_doc_index`:
    // the index build walks the same two entries and does not fail over the
    // second one.
    docs.load(0)
        .await
        .expect("the index build steps over the entry that stopped scan_docs");

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
        1,
        "and the index path keeps answering afterwards — with the entry it \
         could read and without the one it could not, which is exactly the \
         leniency scan_docs refuses"
    );
}
