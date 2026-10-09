//! `SR` §3.2: writes at a STATED entry timestamp — the namespace copy keeps the
//! timestamp its row had in the old namespace.

use guardian_db::guardian::error::GuardianError;
use guardian_db::p2p::network::core::docs::WillowDocs;
use guardian_db::traits::{DocumentStore, Placed};
use iroh_docs::DocTicket;
use iroh_docs::store::Query;
mod common;

use common::TestNode;
use serde_json::json;

/// The store's own iroh-docs document, opened a second time over the same
/// backend — the same engine, so a write here lands in the very document
/// the store reads (as in `doc_index_refresh.rs`).
async fn own_document(
    node: &TestNode,
    docs: &std::sync::Arc<dyn DocumentStore<Error = GuardianError>>,
) -> (iroh_docs::api::Doc, iroh_docs::AuthorId) {
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
    (doc, author)
}

fn row(id: &str, v: &str) -> guardian_db::traits::Document {
    Box::new(json!({ "_id": id, "v": v }))
}

/// `(_id, v, entry timestamp)` under `prefix`, sorted.
async fn stamped(
    docs: &std::sync::Arc<dyn DocumentStore<Error = GuardianError>>,
    prefix: &str,
) -> Vec<(String, String, u64)> {
    let mut out: Vec<_> = docs
        .scan_docs_stamped(prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|(d, ts)| {
            let v = d.downcast::<serde_json::Value>().unwrap();
            (
                v["_id"].as_str().unwrap().to_owned(),
                v["v"].as_str().unwrap_or_default().to_owned(),
                ts,
            )
        })
        .collect();
    out.sort();
    out
}

/// Exactly the stated timestamp, zero included (the claim without a handle,
/// `SR` §3.2), and the index serves both rows at once.
#[tokio::test]
async fn put_at_writes_exactly_the_stated_timestamp_and_zero_is_accepted() {
    let node = TestNode::new("stamped-exact").await.unwrap();
    let docs = node.db.docs("stamped-exact", None).await.unwrap();
    assert_eq!(docs.put_at(row("k/a", "a"), 1_000_000).await.unwrap(), Placed::Written);
    assert_eq!(docs.put_at(row("k/b", "b"), 0).await.unwrap(), Placed::Written);
    assert_eq!(
        stamped(&docs, "k/").await,
        vec![
            ("k/a".to_owned(), "a".to_owned(), 1_000_000),
            ("k/b".to_owned(), "b".to_owned(), 0)
        ]
    );
    assert!(docs.index().get_bytes("k/a").unwrap().is_some());
    assert!(docs.index().get_bytes("k/b").unwrap().is_some());
}

/// Across authors the newest entry wins: an old copy by this node's author
/// is written, and loses to the newer row another author holds.
#[tokio::test]
async fn an_older_copy_loses_to_a_newer_row_of_another_author() {
    let node = TestNode::new("stamped-two-authors").await.unwrap();
    let docs = node.db.docs("stamped-two-authors", None).await.unwrap();
    let (doc, mine) = own_document(&node, &docs).await;
    let willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();
    let other = willow.docs().author_create().await.unwrap();
    willow
        .set_bytes(&doc, other, b"k/x".to_vec(), b"newer".to_vec())
        .await
        .unwrap();
    let own_node = *node.iroh.node_id().as_bytes();
    let placed = willow
        .set_bytes_at(&doc, mine, b"k/x".to_vec(), b"older".to_vec(), 1, own_node)
        .await
        .unwrap();
    assert_eq!(placed, Placed::Written, "another author's row never rejects it");
    let latest = willow
        .get_many(&doc, Query::single_latest_per_key().key_exact(b"k/x"))
        .await
        .unwrap();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].author(), other, "the newer row wins across authors");
}

/// The own author's newer deletion marker outranks an old copy: `Superseded`,
/// not an error — the newer state in the target is the right one.
#[tokio::test]
async fn a_copy_older_than_an_own_deletion_marker_is_superseded_not_an_error() {
    let node = TestNode::new("stamped-superseded").await.unwrap();
    let docs = node.db.docs("stamped-superseded", None).await.unwrap();
    docs.put(row("k/gone", "first")).await.unwrap();
    docs.delete("k/gone").await.unwrap();
    assert_eq!(docs.put_at(row("k/gone", "copy"), 1).await.unwrap(), Placed::Superseded);
    assert!(stamped(&docs, "k/").await.is_empty(), "the newer deletion stands");
    assert!(docs.index().get_bytes("k/gone").unwrap().is_none());
}

/// Prefix deletion (Vorprüfung 2): an entry removes the same author's OLDER
/// entries under its key — an old timestamp removes less, never more.
#[tokio::test]
async fn an_old_timestamp_deletes_less_under_its_prefix_never_more() {
    let node = TestNode::new("stamped-prefix").await.unwrap();
    let docs = node.db.docs("stamped-prefix", None).await.unwrap();
    assert_eq!(docs.put_at(row("p/x", "old"), 5).await.unwrap(), Placed::Written);
    docs.put(row("p/y", "fresh")).await.unwrap();
    assert_eq!(docs.put_at(row("p/", "parent"), 10).await.unwrap(), Placed::Written);
    let keys: Vec<String> = stamped(&docs, "p/").await.into_iter().map(|(k, _, _)| k).collect();
    assert_eq!(keys, vec!["p/".to_owned(), "p/y".to_owned()], "p/x (5) went, p/y (now) stayed");
}
