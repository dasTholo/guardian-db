//! The index refresh through its CALLER: `Store::load` is
//! `sync_index_from_docs`, which is `refresh_doc_index`, so these gates run
//! the `get_many`, the mapping of its entries and the loop in one piece.
//!
//! `LI §9` is why this file exists next to the unit gates in
//! `document_store/mod.rs`: there, a building block was held and its caller
//! was not, and a mutation in the caller left the whole suite green. The
//! unit gates hold the loop mid-flight; these hold what reaches it.

use guardian_db::p2p::network::core::docs::WillowDocs;
use iroh_blobs::Hash;
use iroh_docs::DocTicket;
mod common;

use common::TestNode;
use serde_json::json;

/// The store's own iroh-docs document, opened a second time over the same
/// backend — the same engine, so a write here lands in the very document
/// the store reads (`doc_read_after_write.rs` builds its ghost entry the
/// same way).
async fn own_document(
    node: &TestNode,
    docs: &std::sync::Arc<
        dyn guardian_db::traits::DocumentStore<Error = guardian_db::guardian::error::GuardianError>,
    >,
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

#[tokio::test]
async fn a_newer_entry_whose_blob_is_missing_keeps_the_old_value_in_the_index() {
    let node = TestNode::new("refresh-missing-blob").await.unwrap();
    let docs = node.db.docs("refresh-missing-blob", None).await.unwrap();

    docs.put(Box::new(json!({ "_id": "rev/de/aaa", "name": "Alice" })))
        .await
        .unwrap();
    let index = docs.index();
    assert!(
        index.get_bytes("rev/de/aaa").unwrap().is_some(),
        "green first: the local write is in the index"
    );

    // A NEWER entry for the same key with its blob absent — the state a
    // peer's entry is in between the sync that carried the key and the
    // transfer that carries its content, and the state F3/F4 read through
    // after a restart.
    let (doc, author) = own_document(&node, &docs).await;
    let never_stored = b"a newer payload this node was never sent";
    doc.set_hash(
        author,
        b"rev/de/aaa".to_vec(),
        Hash::new(never_stored),
        never_stored.len() as u64,
    )
    .await
    .unwrap();

    docs.load(0).await.expect("the refresh steps over the fetch");

    let held = index
        .get_bytes("rev/de/aaa")
        .unwrap()
        .expect("the key stays in the index: a failed fetch is not a deletion");
    assert!(
        String::from_utf8_lossy(&held).contains("Alice"),
        "and it holds the value it held before — stale until the blob \
         arrives, but never absent"
    );
}

#[tokio::test]
async fn a_key_deleted_in_the_document_leaves_the_index_on_refresh() {
    let node = TestNode::new("refresh-deleted-key").await.unwrap();
    let docs = node.db.docs("refresh-deleted-key", None).await.unwrap();

    docs.put(Box::new(json!({ "_id": "rev/de/aaa", "name": "Alice" })))
        .await
        .unwrap();
    docs.put(Box::new(json!({ "_id": "rev/de/bbb", "name": "Bob" })))
        .await
        .unwrap();

    // Deleted in the DOCUMENT and not through the store, so the index
    // learns it only from the refresh — which is how a peer's deletion
    // reaches it.
    let (doc, author) = own_document(&node, &docs).await;
    doc.del(author, b"rev/de/aaa".to_vec()).await.unwrap();

    let index = docs.index();
    assert!(
        index.get_bytes("rev/de/aaa").unwrap().is_some(),
        "green first: before the refresh the index still holds it"
    );

    docs.load(0).await.unwrap();

    assert!(
        index.get_bytes("rev/de/aaa").unwrap().is_none(),
        "without a clear, the refresh itself has to remove a deleted key"
    );
    assert!(
        index.get_bytes("rev/de/bbb").unwrap().is_some(),
        "and only that one"
    );
}
