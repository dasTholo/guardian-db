//! The index refresh through its CALLER: `Store::load` is
//! `sync_index_from_docs`, which is `refresh_doc_index`, so these gates run
//! the `get_many`, the mapping of its entries and the loop in one piece.
//!
//! `LI §9` is why this file exists next to the unit gates in
//! `document_store/mod.rs`: there, a building block was held and its caller
//! was not, and a mutation in the caller left the whole suite green. The
//! unit gates hold the loop mid-flight; these hold what reaches it.

use guardian_db::p2p::network::core::docs::WillowDocs;
use guardian_db::stores::document_store::IMG_PREFIX;
use iroh_blobs::Hash;
use iroh_docs::DocTicket;
use iroh_docs::store::Query;
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
async fn a_raw_scan_hands_back_the_stored_bytes_and_the_key() {
    let node = TestNode::new("raw-scan-bytes").await.unwrap();
    let docs = node.db.docs("raw-scan-bytes", None).await.unwrap();

    let document = json!({ "_id": "rev/de/aaa", "name": "Alice" });
    docs.put(Box::new(document.clone())).await.unwrap();

    let rows = docs.scan_raw("rev/").await.unwrap();
    assert_eq!(rows.len(), 1, "one entry under the prefix: {rows:?}");
    assert_eq!(rows[0].key, "rev/de/aaa", "the key is the document's `_id`");
    assert_eq!(
        rows[0].bytes,
        serde_json::to_vec(&document).unwrap(),
        "the plaintext bytes as stored, parsed by nobody"
    );
    assert!(rows[0].timestamp > 0, "the entry's timestamp travels with it");
}

#[tokio::test]
async fn a_raw_write_reads_back_byte_for_byte() {
    // `DR` §6 K2, N9: bytes at a key, no marshal and no key read out of a
    // document — and under `--features encryption` the store's payload
    // codec applied on the way in and taken off on the way out.
    let node = TestNode::new("raw-write-bytes").await.unwrap();
    let docs = node.db.docs("raw-write-bytes", None).await.unwrap();

    let placed = docs.put_raw_at("k/1", vec![1, 0, 255], None).await.unwrap();
    assert_eq!(placed, guardian_db::traits::Placed::Written);
    let rows = docs.scan_raw("k/").await.unwrap();
    assert_eq!(rows.len(), 1, "one entry under the prefix: {rows:?}");
    assert_eq!(rows[0].key, "k/1", "the key as given");
    assert_eq!(rows[0].bytes, vec![1, 0, 255], "the bytes as given");

    let t = rows[0].timestamp - 1_000;
    let placed = docs.put_raw_at("k/2", vec![7], Some(t)).await.unwrap();
    assert_eq!(placed, guardian_db::traits::Placed::Written);
    let rows = docs.scan_raw("k/2").await.unwrap();
    assert_eq!(rows.len(), 1, "one entry at the key: {rows:?}");
    assert_eq!(rows[0].bytes, vec![7]);
    assert_eq!(rows[0].timestamp, t, "a stated timestamp is kept");
}

#[tokio::test]
async fn a_lossy_raw_scan_names_a_key_whose_blob_is_missing() {
    let node = TestNode::new("raw-scan-missing-blob").await.unwrap();
    let docs = node.db.docs("raw-scan-missing-blob", None).await.unwrap();

    docs.put(Box::new(json!({ "_id": "rev/de/aaa", "name": "Alice" })))
        .await
        .unwrap();

    // The missing-blob setup of the neighbour above: a NEWER entry for the
    // key whose content this node was never sent.
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

    assert!(
        docs.scan_raw("rev/").await.is_err(),
        "the strict raw scan fails on a blob that is not local"
    );
    let (rows, missing) = docs.scan_raw_lossy("rev/").await.unwrap();
    assert!(rows.is_empty(), "no row for the skipped key: {rows:?}");
    assert_eq!(missing, vec!["rev/de/aaa".to_string()], "the key is named");
}

#[tokio::test]
async fn keys_under_reads_no_blob() {
    // `DR` §8 S: the arrival of revisions is noted by their keys — an entry
    // whose blob is not local still lists its key, a deleted key does not.
    let node = TestNode::new("keys-under-no-blob").await.unwrap();
    let docs = node.db.docs("keys-under-no-blob", None).await.unwrap();

    docs.put_raw_at("rev/de/aaa", vec![1], None).await.unwrap();
    docs.put_raw_at("rev/de/bbb", vec![2], None).await.unwrap();
    docs.put_raw_at("other/ccc", vec![3], None).await.unwrap();

    let (doc, author) = own_document(&node, &docs).await;
    let never_stored = b"a revision whose content this node was never sent";
    doc.set_hash(
        author,
        b"rev/de/ddd".to_vec(),
        Hash::new(never_stored),
        never_stored.len() as u64,
    )
    .await
    .unwrap();
    doc.del(author, b"rev/de/bbb".to_vec()).await.unwrap();

    let (_, missing) = docs.scan_raw_lossy("rev/").await.unwrap();
    assert_eq!(
        missing,
        vec!["rev/de/ddd".to_string()],
        "green first: the blob of rev/de/ddd is not local"
    );
    let mut keys = docs.keys_under("rev/").await.unwrap();
    keys.sort();
    assert_eq!(
        keys,
        vec!["rev/de/aaa".to_string(), "rev/de/ddd".to_string()],
        "every live key under the prefix, the one without its blob included"
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

#[tokio::test]
async fn an_img_entry_beside_a_document_stays_out_of_the_index() {
    let node = TestNode::new("refresh-img-entry").await.unwrap();
    let docs = node.db.docs("refresh-img-entry", None).await.unwrap();

    docs.put(Box::new(json!({ "_id": "rev/de/aaa", "name": "Alice" })))
        .await
        .unwrap();

    // Raw image bytes under `img/`, written into the DOCUMENT and not
    // through the store — the way a peer's image entry reaches the refresh.
    let (doc, author) = own_document(&node, &docs).await;
    let willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();
    let img_key = format!("{IMG_PREFIX}{}", "cd".repeat(32));
    willow
        .set_bytes(&doc, author, img_key.clone().into_bytes(), &b"\x89PNG raw"[..])
        .await
        .unwrap();

    docs.load(0).await.unwrap();

    let index = docs.index();
    assert!(
        index.get_bytes(&img_key).unwrap().is_none(),
        "an img/ entry is raw image bytes, not a document: never in the index"
    );
    assert!(
        !index
            .keys()
            .unwrap()
            .iter()
            .any(|k| k.starts_with(IMG_PREFIX)),
        "no img/ key at all"
    );
    assert!(
        index.get_bytes("rev/de/aaa").unwrap().is_some(),
        "the document beside it is indexed as before"
    );
}

#[tokio::test]
async fn set_hash_at_points_a_new_key_at_content_the_store_already_holds() {
    let node = TestNode::new("set-hash-at").await.unwrap();
    let docs = node.db.docs("set-hash-at", None).await.unwrap();
    let (doc, author) = own_document(&node, &docs).await;
    let willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();

    let image = b"\x89PNG an image the store already holds";
    let hash = willow
        .set_bytes(&doc, author, b"staging/img".to_vec(), &image[..])
        .await
        .unwrap();

    let img_key = format!("{IMG_PREFIX}{}", hash.to_hex());
    willow
        .set_hash_at(&doc, author, &img_key, hash, image.len() as u64)
        .await
        .unwrap();

    let entry = willow
        .get_one(&doc, Query::author(author).key_exact(img_key.as_bytes()))
        .await
        .unwrap()
        .expect("the entry exists");
    assert_eq!(entry.content_hash(), hash, "it names the existing content");
    assert_eq!(entry.content_len(), image.len() as u64);
}
