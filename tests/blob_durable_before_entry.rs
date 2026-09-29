//! `LN` Task 25 (9), F1 — ist der Blob dauerhaft, wenn `set_bytes` zurückkehrt?
//!
//! A document entry is a key and a content HASH; the content itself lives in
//! the iroh-blobs `FsStore`. Both stores batch their commits, and not alike:
//! iroh-docs commits its redb at most `MAX_COMMIT_DELAY` (500 ms) after an
//! insert, the `FsStore` meta actor holds its write transaction open for up to
//! `max_read_duration` (1 s) collecting further writes (iroh-blobs 0.103,
//! `store/fs/meta.rs`, `Actor::run`). A SIGKILL inside that half second leaves a
//! durable entry whose blob never reached the disk — an entry every later
//! `scan_docs` over its prefix fails on, for good, because nothing in the
//! engine ever asks for an own entry's content again.
//!
//! What this file holds is the property the fix gives: when `set_bytes`
//! returns, the blob is COMMITTED. It is measured the only way a crash can be
//! measured from inside the process that would crash — by copying the blob
//! store's directory at that instant, which is the state a SIGKILL at that
//! instant would leave, and opening the copy as a fresh store. No sleep
//! anywhere: "at the instant `set_bytes` returns" is the question.

use std::path::Path;

use guardian_db::p2p::network::client::IrohClient;
use guardian_db::p2p::network::config::ClientConfig;
use guardian_db::p2p::network::core::docs::WillowDocs;
use iroh_blobs::store::fs::FsStore;
use iroh_docs::DocTicket;
use iroh_docs::store::Query;
use serde_json::json;
use tempfile::TempDir;
mod common;

use common::TestNode;

/// Copy `from` into `to`, recursively — a byte image of a directory the
/// running store still holds open, which is what a killed process leaves.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

#[tokio::test]
async fn the_blob_behind_an_entry_is_on_disk_when_set_bytes_returns() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("iroh");
    let mut cfg = ClientConfig::testing();
    cfg.data_store_path = Some(data.clone());
    cfg.port = 0;
    let client = IrohClient::new(cfg).await.unwrap();
    let mut docs = WillowDocs::new(client.backend().clone()).await.unwrap();
    let author = docs.get_or_init_author().await.unwrap();
    let doc = docs.create_doc().await.unwrap();

    let value = b"a revision header published just before the kill".to_vec();
    let hash = docs
        .set_bytes(&doc, author, b"rev/en/aaa".to_vec(), value.clone())
        .await
        .unwrap();

    // THE KILL, as a state: the blob store's files exactly as they are now.
    let image = TempDir::new().unwrap();
    copy_tree(&data.join("iroh_store"), image.path());

    let reopened = FsStore::load(image.path()).await.unwrap();
    let read = reopened.blobs().get_bytes(hash).await;
    assert_eq!(
        read.as_deref().ok(),
        Some(value.as_slice()),
        "the entry may be durable from this instant on, so its blob must \
         already be — otherwise a SIGKILL here leaves an entry nothing can \
         read (F1); got {read:?}"
    );
    reopened.shutdown().await.unwrap();
}

/// The same property at the CALLER the daemon uses — `put` on a document
/// store — so a store that stopped going through `WillowDocs::set_bytes` turns
/// this red and not only the unit above (the lesson of `LI §9`: a gate at the
/// helper says nothing about the path production takes).
#[tokio::test]
async fn a_document_put_leaves_every_entry_readable_from_a_crash_image() {
    let node = TestNode::new("blob-durable-put").await.unwrap();
    let docs = node.db.docs("blob-durable-put", None).await.unwrap();

    docs.put(Box::new(json!({ "_id": "rev/en/aaa", "name": "Alice" })))
        .await
        .unwrap();

    // THE KILL, as a state — taken first, before anything else can commit.
    let image = TempDir::new().unwrap();
    copy_tree(
        &node.path().join("blob-durable-put/iroh/iroh_store"),
        image.path(),
    );

    // Which hash the entry names, read from the live document afterwards: the
    // entry itself is not in question here, its content is.
    let ticket = docs
        .share_ticket()
        .await
        .unwrap()
        .parse::<DocTicket>()
        .unwrap();
    let willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();
    let doc = willow
        .open_doc(ticket.capability.id())
        .await
        .unwrap()
        .expect("the store's own document, opened a second time");
    let entries = willow
        .get_many(&doc, Query::single_latest_per_key().key_prefix("rev/en/").build())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1, "the one entry the put wrote");

    let reopened = FsStore::load(image.path()).await.unwrap();
    let read = reopened.blobs().get_bytes(entries[0].content_hash()).await;
    assert!(
        read.is_ok(),
        "the put has returned, so the content behind its entry must survive a \
         kill at this instant (F1); got {read:?}"
    );
    reopened.shutdown().await.unwrap();
}
