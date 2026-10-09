//! `SR` D10 contract: how rows written at a STATED timestamp
//! (`DocumentStore::put_at`, `WillowDocs::set_bytes_at`) reach a peer that has
//! already joined the namespace.
//!
//! `set_bytes_at` inserts through `SyncHandle::insert_remote`, which is never
//! gossiped (`engine/live.rs` `on_replica_event` broadcasts `Op::Put` for
//! `LocalInsert` only), and its `entry_put` sets the author's head in
//! `latest_per_author` to the row's timestamp unconditionally (`store/fs.rs`).
//! So a stamped row reaches an already-joined peer only with the next range
//! sync: NeighborUp, `Doc::start_sync` / a direct join, or a `SyncReport` with
//! news (`has_news_for_us`, which fires only for stamps ABOVE the receiver's
//! head). There is no periodic resync.
//!
//! Measured (stress 5): an explicit `start_sync` brings the stamped rows 5/5 in
//! ~0.1 s; a `SyncReport` brings a HIGHER stamp to a third peer 5/5 and a LOWER
//! one 0/5; without a sync nothing arrives 0/5, even after an ordinary write by
//! the same author.
//!
//! Why it matters: the key rotation copies rows with `put_at` while the node is
//! offline, and relies on the next start's join sync to carry them. Nothing
//! else would. The tests pin the behaviour as measured, the negative ones
//! included (their absence claim is bounded by `ABSENCE`); if iroh-docs ever
//! starts gossiping stamped rows, they fail and the doc sentences on
//! `put_at` / `set_bytes_at` need revisiting.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use guardian_db::guardian::error::GuardianError;
use guardian_db::p2p::network::core::docs::WillowDocs;
use guardian_db::traits::{DocumentStore, Placed};
use iroh_docs::store::Query;
use iroh_docs::{AuthorHeads, AuthorId, DocTicket, NamespaceId};
mod common;

use common::{TestNode, connect_nodes};
use serde_json::json;

type Store = std::sync::Arc<dyn DocumentStore<Error = GuardianError>>;

/// How long a joined peer gets to see a row — well past one sync round trip on
/// loopback, short of the 60 s hang gate.
const DEADLINE: Duration = Duration::from_secs(25);

/// How long a row must stay absent for the absence to count as the claim.
const ABSENCE: Duration = Duration::from_secs(4);

fn row(id: &str, v: &str) -> guardian_db::traits::Document {
    Box::new(json!({ "_id": id, "v": v }))
}

fn now_micros() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

/// The store's own iroh-docs document, opened a second time over the same
/// backend (as in `stamped_writes.rs`).
async fn own_document(node: &TestNode, docs: &Store) -> iroh_docs::api::Doc {
    let ticket = docs
        .share_ticket()
        .await
        .unwrap()
        .parse::<DocTicket>()
        .unwrap();
    let willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();
    willow
        .open_doc(ticket.capability.id())
        .await
        .unwrap()
        .expect("the store's own document, opened a second time")
}

/// `(key, timestamp)` of every non-empty entry under `prefix` in `node`'s
/// replica — what replication delivered, independent of blob downloads.
async fn entries(node: &TestNode, doc: &iroh_docs::api::Doc, prefix: &str) -> Vec<(String, u64)> {
    let willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();
    let mut out: Vec<_> = willow
        .get_many(doc, Query::single_latest_per_key().key_prefix(prefix).build())
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.content_len() > 0)
        .map(|e| (String::from_utf8_lossy(e.key()).into_owned(), e.timestamp()))
        .collect();
    out.sort();
    out
}

/// The author of this node's entry at `key`.
async fn author_of(node: &TestNode, doc: &iroh_docs::api::Doc, key: &str) -> AuthorId {
    let willow = WillowDocs::new(node.iroh.backend().clone()).await.unwrap();
    willow
        .get_many(doc, Query::single_latest_per_key().key_exact(key.as_bytes()))
        .await
        .unwrap()
        .first()
        .expect("own entry")
        .author()
}

/// Is `node`'s head for `author` in `namespace` strictly below `t`? Probed
/// through the one public door to `latest_per_author`:
/// `SyncHandle::has_news_for_us` answers `Some` iff `t` is newer than the head
/// (or the author is unknown).
async fn head_below(node: &TestNode, namespace: NamespaceId, author: AuthorId, t: u64) -> bool {
    let sync = node
        .iroh
        .backend()
        .get_sync()
        .await
        .unwrap()
        .read()
        .await
        .as_ref()
        .unwrap()
        .clone();
    let mut heads = AuthorHeads::default();
    heads.insert(author, t);
    sync.has_news_for_us(namespace, heads).await.unwrap().is_some()
}

/// Two connected nodes, both holding the store `name`; B joined through the
/// automatic ticket exchange. Connectivity is proven with an ORDINARY write by
/// B that A must see — so A's own author head is untouched before the copy
/// starts, as for a copier writing into a fresh namespace.
struct Pair {
    a: TestNode,
    b: TestNode,
    docs_a: Store,
    /// Kept alive so B's store stays open for the whole test.
    _docs_b: Store,
    doc_b: iroh_docs::api::Doc,
}

async fn pair(name: &str) -> Pair {
    let a = TestNode::new(&format!("{name}-a")).await.unwrap();
    let b = TestNode::new(&format!("{name}-b")).await.unwrap();
    connect_nodes(&a, &b).await.unwrap();
    let docs_a = a.db.docs(name, None).await.unwrap();
    let docs_b = b.db.docs(name, None).await.unwrap();
    let doc_a = own_document(&a, &docs_a).await;
    let doc_b = own_document(&b, &docs_b).await;
    assert_eq!(doc_a.id(), doc_b.id(), "B joined A's namespace");
    docs_b.put(row("seed/b", "from b")).await.unwrap();
    let start = Instant::now();
    while entries(&a, &doc_a, "seed/").await.is_empty() {
        assert!(start.elapsed() < DEADLINE, "B's ordinary write never reached A: no live link");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Let the join syncs (DirectJoin / NewNeighbor) finish, so a row written
    // below cannot ride on one of them.
    tokio::time::sleep(Duration::from_secs(3)).await;
    Pair { a, b, docs_a, _docs_b: docs_b, doc_b }
}

async fn put_at(p: &Pair, id: &str, ts: u64) {
    assert_eq!(p.docs_a.put_at(row(id, id), ts).await.unwrap(), Placed::Written);
}

/// Wait until `node` holds `key` under `k/`, at most `limit`.
async fn wait_holds(node: &TestNode, doc: &iroh_docs::api::Doc, key: &str, limit: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < limit {
        if entries(node, doc, "k/").await.iter().any(|(k, _)| k == key) {
            eprintln!("[d10] {key} arrived after {:?}", start.elapsed());
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Stamped rows — a high stamp, then a lower one — are not gossiped to a
/// joined peer. Ordinary traffic does flow (A's ordinary row reaches B), so
/// the absence is not a dead link; and that ordinary write does not carry the
/// stamped rows along either.
#[tokio::test]
async fn stamped_rows_are_not_gossiped_to_a_joined_peer() {
    let p = pair("d10-no-gossip").await;
    let base = now_micros() - 3_600_000_000;
    put_at(&p, "k/high", base + 1000).await;
    put_at(&p, "k/low", base + 500).await;
    assert!(
        !wait_holds(&p.b, &p.doc_b, "k/high", ABSENCE).await,
        "k/high must not reach B without a sync"
    );
    assert!(!wait_holds(&p.b, &p.doc_b, "k/low", Duration::from_millis(200)).await);
    p.docs_a.put(row("k/ordinary", "now")).await.unwrap();
    assert!(
        wait_holds(&p.b, &p.doc_b, "k/ordinary", DEADLINE).await,
        "an ordinary write is gossiped: the link is alive"
    );
    assert!(
        !wait_holds(&p.b, &p.doc_b, "k/high", ABSENCE).await
            && !wait_holds(&p.b, &p.doc_b, "k/low", Duration::from_millis(200)).await,
        "an ordinary write does not bring the stamped rows"
    );
}

/// B starts a sync with A explicitly (`Doc::start_sync`, a `DirectJoin` sync):
/// the trigger that does bring the stamped rows, whatever their order.
#[tokio::test]
async fn an_explicit_sync_brings_stamped_rows_to_a_joined_peer() {
    let p = pair("d10-explicit").await;
    let base = now_micros() - 3_600_000_000;
    put_at(&p, "k/high", base + 1000).await;
    put_at(&p, "k/low", base + 500).await;
    let addr = p.a.get_node_addr().await.unwrap();
    p.doc_b.start_sync(vec![addr]).await.unwrap();
    assert!(wait_holds(&p.b, &p.doc_b, "k/high", DEADLINE).await, "k/high after start_sync");
    assert!(wait_holds(&p.b, &p.doc_b, "k/low", DEADLINE).await, "k/low after start_sync");
}

/// Three nodes in one namespace for the `SyncReport` path (`live.rs`
/// `on_sync_finished` → `on_sync_report` → `has_news_for_us`): A copies, B
/// and C have joined.
struct Trio {
    a: TestNode,
    b: TestNode,
    c: TestNode,
    docs_a: Store,
    _docs_b: Store,
    _docs_c: Store,
    doc_a: iroh_docs::api::Doc,
    doc_b: iroh_docs::api::Doc,
    doc_c: iroh_docs::api::Doc,
}

async fn trio(name: &str) -> Trio {
    let a = TestNode::new(&format!("{name}-a")).await.unwrap();
    let b = TestNode::new(&format!("{name}-b")).await.unwrap();
    let c = TestNode::new(&format!("{name}-c")).await.unwrap();
    connect_nodes(&a, &b).await.unwrap();
    connect_nodes(&a, &c).await.unwrap();
    connect_nodes(&b, &c).await.unwrap();
    let docs_a = a.db.docs(name, None).await.unwrap();
    let docs_b = b.db.docs(name, None).await.unwrap();
    let docs_c = c.db.docs(name, None).await.unwrap();
    let doc_a = own_document(&a, &docs_a).await;
    let doc_b = own_document(&b, &docs_b).await;
    let doc_c = own_document(&c, &docs_c).await;
    assert_eq!(doc_a.id(), doc_b.id());
    assert_eq!(doc_a.id(), doc_c.id());
    docs_b.put(row("seed/b", "from b")).await.unwrap();
    docs_c.put(row("seed/c", "from c")).await.unwrap();
    let start = Instant::now();
    for (who, doc) in [(&b, &doc_b), (&c, &doc_c), (&a, &doc_a)] {
        while entries(who, doc, "seed/").await.len() < 2 {
            assert!(start.elapsed() < DEADLINE, "the seeds never met: no live mesh");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    Trio { a, b, c, docs_a, _docs_b: docs_b, _docs_c: docs_c, doc_a, doc_b, doc_c }
}

/// A writes `k/first` at base+1000; B and C pull it with an explicit sync, so
/// B's head for A's author is that stamp. A then writes `k/second` at
/// `base + second_ts_offset` and C pulls it — C's finished sync broadcasts a
/// `SyncReport` with the stamp of what it received. Returns whether B, which
/// only hears that report, got the row within `window`.
async fn report_reaches_b(name: &str, second_ts_offset: u64, window: Duration) -> bool {
    let t = trio(name).await;
    let base = now_micros() - 3_600_000_000;
    let ns = t.doc_a.id();
    assert_eq!(t.docs_a.put_at(row("k/first", "x"), base + 1000).await.unwrap(), Placed::Written);
    let addr_a = t.a.get_node_addr().await.unwrap();
    t.doc_b.start_sync(vec![addr_a.clone()]).await.unwrap();
    t.doc_c.start_sync(vec![addr_a.clone()]).await.unwrap();
    assert!(wait_holds(&t.b, &t.doc_b, "k/first", DEADLINE).await, "B pulls the first row");
    assert!(wait_holds(&t.c, &t.doc_c, "k/first", DEADLINE).await, "C pulls the first row");
    let author = author_of(&t.a, &t.doc_a, "k/first").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        t.docs_a.put_at(row("k/second", "y"), base + second_ts_offset).await.unwrap(),
        Placed::Written
    );
    eprintln!(
        "[d10] report: second at base+{second_ts_offset}; B head below it? {}",
        head_below(&t.b, ns, author, base + second_ts_offset).await
    );
    t.doc_c.start_sync(vec![addr_a]).await.unwrap();
    assert!(wait_holds(&t.c, &t.doc_c, "k/second", DEADLINE).await, "C pulls the second row");
    wait_holds(&t.b, &t.doc_b, "k/second", window).await
}

/// The second row is stamped HIGHER than what B holds: C's sync report has
/// news for B, and B syncs.
#[tokio::test]
async fn a_sync_report_brings_a_higher_stamp_to_a_third_peer() {
    assert!(report_reaches_b("d10-report-higher", 2000, DEADLINE).await);
}

/// The second row is stamped LOWER than what B holds: the report has no news
/// for B (`has_news_for_us`), so B never syncs and never gets the row.
#[tokio::test]
async fn a_sync_report_does_not_bring_a_stamp_below_the_peers_head() {
    assert!(!report_reaches_b("d10-report-lower", 500, ABSENCE).await);
}
