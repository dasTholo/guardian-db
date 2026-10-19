//! A sync that delivers more rows than iroh-docs' subscriber channel holds
//! (`SUBSCRIBE_CHANNEL_CAP`, 256) must reach the receiver's index and leave
//! its docs actor answering.
//!
//! The deadlock this gate holds down: iroh-docs' sync actor hands each
//! replica event to every subscriber by AWAITING that subscriber's bounded
//! channel, inside the insert itself. The live index task used to await a
//! full `refresh_doc_index` — whose `get_many` is a request to that same
//! actor — between two reads of its event stream. Once a sync delivered more
//! than 256 rows while a refresh was pending, the actor waited on the task's
//! channel and the task waited on the actor: every later docs call on that
//! node hung for good (`NZ` rehearsal, 2026-10-19, ~250 revisions).

mod common;

use common::{TestNode, connect_nodes};
use serde_json::json;
use std::time::Duration;

/// Comfortably above the 256 events one subscriber channel holds.
const ROWS: usize = 600;

#[tokio::test]
async fn a_sync_of_more_rows_than_the_event_channel_holds_reaches_the_index() {
    let node1 = TestNode::new("burst_node1").await.unwrap();
    let node2 = TestNode::new("burst_node2").await.unwrap();
    connect_nodes(&node1, &node2).await.unwrap();

    // Every row written BEFORE the receiver opens the store, so the first
    // sync carries all of them in one burst.
    let docs1 = node1.db.docs("burst-docs", None).await.unwrap();
    for i in 0..ROWS {
        docs1
            .put(Box::new(json!({ "_id": format!("row/{i:04}"), "n": i })))
            .await
            .unwrap();
    }

    let docs2 = node2.db.docs("burst-docs", None).await.unwrap();
    node1
        .db
        .connect_to_peer(node2.iroh.node_id())
        .await
        .unwrap();
    node2
        .db
        .connect_to_peer(node1.iroh.node_id())
        .await
        .unwrap();

    let index = docs2.index();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let held = index.keys().unwrap().len();
        if held >= ROWS {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {held} of {ROWS} rows reached node2's index"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The docs actor itself still answers: `load` is a `get_many` through it.
    tokio::time::timeout(Duration::from_secs(20), docs2.load(0))
        .await
        .expect("node2's docs actor answers after the burst")
        .unwrap();
}
