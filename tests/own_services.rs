//! `RN` §2: a node configured with its own relays and its own lookup service talks to
//! exactly those, a relay's token fence holds, and two nodes that know each other only
//! by id find each other through the two. In-process stand-ins only
//! (`p2p::network::test_services`): no container, no network beyond loopback, no n0.

use std::time::Duration;

use guardian_db::p2p::network::client::IrohClient;
use guardian_db::p2p::network::config::{ClientConfig, RelaySpec};
use guardian_db::p2p::network::test_services::{TestPkarr, TestRelay};
use iroh::{Endpoint, EndpointAddr, TransportAddr, Watcher as _};

const ONLINE: Duration = Duration::from_secs(10);

/// A node with no n0 and no mDNS (`testing()`), over a fresh directory, naming
/// `relays` and `pkarr`. The `TempDir` goes back too: dropping it early would delete
/// the store under the running node.
async fn node(relays: Vec<RelaySpec>, pkarr: Option<url::Url>) -> (IrohClient, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = ClientConfig::testing();
    config.data_store_path = Some(dir.path().to_path_buf());
    config.relays = relays;
    config.pkarr_url = pkarr;
    (IrohClient::new(config).await.unwrap(), dir)
}

async fn endpoint(client: &IrohClient) -> Endpoint {
    client
        .backend()
        .get_endpoint()
        .await
        .unwrap()
        .read()
        .await
        .clone()
        .expect("the endpoint bound")
}

fn relay(test: &TestRelay, token: Option<&str>) -> RelaySpec {
    RelaySpec {
        url: test.url().clone(),
        auth_token: token.map(str::to_owned),
    }
}

/// V1 — the counterpart of T1: with a relay named, the endpoint reaches it, and its
/// address names that relay and no other.
#[tokio::test]
async fn a_configured_relay_is_the_only_relay_the_endpoint_names() {
    let test_relay = TestRelay::spawn(None).await;
    let (client, _dir) = node(vec![relay(&test_relay, None)], None).await;
    let endpoint = endpoint(&client).await;
    tokio::time::timeout(ONLINE, endpoint.online())
        .await
        .expect("the endpoint reached its configured relay");
    let relays: Vec<_> = endpoint
        .addr()
        .addrs
        .iter()
        .filter_map(|a| match a {
            TransportAddr::Relay(url) => Some(url.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(relays, vec![test_relay.url().clone()], "exactly the configured relay");
}

/// V2 — A publishes at the configured lookup service; B knows nothing of A but its id
/// and still connects. mDNS is off (`testing()`), so the address can only have come
/// from the lookup service.
#[tokio::test]
async fn two_endpoints_that_know_each_other_only_by_id_connect_through_the_configured_services() {
    let test_relay = TestRelay::spawn(None).await;
    let pkarr = TestPkarr::spawn().await;
    let (a, _da) = node(vec![relay(&test_relay, None)], Some(pkarr.url().clone())).await;
    let (b, _db) = node(vec![relay(&test_relay, None)], Some(pkarr.url().clone())).await;
    let a_endpoint = endpoint(&a).await;
    tokio::time::timeout(ONLINE, a_endpoint.online())
        .await
        .expect("A reached its relay");
    let a_id = a_endpoint.id();
    let deadline = tokio::time::Instant::now() + ONLINE;
    while !pkarr.holds(&a_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "A never published to the configured lookup service"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::timeout(
        Duration::from_secs(20),
        endpoint(&b).await.connect(EndpointAddr::new(a_id), iroh_gossip::ALPN),
    )
    .await
    .expect("B dialled A within 20 s")
    .expect("B reached A by its id alone");
}

/// V3 — n0 beside an own service is refused before anything binds or is written.
#[tokio::test]
async fn n0_together_with_a_configured_service_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = ClientConfig::testing();
    config.data_store_path = Some(dir.path().to_path_buf());
    config.enable_discovery_n0 = true;
    config.pkarr_url = Some("http://127.0.0.1:1/pkarr".parse().unwrap());
    let err = IrohClient::new(config)
        .await
        .err()
        .expect("refused before anything binds");
    assert!(err.to_string().contains("enable_discovery_n0"), "{err}");
    assert!(
        std::fs::read_dir(dir.path()).unwrap().next().is_none(),
        "nothing written before the refusal"
    );
}

/// V4 — the fence (`RN` §5, AW1): the right token in the header is admitted, a
/// missing or a wrong one is turned away at the upgrade.
#[tokio::test]
async fn a_relay_token_in_the_header_is_admitted_and_a_missing_or_wrong_one_is_turned_away() {
    let test_relay = TestRelay::spawn(Some("fence")).await;
    let (admitted, _a) = node(vec![relay(&test_relay, Some("fence"))], None).await;
    tokio::time::timeout(ONLINE, endpoint(&admitted).await.online())
        .await
        .expect("the right token is admitted");
    for token in [None, Some("wrong")] {
        let (turned_away, _dir) = node(vec![relay(&test_relay, token)], None).await;
        let endpoint = endpoint(&turned_away).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(5), endpoint.online())
                .await
                .is_err(),
            "{token:?} reached the relay"
        );
        let status = endpoint.home_relay_status().get();
        assert!(
            status.iter().all(|s| !s.is_connected()),
            "{token:?}: connected anyway; denial reasons: {:?}",
            status.iter().map(|s| s.auth_denied_reason()).collect::<Vec<_>>()
        );
    }
}
