//! In-process stand-ins for the two outside services a node can be configured with
//! (`ClientConfig::relays`, `ClientConfig::pkarr_url`; `RN` §6) — test-only, behind
//! the `test-services` feature, so no release build carries them.
//!
//! The relay is iroh's own server without TLS on `127.0.0.1:0`, so its URL is
//! `http://127.0.0.1:<port>` — the form `iroh-relay --dev` has. Not iroh's
//! `test_utils::run_relay_server`: that one serves `https://` with a self-signed
//! certificate, which an endpoint reaches only with a skip-verify switch no
//! configuration here has.
//!
//! The lookup service is an ECHO: `PUT /pkarr/<z32>` keeps the body, `GET` hands it
//! back. Not iroh's `test_utils::DnsPkarrServer`: that one routes `PUT` only and
//! resolves through its own DNS, while `PkarrResolver` resolves by `GET`
//! (iroh 1.3.0 `address_lookup/pkarr.rs`). Nothing is verified here — the reading
//! endpoint verifies the signed packet itself (`SignedPacket::from_relay_payload`).

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use iroh::{EndpointId, RelayUrl};
use iroh_relay::server::{Access, AccessControl, ClientRequest, RelayConfig, Server, ServerConfig};
use parking_lot::Mutex;
use url::Url;

/// An iroh relay on loopback, optionally fenced by a shared token.
pub struct TestRelay {
    // Held, never read: dropping the server stops it.
    _server: Server,
    url: RelayUrl,
}

impl TestRelay {
    /// Start one. `Some(token)` admits only a client presenting that token — what
    /// `access.shared_token` does in the relay binary, whose implementation is
    /// private to it.
    ///
    /// # Panics
    /// If the server does not bind.
    pub async fn spawn(token: Option<&str>) -> Self {
        let mut relay = RelayConfig::new((Ipv4Addr::LOCALHOST, 0));
        if let Some(token) = token {
            relay.access = Arc::new(SharedToken(token.to_owned()));
        }
        let mut config = ServerConfig::default();
        config.relay = Some(relay);
        let server = Server::spawn(config).await.expect("the test relay binds");
        let addr = server.http_addr().expect("a relay config was given");
        let url = format!("http://{addr}")
            .parse()
            .expect("a loopback address is a URL");
        Self {
            _server: server,
            url,
        }
    }

    /// Where it is dialled.
    pub fn url(&self) -> &RelayUrl {
        &self.url
    }
}

/// `access.shared_token` with one token.
#[derive(Debug)]
struct SharedToken(String);

impl AccessControl for SharedToken {
    async fn on_connect(&self, request: &ClientRequest) -> Access {
        if request.auth_token().as_deref() == Some(self.0.as_str()) {
            Access::Allow
        } else {
            Access::Deny {
                reason: Some("no matching token".into()),
            }
        }
    }
}

/// What the echo keeps, by z-base-32 id.
type Records = Arc<Mutex<HashMap<String, Bytes>>>;

/// A pkarr lookup service on loopback that keeps what it is handed.
pub struct TestPkarr {
    url: Url,
    records: Records,
    task: tokio::task::JoinHandle<()>,
}

impl TestPkarr {
    /// Start one; its [`Self::url`] is `http://127.0.0.1:<port>/pkarr`.
    ///
    /// # Panics
    /// If the listener does not bind.
    pub async fn spawn() -> Self {
        let records = Records::default();
        let app = Router::new()
            .route("/pkarr/{key}", get(resolve).put(publish))
            .with_state(Arc::clone(&records));
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("the test lookup service binds");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let url = format!("http://{addr}/pkarr")
            .parse()
            .expect("a loopback address is a URL");
        Self { url, records, task }
    }

    /// The URL a node takes as `pkarr_url`.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Whether `id` has published here.
    pub fn holds(&self, id: &EndpointId) -> bool {
        self.records.lock().contains_key(&id.to_z32())
    }
}

impl Drop for TestPkarr {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn publish(
    State(records): State<Records>,
    Path(key): Path<String>,
    body: Bytes,
) -> StatusCode {
    records.lock().insert(key, body);
    StatusCode::NO_CONTENT
}

async fn resolve(
    State(records): State<Records>,
    Path(key): Path<String>,
) -> Result<Bytes, StatusCode> {
    records.lock().get(&key).cloned().ok_or(StatusCode::NOT_FOUND)
}
