//! An in-process stand-in for the outside service a node can be configured with
//! (`ClientConfig::relays`; `RN` §6) — test-only, behind the `test-services`
//! feature, so no release build carries it. The own pkarr lookup service is gone
//! (`EV` §5.3), and its stand-in with it.
//!
//! The relay is iroh's own server without TLS on `127.0.0.1:0`, so its URL is
//! `http://127.0.0.1:<port>` — the form `iroh-relay --dev` has. Not iroh's
//! `test_utils::run_relay_server`: that one serves `https://` with a self-signed
//! certificate, which an endpoint reaches only with a skip-verify switch no
//! configuration here has.

use std::net::Ipv4Addr;
use std::sync::Arc;

use iroh::RelayUrl;
use iroh_relay::server::{Access, AccessControl, ClientRequest, RelayConfig, Server, ServerConfig};

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
