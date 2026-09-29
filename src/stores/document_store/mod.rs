use crate::access_control::acl_simple::SimpleAccessController;
use crate::access_control::traits::AccessController;
use crate::address::Address;
use crate::data_store::Datastore;
use crate::events::EventBus;
use crate::events::EventEmitter;
use crate::guardian::error::{GuardianError, Result};
use crate::log::identity::Identity;
use crate::log::lamport_clock::LamportClock;
use crate::p2p::network::client::IrohClient;
use crate::p2p::network::core::docs::WillowDocs;
use crate::stores::operation::Operation;
use crate::stores::payload_codec::{RecordCtx, SharedPayloadCodec, codec_or_identity};
use crate::traits::{
    CreateDocumentDBOptions, DocumentStoreGetOptions, NewStoreOptions, Store, StoreIndex,
    TracerWrapper,
};
use bytes::Bytes;
use iroh_docs::{AuthorId, Capability, api::Doc, store::Query};
use opentelemetry::trace::{TracerProvider, noop::NoopTracerProvider};
use parking_lot::RwLock;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{Span, debug, info, instrument, warn};

/// Represents a generic document.
pub type Document = Value;

/// Cache key used to persist the iroh-docs document's NamespaceId.
const NAMESPACE_CACHE_KEY: &[u8] = b"_iroh_docs_doc_namespace_id";
/// Cache key used to persist whether this replica holds the namespace write secret.
/// Stored as a single byte: 1 = write-capable, 0 = read-only.
const WRITABLE_CACHE_KEY: &[u8] = b"_iroh_docs_doc_writable";

/// Local in-memory index that mirrors the iroh-docs document state.
///
/// Updated atomically after each put/delete operation,
/// serving as a synchronous cache for StoreIndex queries.
pub struct DocumentStoreIndex {
    index: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl Default for DocumentStoreIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentStoreIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_value(&self, key: &str) -> Option<Vec<u8>> {
        let guard = self.index.read();
        guard.get(key).cloned()
    }

    pub fn keys(&self) -> Vec<String> {
        let guard = self.index.read();
        guard.keys().cloned().collect()
    }

    pub fn insert(&self, key: String, value: Vec<u8>) {
        let mut guard = self.index.write();
        guard.insert(key, value);
    }

    pub fn remove(&self, key: &str) {
        let mut guard = self.index.write();
        guard.remove(key);
    }

    pub fn clear_all(&self) {
        let mut guard = self.index.write();
        guard.clear();
    }

    /// Drop every key not in `keep`, in ONE write — the pruning half of
    /// `refresh_in_place`, which never clears.
    pub fn retain_keys(&self, keep: &HashSet<&str>) {
        let mut guard = self.index.write();
        guard.retain(|key, _| keep.contains(key.as_str()));
    }

    /// Replace the whole map in ONE write.
    ///
    /// The reason it exists is that `clear_all` + refill is visible from
    /// the OUTSIDE as an empty index: between the clear and the last
    /// insert every reader sees a map that is missing keys it saw a
    /// moment ago, and for the duration of the first content fetch it
    /// sees nothing at all.
    ///
    /// LOCK LOAD GOES DOWN, not up, and it is worth saying because the
    /// opposite is the natural guess: this holds the write lock for a
    /// `HashMap` move where `clear_all` held it for a `clear` — both O(1)
    /// against the map size — while the REBUILD now runs outside any
    /// lock, where it used to take one per `insert`.
    ///
    /// The price is memory: the old map and the new one stand side by
    /// side while the rebuild runs. That is exactly what buys a
    /// continuously readable index, and it is the deliberate difference
    /// to `clear_all`.
    pub fn replace_all(&self, next: HashMap<String, Vec<u8>>) {
        let mut guard = self.index.write();
        *guard = next;
    }

    pub fn len(&self) -> usize {
        let guard = self.index.read();
        guard.len()
    }

    pub fn is_empty(&self) -> bool {
        let guard = self.index.read();
        guard.is_empty()
    }
}

/// Refreshes the in-memory index from the current state of the iroh-docs document.
/// Shared between `sync_index_from_docs` and the reactive live-sync task.
///
/// **`L7`:** this block opened with "IT CLEARS AND REFILLS, and that is a
/// MEASURED decision rather than the state nobody got round to changing".
/// It no longer clears — see IN PLACE below. The measurement that sentence
/// stood on still holds, and it is why the replacement is not the swap:
///
/// `LI` swapped this loop for
/// [`rebuild_from`] + [`DocumentStoreIndex::replace_all`], which removes
/// the window in which this index is empty, and then measured three
/// blocks of 60 suite runs at two gitlinks under one compiler: the swap
/// raised the loss rate from 10/60 to 28/60, deadline losses from 5/60 to
/// 17/60 (Fisher exact two-sided over the pooled point, p = 0.00072), and
/// an A-B-A repeat at the first point reproduced 5/60 hours later. The
/// suspicion that the host had drifted is refuted by that order:
/// low-high-low.
///
/// THE HYPOTHESIS, and it is a hypothesis rather than a measurement: this
/// form makes each key visible the moment its own `cat_bytes` returns, so
/// a reader waiting on one key can finish mid-rebuild. The swap makes NO
/// key visible until the WHOLE rebuild is done, and since the live-sync
/// task rebuilds on every remote event, a dense event stream pushes that
/// moment further and further back. The empty window is real — `LI T1`,
/// `T2` and `T3` hold it down — but trading it for a later-and-all-at-once
/// window measured worse. Whoever tries again should measure before
/// wiring it up: no gate here covers the CALLER, only the building block.
///
/// IN PLACE, NOT SWAPPED, and not the clear either (`LN §14.2` Task 25 (8),
/// user decision 2026-09-29). Each key is written the moment its own
/// `cat_bytes` returns, exactly as under the clear — the property the
/// hypothesis above credits the clear with is kept. What the clear added on
/// top is gone: every key absent until its fetch returned, and a key whose
/// fetch FAILED (`Error::Io`, the blob not local yet) absent for the whole
/// of that rebuild. That second absence is what `LN` F3 and F4 read as "the
/// store holds no such value". A failed fetch now keeps the key's previous
/// value — stale until the blob arrives, never absent; a key that had none
/// stays absent, as before. Keys leave only when the document no longer
/// holds them: missing from the snapshot, or a deletion marker
/// (`content_len() == 0`). That pruning runs BEFORE the first `await`, where
/// `clear_all` sat, so a local write during the loop survives it exactly as
/// it survived the clear: the local-write race is as narrow as it was, not
/// widened the way the swap widened it (`LI §10.9` finding 5).
///
/// THE CALLER IS COVERED this time, which is the lesson `LI §9` paid for:
/// the loop is `refresh_in_place`, which this function calls and the unit
/// gates call too, and `tests/doc_index_refresh.rs` runs the whole of this
/// through `Store::load`. What is NOT measured yet is the rate: whether
/// this lowers F3's reader-side deletions and F4's 422 in `local_net` is a
/// block of runs that has not been driven when this was written.
async fn refresh_doc_index(
    docs: &WillowDocs,
    doc: &Doc,
    client: &Arc<IrohClient>,
    index: &Arc<DocumentStoreIndex>,
) -> Result<usize> {
    let entries = docs
        .get_many(doc, Query::single_latest_per_key().build())
        .await?;

    let snapshot = entries
        .iter()
        .map(|entry| {
            let key = String::from_utf8_lossy(entry.key()).to_string();
            let hash = (entry.content_len() > 0).then(|| entry.content_hash().to_hex());
            (key, hash)
        })
        .collect();
    let count = refresh_in_place(index, snapshot, |hash: String| async move {
        client.cat_bytes(&hash).await
    })
    .await;

    debug!(
        "DocumentStore index synchronized from iroh-docs: {} entries",
        count
    );
    Ok(count)
}

/// The body of [`refresh_doc_index`] after the `get_many`: `snapshot` is
/// its entries as `(key, Some(content hash))`, or `None` for a deletion
/// marker, and `fetch` is `cat_bytes`.
///
/// SPLIT FROM ITS SOURCE for the reason `rebuild_from` was — so a gate can
/// hold it mid-flight — with the one difference `LI §9` asked for: this is
/// the loop production runs, not a block beside it.
async fn refresh_in_place<F, Fut>(
    index: &DocumentStoreIndex,
    snapshot: Vec<(String, Option<String>)>,
    fetch: F,
) -> usize
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>>>,
{
    // Where `clear_all` sat: before the first `await`, so nothing written
    // locally during the loop below can be pruned by it.
    let live: HashSet<&str> = snapshot
        .iter()
        .filter(|(_, hash)| hash.is_some())
        .map(|(key, _)| key.as_str())
        .collect();
    index.retain_keys(&live);

    let mut count = 0;
    for (key, hash) in snapshot {
        let Some(hash) = hash else { continue };
        match fetch(hash).await {
            Ok(value) => {
                index.insert(key, value);
                count += 1;
            }
            Err(e) => {
                warn!(
                    "Failed to read content for key from iroh-docs, keeping its previous value: {:?}",
                    e
                );
            }
        }
    }
    count
}

/// Build the next map from `keys` and install it in ONE write.
///
/// SPLIT FROM ITS SOURCE so a test can hold it mid-flight: `LI T2` passes
/// a fetch that blocks on a channel after the first entry and reads the
/// index while the rebuild stands still there. NOTHING IN PRODUCTION
/// CALLS THIS — see the note below the doc block — and whoever wires it
/// up again inherits `T2` with it, which is a decision, then, and not an
/// accident.
///
/// The map is built OUTSIDE the lock and installed with `replace_all`.
//
// NO PRODUCTION CALLER any more, and it stays anyway. `refresh_doc_index`
// above went back to clear-and-refill because the swap measured worse, so
// the paragraph above describes the path `LI` built and the measurement
// then took away — read it as the starting point of a second attempt, not
// as a description of what this tree runs.
//
// `L7`: "went back to clear-and-refill" held until `LN` Task 25 (8). The
// second attempt did not start from here: it refreshes in place
// (`refresh_in_place`), because this block delays every key to the end,
// which is the half of the swap that measured worse.
//
// IT STAYS because it is the building block `LI T2` stands on: that gate
// can hold a rebuild mid-flight only because the fetch is a parameter
// here. Delete this and `T2` goes with it, and whoever next tries to
// close the empty window starts from zero instead of from a block that
// already has a test around it.
#[allow(dead_code)]
async fn rebuild_from<F, Fut>(
    index: &Arc<DocumentStoreIndex>,
    keys: Vec<(String, String)>,
    fetch: F,
) -> usize
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>>>,
{
    let mut next = HashMap::with_capacity(keys.len());
    let mut count = 0;
    for (key, hash) in keys {
        match fetch(hash).await {
            Ok(value) => {
                next.insert(key, value);
                count += 1;
            }
            Err(e) => {
                warn!("Failed to read content for key from iroh-docs: {:?}", e);
            }
        }
    }
    index.replace_all(next);
    count
}

impl StoreIndex for DocumentStoreIndex {
    type Error = GuardianError;

    fn contains_key(&self, key: &str) -> std::result::Result<bool, Self::Error> {
        let guard = self.index.read();
        Ok(guard.contains_key(key))
    }

    fn get_bytes(&self, key: &str) -> std::result::Result<Option<Vec<u8>>, Self::Error> {
        let guard = self.index.read();
        Ok(guard.get(key).cloned())
    }

    fn keys(&self) -> std::result::Result<Vec<String>, Self::Error> {
        let guard = self.index.read();
        Ok(guard.keys().cloned().collect())
    }

    fn len(&self) -> std::result::Result<usize, Self::Error> {
        let guard = self.index.read();
        Ok(guard.len())
    }

    fn is_empty(&self) -> std::result::Result<bool, Self::Error> {
        let guard = self.index.read();
        Ok(guard.is_empty())
    }

    /// No-op for the iroh-docs-based implementation.
    /// The local index is updated directly after each put/delete operation.
    fn update_index(
        &mut self,
        _log: &crate::log::Log,
        _entries: &[crate::log::entry::Entry],
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn clear(&mut self) -> std::result::Result<(), Self::Error> {
        let mut guard = self.index.write();
        guard.clear();
        Ok(())
    }
}

/// DocumentStore implementation for GuardianDB using iroh-docs (WillowDocs).
///
/// This implementation uses the iroh-docs protocol for distributed document
/// storage with Last-Write-Wins (LWW) conflict resolution, replacing the
/// previous architecture based on BaseStore + OpLog.
///
/// # Architecture
///
/// - **Backend**: iroh-docs (Willow range-based reconciliation)
/// - **Write**: `doc.set_bytes()` / `doc.del()` via WillowDocs
/// - **Read**: Local in-memory index mirroring the iroh-docs state
/// - **Sync**: Automatic via Willow (no manual gossip heads)
/// - **Local index**: In-memory HashMap mirroring the iroh-docs state
pub struct GuardianDBDocumentStore {
    /// WillowDocs backend (iroh-docs) — replaces BaseStore.
    docs: WillowDocs,
    /// iroh-docs document handle for this namespace.
    doc_handle: Doc,
    /// AuthorId for write operations (mapped from the Identity).
    author_id: AuthorId,
    /// Access controller for permission validation.
    access_controller: Arc<dyn AccessController>,
    /// Event bus for reactive notifications.
    event_bus: Arc<EventBus>,
    /// Reference to the IrohClient (Store trait compatibility).
    client: Arc<IrohClient>,
    /// Cryptographic identity of the store.
    identity: Arc<Identity>,
    /// Store address (cached to resolve lifetime issues).
    cached_address: Arc<dyn Address + Send + Sync>,
    /// Database name.
    db_name: String,
    /// Local cache (sled) — used to persist the NamespaceId across reloads.
    cache: Arc<dyn Datastore>,
    /// Local in-memory index mirroring the iroh-docs state.
    index: Arc<DocumentStoreIndex>,
    /// Document options (marshal, unmarshal, key_extractor).
    doc_opts: CreateDocumentDBOptions,
    /// Span for structured tracing.
    span: Span,
    /// Tracer for telemetry.
    tracer: Arc<TracerWrapper>,
    /// Event emission interface (Store trait compatibility).
    emitter_interface: Arc<dyn crate::events::EmitterInterface + Send + Sync>,
    /// Empty log for compatibility with the Store trait (op_log()).
    empty_log: Arc<RwLock<crate::log::Log>>,
    /// Optional: BlobStore for large binary attachments.
    #[allow(dead_code)]
    blob_store: Option<Arc<crate::p2p::network::core::blobs::BlobStore>>,
    /// Whether this replica may originate writes. `false` when opened read-only (via the
    /// `read_only` option) or imported from a read-only `DocTicket` (no namespace secret).
    writable: bool,
    /// Transforms marshalled document bytes on the way to and from iroh-docs. Defaults to
    /// the no-op `IdentityCodec`. Applied *after* `doc_opts.marshal` and *before*
    /// `unmarshal`, so the codec sees serialized documents and the index mirrors the
    /// **stored** form. See [`crate::stores::payload_codec`].
    payload_codec: SharedPayloadCodec,
}

#[async_trait::async_trait]
impl Store for GuardianDBDocumentStore {
    type Error = GuardianError;

    #[allow(deprecated)]
    fn events(&self) -> &dyn crate::events::EmitterInterface {
        self.emitter_interface.as_ref()
    }

    async fn close(&self) -> std::result::Result<(), Self::Error> {
        debug!("Starting DocumentStore close operation (iroh-docs backend)");
        if let Err(e) = self.docs.close_doc(&self.doc_handle).await {
            warn!("Failed to close iroh-docs document: {:?}", e);
        }
        debug!("DocumentStore close completed");
        Ok(())
    }

    fn address(&self) -> &dyn Address {
        self.cached_address.as_ref()
    }

    fn index(&self) -> Box<dyn crate::traits::StoreIndex<Error = GuardianError> + Send + Sync> {
        Box::new(DocumentStoreIndex {
            index: self.index.index.clone(),
        })
    }

    fn store_type(&self) -> &str {
        "document"
    }

    fn cache(&self) -> Arc<dyn Datastore> {
        self.cache.clone()
    }

    async fn drop(&self) -> std::result::Result<(), Self::Error> {
        debug!("Starting DocumentStore drop operation (iroh-docs backend)");
        self.index.clear_all();
        let namespace_id = self.doc_handle.id();
        if let Err(e) = self.docs.drop_doc(namespace_id).await {
            warn!("Failed to drop iroh-docs document: {:?}", e);
        }
        if let Err(e) = self.cache.delete(NAMESPACE_CACHE_KEY).await {
            warn!("Failed to remove namespace from cache: {:?}", e);
        }
        debug!("DocumentStore drop completed");
        Ok(())
    }

    /// No-op for iroh-docs — Willow sync handles loading automatically.
    async fn load(&self, _amount: usize) -> std::result::Result<(), Self::Error> {
        self.sync_index_from_docs().await?;
        Ok(())
    }

    /// No-op for iroh-docs — Willow sync replaces the gossip heads exchange.
    async fn sync(
        &self,
        _heads: Vec<crate::log::entry::Entry>,
    ) -> std::result::Result<(), Self::Error> {
        self.sync_index_from_docs().await?;
        Ok(())
    }

    /// No-op for iroh-docs.
    async fn load_more_from(&self, _amount: u64, _entries: Vec<crate::log::entry::Entry>) {
        // iroh-docs manages its own incremental loading.
    }

    /// No-op for iroh-docs.
    async fn load_from_snapshot(&self) -> std::result::Result<(), Self::Error> {
        self.sync_index_from_docs().await?;
        Ok(())
    }

    /// Returns an empty Log for compatibility with the Store trait.
    /// In the iroh-docs architecture, the OpLog is no longer used.
    fn op_log(&self) -> Arc<parking_lot::RwLock<crate::log::Log>> {
        self.empty_log.clone()
    }

    fn client(&self) -> Arc<IrohClient> {
        self.client.clone()
    }

    fn db_name(&self) -> &str {
        &self.db_name
    }

    fn identity(&self) -> &Identity {
        &self.identity
    }

    fn access_controller(&self) -> &dyn crate::access_control::traits::AccessController {
        self.access_controller.as_ref()
    }

    /// Translates an Operation into iroh-docs operations (set_bytes/del).
    /// Returns a synthetic Entry for compatibility with the Store trait.
    async fn add_operation(
        &self,
        op: Operation,
        _on_progress_callback: Option<tokio::sync::mpsc::Sender<crate::log::entry::Entry>>,
    ) -> std::result::Result<crate::log::entry::Entry, Self::Error> {
        // Canonical write path for the public DocumentStore API: enforce read-only here too.
        self.ensure_writable()?;

        let key = op.key().cloned().unwrap_or_default();

        match op.op() {
            "PUT" => {
                // The operation carries marshalled document bytes, so it goes through the
                // codec exactly like the other write paths.
                let stored = self.encode_value(&key, op.value().to_vec())?;
                self.docs
                    .set_bytes(
                        &self.doc_handle,
                        self.author_id,
                        Bytes::from(key.clone().into_bytes()),
                        Bytes::from(stored.clone()),
                    )
                    .await?;
                self.index.insert(key, stored);
            }
            "DEL" => {
                self.docs
                    .del(
                        &self.doc_handle,
                        self.author_id,
                        Bytes::from(key.clone().into_bytes()),
                    )
                    .await?;
                self.index.remove(&key);
            }
            other => {
                return Err(GuardianError::Store(format!(
                    "Unknown operation: {}",
                    other
                )));
            }
        }

        // Create a synthetic Entry for compatibility.
        let payload = crate::guardian::serializer::serialize(&op).unwrap_or_default();
        let clock = LamportClock::new(self.identity.pub_key());
        let entry_arc = crate::log::entry::Entry::create(
            &self.client,
            (*self.identity).clone(),
            "",
            &payload,
            &[],
            Some(clock),
        );
        let entry = (*entry_arc).clone();
        Ok(entry)
    }

    fn span(&self) -> Arc<tracing::Span> {
        Arc::new(self.span.clone())
    }

    fn tracer(&self) -> Arc<TracerWrapper> {
        self.tracer.clone()
    }

    fn event_bus(&self) -> Arc<EventBus> {
        self.event_bus.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl GuardianDBDocumentStore {
    /// Returns a reference to the tracing span used for instrumentation.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Returns the NamespaceId of the underlying iroh-docs document.
    pub fn namespace_id(&self) -> iroh_docs::NamespaceId {
        self.doc_handle.id()
    }

    /// Generates a `DocTicket` (with write capability) for this store's iroh-docs namespace.
    /// The peer that receives the ticket can import the same namespace and replicate securely.
    ///
    /// Note: this grants write capability (carries the namespace secret). For role-gated
    /// sharing, the automatic ticket exchange hands out read or write tickets per requester
    /// (see [`GuardianDBDocumentStore::share_tickets`]).
    pub async fn share_ticket(&self) -> Result<String> {
        let ticket = self.docs.share_doc(&self.doc_handle, true).await?;
        Ok(ticket.to_string())
    }

    /// Generates both the read-only and read-write `DocTicket`s for this store's namespace.
    ///
    /// Returns `(read_ticket, write_ticket)`. The read ticket carries only the namespace
    /// public key (no write secret); the write ticket carries the namespace secret.
    pub async fn share_tickets(&self) -> Result<(String, String)> {
        let read_ticket = self.docs.share_doc(&self.doc_handle, false).await?;
        let write_ticket = self.docs.share_doc(&self.doc_handle, true).await?;
        Ok((read_ticket.to_string(), write_ticket.to_string()))
    }

    /// Returns whether this replica may originate writes.
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Fails fast if this replica is read-only, so callers get a clear error instead of
    /// producing entries that remote peers would silently reject.
    fn ensure_writable(&self) -> Result<()> {
        if self.writable {
            Ok(())
        } else {
            Err(GuardianError::Store(
                "store is read-only: this replica cannot originate writes".to_string(),
            ))
        }
    }

    /// Persists the replica's writability flag (best-effort).
    async fn persist_writable(cache: &dyn Datastore, writable: bool) {
        if let Err(e) = cache.put(WRITABLE_CACHE_KEY, &[writable as u8]).await {
            warn!("Failed to persist writability flag: {:?}", e);
        }
    }

    /// Loads the replica's writability flag, defaulting to `true` for legacy stores that
    /// predate the flag (they were always write-capable).
    async fn load_writable(cache: &dyn Datastore) -> bool {
        match cache.get(WRITABLE_CACHE_KEY).await {
            Ok(Some(bytes)) if !bytes.is_empty() => bytes[0] != 0,
            _ => true,
        }
    }

    /// Returns the AuthorId used for write operations.
    pub fn author_id(&self) -> AuthorId {
        self.author_id
    }

    /// Synchronizes the local index with the current state of the iroh-docs document.
    ///
    /// Queries all document entries and rebuilds the in-memory index.
    pub async fn sync_index_from_docs(&self) -> Result<usize> {
        refresh_doc_index(&self.docs, &self.doc_handle, &self.client, &self.index).await
    }

    /// Starts a background task that keeps the in-memory index synchronized with the
    /// iroh-docs namespace as REMOTE documents arrive via Willow sync.
    fn spawn_live_index_sync(&self) {
        let docs = self.docs.clone();
        let doc = self.doc_handle.clone();
        let client = self.client.clone();
        let index = self.index.clone();

        tokio::spawn(async move {
            let mut stream = match doc.subscribe().await {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        "Failed to subscribe to iroh-docs doc events (Document): {:?}",
                        e
                    );
                    return;
                }
            };

            use futures::StreamExt;
            use iroh_docs::engine::LiveEvent;
            while let Some(event) = stream.next().await {
                // Rebuild the index ONLY on REMOTE-origin events (peer sync).
                // Local writes update the index directly (`put_impl`), so
                // rebuilding on them would be pure waste.
                //
                // THIS DOES NOT AVOID THE RACE WITH A LOCAL WRITE, and the
                // line here used to claim it did. A remote event can arrive
                // between a local write and the read that follows it, and the
                // rebuild then overwrites the local key with whatever the
                // document holds. Closing that needs a generation counter at
                // `put_impl`/`del` or a rebuild lock; both were on the table
                // for `LI` and both were declined as a second, separate
                // decision (`LI §0.5`, `§7`).
                let is_remote = matches!(
                    event,
                    Ok(LiveEvent::InsertRemote { .. })
                        | Ok(LiveEvent::ContentReady { .. })
                        | Ok(LiveEvent::PendingContentReady)
                        | Ok(LiveEvent::SyncFinished(_))
                );
                if is_remote && let Err(e) = refresh_doc_index(&docs, &doc, &client, &index).await {
                    warn!("Failed to update Document index via live sync: {:?}", e);
                }
            }
            debug!("Live index sync terminated for Document store");
        });
    }

    #[instrument(level = "debug", skip(client, identity, options))]
    pub async fn new(
        client: Arc<IrohClient>,
        identity: Arc<Identity>,
        addr: Arc<dyn Address>,
        mut options: NewStoreOptions,
    ) -> Result<Self> {
        // 1. If store-specific options are not provided, use the default for
        //    documents with an "_id" key.
        if options.store_specific_opts.is_none() {
            let default_opts = default_store_opts_for_map("_id");
            options.store_specific_opts = Some(Box::new(default_opts));
        }

        // 2. Downcast the specific options to the expected type.
        let specific_opts_box = options.store_specific_opts.take().ok_or_else(|| {
            GuardianError::InvalidArgument("StoreSpecificOpts is required".to_string())
        })?;
        let doc_opts = *specific_opts_box
            .downcast::<CreateDocumentDBOptions>()
            .map_err(|_| {
                GuardianError::InvalidArgument(
                    "Invalid type provided for opts.StoreSpecificOpts".to_string(),
                )
            })?;

        // --- 3. Initialize iroh-docs ---

        if !client.has_docs_client().await {
            client.init_docs().await.map_err(|e| {
                GuardianError::Store(format!("Failed to initialize iroh-docs: {}", e))
            })?;
        }

        let mut docs = client.docs_client().await.ok_or_else(|| {
            GuardianError::Store("iroh-docs not available after initialization".to_string())
        })?;

        // --- 4. Get the AuthorId ---

        let author_id = docs.get_or_init_author().await.map_err(|e| {
            GuardianError::Store(format!("Failed to initialize iroh-docs author: {}", e))
        })?;

        // --- 5. Configure the components ---

        let db_name = addr.get_path().to_string();
        let span = tracing::info_span!("document_store", address = %addr.to_string());

        let event_bus = Arc::new(options.event_bus.unwrap_or_default());

        let access_controller = options.access_controller.unwrap_or_else(|| {
            let mut default_access = HashMap::new();
            default_access.insert("write".to_string(), vec!["*".to_string()]);
            Arc::new(SimpleAccessController::new(Some(default_access))) as Arc<dyn AccessController>
        });
        // Clone to register the ticket provider (gate for who can replicate).
        let access_controller_for_registry = access_controller.clone();
        // Ticket exchange key = the store NAME (last segment of the address), consistent
        // across nodes (same as the gossip topic). Captured before `addr` is moved.
        let store_key = addr
            .to_string()
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();

        let tracer = options.tracer.unwrap_or_else(|| {
            Arc::new(TracerWrapper::Noop(
                NoopTracerProvider::new().tracer("berty.guardian-db"),
            ))
        });

        let cache: Arc<dyn Datastore> = if let Some(cache) = options.cache {
            cache
        } else {
            let cache_dir = if options.directory.is_empty() {
                format!("./GuardianDB/{}/cache", addr)
            } else {
                format!("{}/cache", options.directory)
            };
            Self::create_cache(addr.as_ref(), &cache_dir)?
        };

        let emitter_interface: Arc<dyn crate::events::EmitterInterface + Send + Sync> =
            Arc::new(EventEmitter::new());

        // --- 6. Create, open or import the iroh-docs document ---

        // A node opened read-only must never create a namespace (it would mint its own write
        // secret and become an isolated writer). It may only import an existing one.
        let requested_read_only = options.read_only.unwrap_or(false);

        // Resolve the DocTicket: explicit (options) takes priority; otherwise try AUTOMATIC EXCHANGE
        // with known peers (joining the shared namespace of an authorized peer).
        let resolved_ticket: Option<String> = match options.doc_ticket.clone() {
            Some(t) => Some(t),
            None => client.backend().resolve_shared_ticket(&store_key).await,
        };

        // Establish the document, tracking whether this replica holds the namespace write
        // secret (`doc_is_writable`). If a DocTicket was resolved, import the peer's SHARED
        // namespace (secure replication via capability). Otherwise, create/reopen locally.
        let (doc_handle, doc_is_writable) = if let Some(ticket_str) = resolved_ticket.as_ref() {
            let ticket = ticket_str
                .parse::<iroh_docs::DocTicket>()
                .map_err(|e| GuardianError::Store(format!("Invalid DocTicket: {}", e)))?;
            // The ticket's capability determines whether we receive the write secret.
            let ticket_writable = matches!(ticket.capability, Capability::Write(_));
            let doc = docs.import_doc(ticket).await?;
            let ns_id = doc.id();
            cache
                .put(NAMESPACE_CACHE_KEY, ns_id.as_bytes())
                .await
                .map_err(|e| {
                    GuardianError::Store(format!("Failed to persist imported NamespaceId: {}", e))
                })?;
            Self::persist_writable(cache.as_ref(), ticket_writable).await;
            info!(
                writable = ticket_writable,
                "Imported shared iroh-docs document via ticket: {:?}", ns_id
            );
            (doc, ticket_writable)
        } else {
            match cache.get(NAMESPACE_CACHE_KEY).await {
                Ok(Some(namespace_bytes)) if namespace_bytes.len() == 32 => {
                    let mut ns_bytes = [0u8; 32];
                    ns_bytes.copy_from_slice(&namespace_bytes);
                    let namespace_id = iroh_docs::NamespaceId::from(ns_bytes);

                    match docs.open_doc(namespace_id).await? {
                        Some(doc) => {
                            // Writability recorded when the namespace was established; legacy
                            // stores without the flag are assumed write-capable.
                            let writable = Self::load_writable(cache.as_ref()).await;
                            info!(
                                writable,
                                "Reopened existing iroh-docs document: {:?}", namespace_id
                            );
                            (doc, writable)
                        }
                        None if requested_read_only => {
                            return Err(GuardianError::Store(format!(
                                "Read-only store '{}' cannot create a namespace and the cached \
                                 namespace {:?} was not found; no ticket available to import",
                                store_key, namespace_id
                            )));
                        }
                        None => {
                            warn!(
                                "Cached namespace {:?} not found, creating new document",
                                namespace_id
                            );
                            let doc = docs.create_doc().await?;
                            let ns_id = doc.id();
                            cache
                                .put(NAMESPACE_CACHE_KEY, ns_id.as_bytes())
                                .await
                                .map_err(|e| {
                                    GuardianError::Store(format!(
                                        "Failed to persist NamespaceId: {}",
                                        e
                                    ))
                                })?;
                            Self::persist_writable(cache.as_ref(), true).await;
                            info!("Created new iroh-docs document: {:?}", ns_id);
                            (doc, true)
                        }
                    }
                }
                _ if requested_read_only => {
                    return Err(GuardianError::Store(format!(
                        "Read-only store '{}' cannot create a namespace and none was available \
                         to import (no ticket, no cached namespace)",
                        store_key
                    )));
                }
                _ => {
                    let doc = docs.create_doc().await?;
                    let ns_id = doc.id();
                    cache
                        .put(NAMESPACE_CACHE_KEY, ns_id.as_bytes())
                        .await
                        .map_err(|e| {
                            GuardianError::Store(format!("Failed to persist NamespaceId: {}", e))
                        })?;
                    Self::persist_writable(cache.as_ref(), true).await;
                    info!("Created new iroh-docs document: {:?}", ns_id);
                    (doc, true)
                }
            }
        };

        // Effective writability: a node explicitly opened read-only never writes, even if it
        // happens to hold a write-capable namespace (defense in depth).
        let writable = doc_is_writable && !requested_read_only;

        // --- 7. Create an empty Log for compatibility with the Store trait ---

        let empty_log = {
            use crate::log::{AdHocAccess, Log, LogOptions};
            let log_opts = LogOptions {
                id: Some(&db_name),
                access: AdHocAccess,
                entries: &[],
                heads: &[],
                clock: None,
                sort_fn: None,
            };
            Arc::new(RwLock::new(Log::new(
                client.clone(),
                (*identity).clone(),
                log_opts,
            )))
        };

        // --- 8. Initialize the BlobStore (optional, for large attachments) ---

        let blob_store = client.blobs_client().await.map(Arc::new);

        // --- 9. Create the instance and synchronize the index ---

        let index = Arc::new(DocumentStoreIndex::new());
        // Address trait already requires Send + Sync, so this coercion is safe
        let cached_address: Arc<dyn Address + Send + Sync> = addr as Arc<dyn Address + Send + Sync>;

        let store = GuardianDBDocumentStore {
            docs,
            doc_handle,
            author_id,
            access_controller,
            event_bus,
            client,
            identity,
            cached_address,
            db_name,
            cache,
            index,
            doc_opts,
            span,
            tracer,
            emitter_interface,
            empty_log,
            blob_store,
            writable,
            payload_codec: codec_or_identity(options.payload_codec),
        };

        // Synchronize the local index with the iroh-docs document state.
        match store.sync_index_from_docs().await {
            Ok(count) => {
                if count > 0 {
                    info!(
                        "DocumentStore initialized with {} entries from iroh-docs",
                        count
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Failed to sync index on initialization: {:?}. Store will start empty.",
                    e
                );
            }
        }

        // Start the reactive live sync: keeps the index updated as remote documents
        // arrive via Willow sync (essential for P2P replication to reflect in get()/query()).
        store.spawn_live_index_sync();

        // Register this store as a DocTicket provider for authorized peers (automatic exchange).
        // The capability is gated per requester by the AccessController: write-authorized peers
        // get the write ticket (namespace secret), read-only peers get the read ticket.
        match store.share_tickets().await {
            Ok((read_ticket, write_ticket)) => {
                store
                    .client
                    .backend()
                    .register_ticket_provider(
                        store_key,
                        read_ticket,
                        write_ticket,
                        access_controller_for_registry,
                    )
                    .await;
            }
            Err(e) => {
                warn!(
                    "Failed to generate share tickets, store not registered for exchange: {:?}",
                    e
                );
            }
        }

        info!(
            "GuardianDBDocumentStore initialized with iroh-docs backend (namespace={:?}, author={:?})",
            store.doc_handle.id(),
            store.author_id
        );

        Ok(store)
    }

    /// Returns the raw, still-encoded bytes held in the index for `key`.
    ///
    /// Test-only: lets the suite assert what actually reaches storage, which is the
    /// whole point of a payload codec and is otherwise invisible through the API.
    #[cfg(test)]
    pub(crate) fn stored_value_for_test(&self, key: &str) -> Option<Vec<u8>> {
        self.index.get_value(key)
    }

    /// Injects already-encoded bytes straight into the index, bypassing the codec.
    ///
    /// Test-only: reproduces a record replicated from a peer whose key material this
    /// node does not have, which is otherwise only reachable by standing up two
    /// synchronizing nodes.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn put_raw_for_test(&self, key: &str, stored: Vec<u8>) {
        self.index.insert(key.to_string(), stored);
    }

    /// Encodes marshalled document bytes into the form stored in iroh-docs.
    fn encode_value(&self, key: &str, marshalled: Vec<u8>) -> Result<Vec<u8>> {
        let address = self.cached_address.to_string();
        let ctx = RecordCtx::new(&address, key);
        self.payload_codec.encode(&ctx, &marshalled)
    }

    /// Recovers marshalled document bytes from the stored form.
    fn decode_value(&self, key: &str, stored: Vec<u8>) -> Result<Vec<u8>> {
        let address = self.cached_address.to_string();
        let ctx = RecordCtx::new(&address, key);
        self.payload_codec.decode(&ctx, &stored)
    }

    /// Reads a record from the index and decodes it, yielding `None` both when the key is
    /// absent and when this replica cannot decode the value (a namespace may legitimately
    /// hold records encoded with key material this node does not have).
    fn indexed_value(&self, key: &str) -> Option<Vec<u8>> {
        let stored = self.index.get_value(key)?;
        match self.decode_value(key, stored) {
            Ok(plaintext) => Some(plaintext),
            Err(_) => {
                warn!(
                    codec = self.payload_codec.codec_id(),
                    key, "Skipping record that could not be decoded with the configured codec"
                );
                None
            }
        }
    }

    #[instrument(level = "debug", skip(self, opts))]
    pub async fn get(
        &self,
        key: &str,
        opts: Option<DocumentStoreGetOptions>,
    ) -> Result<Vec<Document>> {
        let _entered = self.span.enter();
        let opts = opts.unwrap_or_default();

        let has_multiple_terms = key.contains(' ');
        let mut key_for_search = key.to_string();

        if has_multiple_terms {
            key_for_search = key_for_search.replace('.', " ");
        }
        if opts.case_insensitive {
            key_for_search = key_for_search.to_lowercase();
        }

        let mut documents: Vec<Document> = Vec::new();

        for index_key in self.index.keys() {
            let mut index_key_for_search = index_key.clone();

            if opts.case_insensitive {
                index_key_for_search = index_key_for_search.to_lowercase();
                if has_multiple_terms {
                    index_key_for_search = index_key_for_search.replace('.', " ");
                }
            }

            let matches = if opts.partial_matches {
                index_key_for_search.contains(&key_for_search)
            } else {
                index_key_for_search == key_for_search
            };

            if !matches {
                continue;
            }

            if let Some(value_bytes) = self.indexed_value(&index_key) {
                let doc: Document = serde_json::from_slice(&value_bytes).map_err(|e| {
                    GuardianError::Serialization(format!(
                        "Unable to deserialize the value for key {}: {}",
                        index_key, e
                    ))
                })?;
                documents.push(doc);
            }
        }

        Ok(documents)
    }

    #[instrument(level = "debug", skip(self, document))]
    pub async fn put(&mut self, document: Document) -> Result<Operation> {
        self.ensure_writable()?;
        let _entered = self.span.enter();

        let key = (self.doc_opts.key_extractor)(&document)?;
        let data = (self.doc_opts.marshal)(&document)?;
        let stored = self.encode_value(&key, data.clone())?;

        // Write directly to iroh-docs (Willow handles sync).
        self.docs
            .set_bytes(
                &self.doc_handle,
                self.author_id,
                Bytes::from(key.clone().into_bytes()),
                Bytes::from(stored.clone()),
            )
            .await
            .map_err(|e| {
                GuardianError::Store(format!("Error writing key '{}' to iroh-docs: {}", key, e))
            })?;

        // Update the local index immediately (stored form).
        self.index.insert(key.clone(), stored);

        debug!("PUT key='{}' ({} bytes) via iroh-docs", key, data.len());

        Ok(Operation::new(Some(key), "PUT".to_string(), Some(data)))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn delete(&mut self, document_id: &str) -> Result<Operation> {
        self.ensure_writable()?;
        let _entered = self.span.enter();

        // Check whether the entry exists in the local index.
        if self.index.get_value(document_id).is_none() {
            return Err(GuardianError::NotFound(format!(
                "No entry with key '{}' in the database",
                document_id
            )));
        }

        // Remove from iroh-docs (Willow handles sync).
        let deleted = self
            .docs
            .del(
                &self.doc_handle,
                self.author_id,
                Bytes::from(document_id.as_bytes().to_vec()),
            )
            .await
            .map_err(|e| {
                GuardianError::Store(format!(
                    "Error deleting key '{}' in iroh-docs: {}",
                    document_id, e
                ))
            })?;

        // Update the local index immediately.
        self.index.remove(document_id);

        debug!(
            "DEL key='{}' ({} entries removed) via iroh-docs",
            document_id, deleted
        );

        Ok(Operation::new(
            Some(document_id.to_string()),
            "DEL".to_string(),
            None,
        ))
    }

    /// `&self` variant of [`put`](Self::put), for callers holding a shared reference
    /// (e.g. the admin RPC over `Arc<dyn Store>`). The write path is interior-mutable
    /// (iroh-docs `set_bytes` + the index's own locking), so no `&mut self` is needed.
    #[instrument(level = "debug", skip(self, document))]
    pub async fn put_impl(&self, document: Document) -> Result<Operation> {
        self.ensure_writable()?;
        let _entered = self.span.enter();

        let key = (self.doc_opts.key_extractor)(&document)?;
        let data = (self.doc_opts.marshal)(&document)?;
        let stored = self.encode_value(&key, data.clone())?;

        self.docs
            .set_bytes(
                &self.doc_handle,
                self.author_id,
                Bytes::from(key.clone().into_bytes()),
                Bytes::from(stored.clone()),
            )
            .await
            .map_err(|e| {
                GuardianError::Store(format!("Error writing key '{}' to iroh-docs: {}", key, e))
            })?;

        self.index.insert(key.clone(), stored);
        Ok(Operation::new(Some(key), "PUT".to_string(), Some(data)))
    }

    /// `&self` variant of [`delete`](Self::delete). See [`put_impl`](Self::put_impl).
    #[instrument(level = "debug", skip(self))]
    pub async fn delete_impl(&self, document_id: &str) -> Result<Operation> {
        self.ensure_writable()?;
        let _entered = self.span.enter();

        if self.index.get_value(document_id).is_none() {
            return Err(GuardianError::NotFound(format!(
                "No entry with key '{}' in the database",
                document_id
            )));
        }

        self.docs
            .del(
                &self.doc_handle,
                self.author_id,
                Bytes::from(document_id.as_bytes().to_vec()),
            )
            .await
            .map_err(|e| {
                GuardianError::Store(format!(
                    "Error deleting key '{}' in iroh-docs: {}",
                    document_id, e
                ))
            })?;

        self.index.remove(document_id);
        Ok(Operation::new(
            Some(document_id.to_string()),
            "DEL".to_string(),
            None,
        ))
    }

    #[instrument(level = "debug", skip(self, documents))]
    pub async fn put_batch(&mut self, documents: Vec<Document>) -> Result<Vec<Operation>> {
        self.ensure_writable()?;
        if documents.is_empty() {
            return Err(GuardianError::InvalidArgument(
                "Nothing to add to the store".to_string(),
            ));
        }

        let mut operations = Vec::new();

        for doc in documents {
            let op = self.put(doc).await?;
            operations.push(op);
        }

        Ok(operations)
    }

    #[instrument(level = "debug", skip(self, documents))]
    pub async fn put_all(&mut self, documents: Vec<Document>) -> Result<Operation> {
        self.ensure_writable()?;
        if documents.is_empty() {
            return Err(GuardianError::InvalidArgument(
                "Nothing to add to the store".to_string(),
            ));
        }

        let mut to_add: Vec<(String, Vec<u8>)> = Vec::new();

        for doc in documents {
            let key = (self.doc_opts.key_extractor)(&doc).map_err(|_| {
                GuardianError::InvalidArgument(
                    "One of the provided documents has no index key".to_string(),
                )
            })?;

            let data = (self.doc_opts.marshal)(&doc).map_err(|_| {
                GuardianError::Serialization(
                    "Could not serialize one of the provided documents".to_string(),
                )
            })?;

            to_add.push((key, data));
        }

        // Each document is an individual set_bytes (iroh-docs has no batch API).
        for (key, data) in &to_add {
            let stored = self.encode_value(key, data.clone())?;
            self.docs
                .set_bytes(
                    &self.doc_handle,
                    self.author_id,
                    Bytes::from(key.clone().into_bytes()),
                    Bytes::from(stored.clone()),
                )
                .await
                .map_err(|e| {
                    GuardianError::Store(format!("Error writing key '{}' to iroh-docs: {}", key, e))
                })?;

            // Update the local index immediately (stored form).
            self.index.insert(key.clone(), stored);
        }

        debug!("PUTALL {} documents via iroh-docs", to_add.len());

        // Return an Operation representing the batch.
        let first_key = to_add.first().map(|(k, _)| k.clone());
        Ok(Operation::new(first_key, "PUTALL".to_string(), None))
    }

    #[instrument(level = "debug", skip(self, filter))]
    pub fn query<F>(&self, mut filter: F) -> Result<Vec<Document>>
    where
        F: FnMut(&Document) -> Result<bool>,
    {
        let mut results: Vec<Document> = Vec::new();

        for index_key in self.index.keys() {
            if let Some(doc_bytes) = self.indexed_value(&index_key) {
                let doc: Document = serde_json::from_slice(&doc_bytes).map_err(|e| {
                    GuardianError::Serialization(format!(
                        "Could not deserialize the document: {}",
                        e
                    ))
                })?;

                if filter(&doc)? {
                    results.push(doc);
                }
            }
        }

        Ok(results)
    }

    /// Every document under `prefix`, read from the iroh-docs document itself.
    ///
    /// The trait method of the same name, on the concrete store — see
    /// `crate::traits::DocumentStore::scan_docs` for what it is FOR. What is
    /// worth saying here is how it differs from its two neighbours:
    ///
    /// - against [`Self::query`]: it never looks at `self.index`, so a rebuild
    ///   running beside it cannot empty its answer;
    /// - against [`refresh_doc_index`]: an entry it cannot fetch or decode is an
    ///   ERROR and not a `warn!` that keeps whatever the index held for the key
    ///   (`L7`: "and a skip" until `LN` Task 25 (8)). For a cache a dropped
    ///   entry is a miss the next read repairs; for a lineage it is an invisible ancestor,
    ///   which is the same fork this whole path exists to prevent — only quieter.
    ///   Same line as `an_unreadable_peer_makes_list_fail_instead_of_shortening_it`.
    ///
    /// An entry with `content_len() == 0` is skipped all the same: that is a
    /// deletion marker and not an unreadable entry, and the two are not the same
    /// thing.
    pub async fn scan_docs(&self, prefix: &str) -> Result<Vec<Document>> {
        let _entered = self.span.enter();

        // `key_prefix` on the SingleLatestPerKey builder and not the other way
        // round: `Query::key_prefix` yields a `QueryBuilder<FlatQuery>`, which has
        // no `single_latest_per_key`. The vendor's own note says the key filter is
        // applied BEFORE the grouping, which is the order this read wants.
        let entries = self
            .docs
            .get_many(
                &self.doc_handle,
                Query::single_latest_per_key().key_prefix(prefix).build(),
            )
            .await?;

        let mut out = Vec::with_capacity(entries.len());
        for entry in &entries {
            if entry.content_len() == 0 {
                continue;
            }
            let key = String::from_utf8_lossy(entry.key()).to_string();
            let hash_str = entry.content_hash().to_hex();
            let stored = self.client.cat_bytes(&hash_str).await?;
            let plaintext = self.decode_value(&key, stored)?;
            let document: Document = serde_json::from_slice(&plaintext).map_err(|e| {
                GuardianError::Serialization(format!(
                    "Could not deserialize the document under '{}': {}",
                    key, e
                ))
            })?;
            out.push(document);
        }

        debug!("SCAN prefix='{}' → {} documents via iroh-docs", prefix, out.len());
        Ok(out)
    }

    pub fn store_type(&self) -> &'static str {
        "document"
    }

    /// Creates a sled-based cache to persist the NamespaceId.
    fn create_cache(address: &dyn Address, cache_dir: &str) -> Result<Arc<dyn Datastore>> {
        use crate::cache::level_down::LevelDownCache;
        use crate::cache::{Cache, CacheMode, Options};

        let cache_options = Options {
            span: None,
            max_cache_size: Some(100 * 1024 * 1024),
            cache_mode: CacheMode::Auto,
        };

        let cache_manager = LevelDownCache::new(Some(&cache_options));
        let address_string = address.to_string();
        let parsed_address = crate::address::parse(&address_string)
            .map_err(|e| GuardianError::Store(format!("Failed to parse address: {}", e)))?;

        let boxed_datastore = cache_manager
            .load(cache_dir, &parsed_address)
            .map_err(|e| GuardianError::Store(format!("Failed to create cache: {}", e)))?;

        struct DatastoreWrapper {
            inner: Box<dyn Datastore + Send + Sync>,
        }

        #[async_trait::async_trait]
        impl Datastore for DatastoreWrapper {
            async fn get(&self, key: &[u8]) -> crate::guardian::error::Result<Option<Vec<u8>>> {
                self.inner.get(key).await
            }
            async fn put(&self, key: &[u8], value: &[u8]) -> crate::guardian::error::Result<()> {
                self.inner.put(key, value).await
            }
            async fn has(&self, key: &[u8]) -> crate::guardian::error::Result<bool> {
                self.inner.has(key).await
            }
            async fn delete(&self, key: &[u8]) -> crate::guardian::error::Result<()> {
                self.inner.delete(key).await
            }
            async fn query(
                &self,
                query: &crate::data_store::Query,
            ) -> crate::guardian::error::Result<crate::data_store::Results> {
                self.inner.query(query).await
            }
            async fn list_keys(
                &self,
                prefix: &[u8],
            ) -> crate::guardian::error::Result<Vec<crate::data_store::Key>> {
                self.inner.list_keys(prefix).await
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        Ok(Arc::new(DatastoreWrapper {
            inner: boxed_datastore,
        }))
    }
}

/// Returns a closure that extracts a field from a `serde_json::Value::Object`.
///
/// The returned closure captures the `key_field` for later use.
pub fn map_key_extractor(key_field: String) -> impl Fn(&Document) -> Result<String> {
    move |doc: &Document| {
        // Ensure the document is a JSON object (map).
        let obj = doc.as_object().ok_or_else(|| {
            GuardianError::InvalidArgument(
                "The entry must be a JSON object (map[string]interface{{}})".to_string(),
            )
        })?;

        // Look up the key field in the object.
        let value = obj.get(&key_field).ok_or_else(|| {
            GuardianError::NotFound(format!(
                "Missing value for field `{}` in the entry",
                key_field
            ))
        })?;

        // Ensure the found value is a string.
        let key = value.as_str().ok_or_else(|| {
            GuardianError::InvalidArgument(format!(
                "The value for field `{}` is not a string",
                key_field
            ))
        })?;

        // Validate that the key is not empty.
        if key.is_empty() {
            return Err(GuardianError::InvalidArgument(format!(
                "The field `{}` cannot be an empty string",
                key_field
            )));
        }

        Ok(key.to_string())
    }
}

/// Creates a default set of options for a store that handles map-based documents
/// (JSON Objects), using a specific field as the key.
pub fn default_store_opts_for_map(key_field: &str) -> CreateDocumentDBOptions {
    CreateDocumentDBOptions {
        marshal: Arc::new(|doc: &Document| serde_json::to_vec(doc).map_err(GuardianError::from)),
        unmarshal: Arc::new(|bytes: &[u8]| {
            serde_json::from_slice(bytes).map_err(GuardianError::from)
        }),
        // Use the higher-order function to create the key-extractor closure.
        key_extractor: Arc::new(map_key_extractor(key_field.to_string())),

        item_factory: Arc::new(|| Value::Object(Map::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hand_built_clear_and_refill_loses_a_visible_key_the_swap_never_does() {
        // TWO HALVES IN ONE FUNCTION, so no fixture difference carries the
        // statement: the same index type, the same key, the same value —
        // only the way the map is replaced moves.
        //
        // The first half is the state the OLD path produces, built by
        // hand. It is not a test of dead code: it is what names that
        // state instead of describing it, and it is what stays behind as
        // the red proof once the production path stops producing it
        // (`LI §4 P3`).
        let index = Arc::new(DocumentStoreIndex::new());
        index.insert("k".to_string(), b"VISIBLE".to_vec());
        assert_eq!(index.get_value("k"), Some(b"VISIBLE".to_vec()));

        // HAND-BUILT, the old way: clear first, refill after.
        index.clear_all();
        assert_eq!(
            index.get_value("k"),
            None,
            "between the clear and the refill a key that WAS visible is gone \
             — and every reader polling the index sees exactly this"
        );
        index.insert("k".to_string(), b"VISIBLE".to_vec());

        // THE PRODUCTION WAY: one write, and the key never leaves.
        let mut next = HashMap::new();
        next.insert("k".to_string(), b"REBUILT".to_vec());
        index.replace_all(next);
        assert_eq!(
            index.get_value("k"),
            Some(b"REBUILT".to_vec()),
            "the swap is atomic against a reader: old value or new value, \
             never nothing"
        );
    }

    #[tokio::test]
    async fn a_key_stays_readable_while_the_rebuild_is_held_mid_flight() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // DETERMINISTIC, and that is the whole design of this gate: the
        // test controls the ORDER through a channel, so there is no
        // timing race and no "at N large enough it falls over". The
        // rebuild stops between entry 1 and entry 2 because the test says
        // so, and the reading happens while it stands there.
        let index = Arc::new(DocumentStoreIndex::new());
        index.insert("a".to_string(), b"OLD-A".to_vec());
        index.insert("b".to_string(), b"OLD-B".to_vec());

        let (reached_tx, mut reached_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let calls = Arc::new(AtomicUsize::new(0));

        let fetch = move |hash: String| {
            let reached_tx = reached_tx.clone();
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            async move {
                // The SECOND entry: the first is already in the next map,
                // so the rebuild is genuinely mid-flight when it stops.
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    reached_tx.send(()).await.unwrap();
                    let rx = release.lock().await.take().unwrap();
                    rx.await.unwrap();
                }
                Ok(format!("NEW-{hash}").into_bytes())
            }
        };

        let index_for_task = Arc::clone(&index);
        let task = tokio::spawn(async move {
            rebuild_from(
                &index_for_task,
                vec![
                    ("a".to_string(), "a".to_string()),
                    ("b".to_string(), "b".to_string()),
                ],
                fetch,
            )
            .await
        });

        reached_rx.recv().await.unwrap();

        // THE MEASUREMENT. The rebuild stands between its two entries.
        // Against the OLD form — `clear_all` before the loop — both of
        // these would be `None`, and a reader polling here would see an
        // index that had lost two keys it saw a moment ago.
        assert_eq!(
            index.get_value("a"),
            Some(b"OLD-A".to_vec()),
            "held mid-flight, the index still answers the COMPLETE old state"
        );
        assert_eq!(index.get_value("b"), Some(b"OLD-B".to_vec()));

        release_tx.send(()).unwrap();
        assert_eq!(task.await.unwrap(), 2, "both entries were fetched");
        assert_eq!(index.get_value("a"), Some(b"NEW-a".to_vec()));
        assert_eq!(index.get_value("b"), Some(b"NEW-b".to_vec()));
    }

    // THE THREE GATES BELOW CALL THE LOOP PRODUCTION RUNS. `LI`'s gates
    // held a building block nobody called (`LI §9`, the mutation probe that
    // left 902/902 green); `refresh_in_place` is the body of
    // `refresh_doc_index` minus the `get_many` and the mapping of its
    // entries, so a `clear_all` put back into it turns these red. The part
    // they cannot reach — the snapshot itself — is held from the outside by
    // `tests/doc_index_refresh.rs` through `Store::load`.

    #[tokio::test]
    async fn a_key_whose_fetch_fails_keeps_its_previous_value() {
        // The state F3 and F4 meet after a restart: the document already
        // names a newer entry for a key whose blob is not local yet. The old
        // form cleared the key and then could not refill it, so for the whole
        // of that rebuild the store read as holding no value at all.
        let index = DocumentStoreIndex::new();
        index.insert("a".to_string(), b"OLD-A".to_vec());
        index.insert("b".to_string(), b"OLD-B".to_vec());

        let fetch = |hash: String| async move {
            if hash == "a" {
                Err(GuardianError::Store("blob not local yet".to_string()))
            } else {
                Ok(format!("NEW-{hash}").into_bytes())
            }
        };
        let count = refresh_in_place(
            &index,
            vec![
                ("a".to_string(), Some("a".to_string())),
                ("b".to_string(), Some("b".to_string())),
            ],
            fetch,
        )
        .await;

        assert_eq!(count, 1, "only the fetch that returned counts");
        assert_eq!(
            index.get_value("a"),
            Some(b"OLD-A".to_vec()),
            "a failed fetch keeps what the index held — stale, but not absent"
        );
        assert_eq!(index.get_value("b"), Some(b"NEW-b".to_vec()));
    }

    #[tokio::test]
    async fn a_key_gone_from_the_document_leaves_the_index() {
        // The other half of "no clear": without it nothing removes a key,
        // so the refresh has to do it for the two ways a key leaves — a
        // deletion marker (`content_len() == 0`, here `None`) and no entry
        // in the snapshot at all.
        let index = DocumentStoreIndex::new();
        index.insert("kept".to_string(), b"OLD".to_vec());
        index.insert("deleted".to_string(), b"OLD".to_vec());
        index.insert("vanished".to_string(), b"OLD".to_vec());

        let fetch = |hash: String| async move { Ok(format!("NEW-{hash}").into_bytes()) };
        refresh_in_place(
            &index,
            vec![
                ("kept".to_string(), Some("kept".to_string())),
                ("deleted".to_string(), None),
            ],
            fetch,
        )
        .await;

        assert_eq!(index.get_value("kept"), Some(b"NEW-kept".to_vec()));
        assert_eq!(index.get_value("deleted"), None, "a deletion marker removes");
        assert_eq!(index.get_value("vanished"), None, "absence removes");
        assert_eq!(index.len(), 1);
    }

    #[tokio::test]
    async fn a_refresh_held_mid_flight_leaves_no_key_absent_and_a_local_write_standing() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // `LI T2`'s channel, pointed at the loop production runs. Held
        // between entry 1 and entry 2, the index has to show THREE things at
        // once: the fetched key already NEW (what the swap lost, `LI §10.6`),
        // the unfetched key still OLD (what the clear lost), and neither
        // absent.
        let index = Arc::new(DocumentStoreIndex::new());
        index.insert("a".to_string(), b"OLD-A".to_vec());
        index.insert("b".to_string(), b"OLD-B".to_vec());

        let (reached_tx, mut reached_rx) = tokio::sync::mpsc::channel::<()>(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let calls = Arc::new(AtomicUsize::new(0));

        let fetch = move |hash: String| {
            let reached_tx = reached_tx.clone();
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    reached_tx.send(()).await.unwrap();
                    let rx = release.lock().await.take().unwrap();
                    rx.await.unwrap();
                }
                Ok(format!("NEW-{hash}").into_bytes())
            }
        };

        let index_for_task = Arc::clone(&index);
        let task = tokio::spawn(async move {
            refresh_in_place(
                &index_for_task,
                vec![
                    ("a".to_string(), Some("a".to_string())),
                    ("b".to_string(), Some("b".to_string())),
                ],
                fetch,
            )
            .await
        });

        reached_rx.recv().await.unwrap();

        assert_eq!(
            index.get_value("a"),
            Some(b"NEW-a".to_vec()),
            "a key is visible the moment its own fetch returns"
        );
        assert_eq!(
            index.get_value("b"),
            Some(b"OLD-B".to_vec()),
            "a key not fetched yet still answers its old value, never nothing"
        );

        // A LOCAL WRITE landing mid-flight — `put_impl` sets the index
        // directly — is not in the snapshot. It must outlive the refresh,
        // as it did under the clear, whose clear sat before the first
        // `await`: pruning at the END would widen the local-write race the
        // way the swap did (`LI §10.9` finding 5).
        index.insert("local".to_string(), b"LOCAL".to_vec());

        release_tx.send(()).unwrap();
        assert_eq!(task.await.unwrap(), 2);
        assert_eq!(index.get_value("b"), Some(b"NEW-b".to_vec()));
        assert_eq!(
            index.get_value("local"),
            Some(b"LOCAL".to_vec()),
            "a key written during the refresh survives it"
        );
    }
}
