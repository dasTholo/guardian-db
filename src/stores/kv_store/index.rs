use crate::guardian::error::GuardianError;
use crate::log::{Log, entry::Entry};
use crate::traits::StoreIndex;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// KvIndex maintains an in-memory key-value index for the KvStore.
///
/// In the iroh-docs architecture, this index is a local mirror of the
/// iroh-docs document state. `update_index` is a no-op because the index
/// is updated directly by the put/delete operations.
pub struct KvIndex {
    index: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl Default for KvIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl KvIndex {
    /// Creates a new KvIndex instance.
    pub fn new() -> Self {
        KvIndex {
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl StoreIndex for KvIndex {
    type Error = GuardianError;

    fn contains_key(&self, key: &str) -> std::result::Result<bool, Self::Error> {
        let index = self.index.read();
        Ok(index.contains_key(key))
    }

    fn get_bytes(&self, key: &str) -> std::result::Result<Option<Vec<u8>>, Self::Error> {
        let index = self.index.read();
        Ok(index.get(key).cloned())
    }

    fn keys(&self) -> std::result::Result<Vec<String>, Self::Error> {
        let index = self.index.read();
        Ok(index.keys().cloned().collect())
    }

    fn keys_with_prefix(&self, prefix: &str) -> std::result::Result<Vec<String>, Self::Error> {
        let index = self.index.read();
        Ok(index
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn len(&self) -> std::result::Result<usize, Self::Error> {
        let index = self.index.read();
        Ok(index.len())
    }

    fn is_empty(&self) -> std::result::Result<bool, Self::Error> {
        let index = self.index.read();
        Ok(index.is_empty())
    }

    /// No-op for iroh-docs — the local index is updated directly
    /// by the put/delete operations in GuardianDBKeyValue.
    fn update_index(
        &mut self,
        _oplog: &Log,
        _entries: &[Entry],
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn clear(&mut self) -> std::result::Result<(), Self::Error> {
        let mut index = self.index.write();
        index.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_with_prefix_filters_as_keys_does() {
        // Noosphere `CN` K6: the override filters before it clones and
        // answers exactly the keys `keys()` holds under the prefix.
        let index = KvIndex::new();
        for key in ["a/1", "a/2", "a", "b/1", "ab/1"] {
            index.index.write().insert(key.to_string(), Vec::new());
        }
        for prefix in ["a/", "a", "b/", "", "c/"] {
            let mut got = index.keys_with_prefix(prefix).unwrap();
            got.sort_unstable();
            let mut want: Vec<String> = index
                .keys()
                .unwrap()
                .into_iter()
                .filter(|k| k.starts_with(prefix))
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "{prefix:?}");
        }
    }
}
