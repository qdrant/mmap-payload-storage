use mmap_payload_storage::fixtures::empty_storage;
use mmap_payload_storage::PayloadStorage;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;
use mmap_payload_storage::bustle_32::{Collection, CollectionHandle, Mix, Workload};

#[derive(Clone)]
struct ArcStorage {
    storage: Arc<RwLock<PayloadStorage>>,
    _dir: Arc<TempDir>,
}

impl Collection for ArcStorage {
    type Handle = Self;

    fn with_capacity(_capacity: usize) -> Self {
        let (dir, storage) = empty_storage();
        Self {
            storage: Arc::new(RwLock::new(storage)),
            _dir: Arc::new(dir),
        }
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for ArcStorage {
    type Key = u32;

    /// Perform a lookup for key.
    /// Should return true if the key is found
    fn get(&mut self, key: &Self::Key) -> bool {
        eprintln!("GET {}", key);
        self.storage.read().get_payload(*key).is_some()
    }

    /// Insert key into the collection.
    /// Should return true if no value previously existed for the key.
    fn insert(&mut self, key: &Self::Key) -> bool {
        eprintln!("INSERT {}", key);
        self.storage.write().put_payload(*key, &Default::default())
    }

    /// Remove key from the collection.
    /// Should return true if the key existed and was removed.
    fn remove(&mut self, key: &Self::Key) -> bool {
        eprintln!("REMOVE {}", key);
        self.storage.write().delete_payload(*key).is_some()
    }

    /// Update the value for key in the collection, if it exists.
    /// Should return true if the key existed and was updated.
    /// Should not insert the key if it did not exist.
    fn update(&mut self, key: &Self::Key) -> bool {
        eprintln!("UPDATE {}", key);
        self.storage.write().update_payload(*key, &Default::default())
    }
}

fn main() {
    // against 2 threads
    for n in 1..=1 {
        Workload::new(n, Mix::read_heavy()).run::<ArcStorage>();
    }
}