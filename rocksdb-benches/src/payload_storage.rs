use std::sync::Arc;

use bustle::Collection;
use mmap_payload_storage::{fixtures::empty_storage, payload::Payload, PayloadStorage};
use parking_lot::RwLock;

use crate::fixture::{ArcStorage, SequentialCollectionHandle, StorageProxy};

impl Collection for ArcStorage<PayloadStorage> {
    type Handle = Self;

    fn with_capacity(_capacity: usize) -> Self {
        let (dir, storage) = empty_storage();

        let proxy = StorageProxy::new(storage);
        ArcStorage {
            proxy: Arc::new(RwLock::new(proxy)),
            _dir: Arc::new(dir),
        }
    }

    fn pin(&self) -> Self::Handle {
        Self {
            proxy: self.proxy.clone(),
            _dir: self._dir.clone(),
        }
    }
}

impl SequentialCollectionHandle for PayloadStorage {
    fn get(&self, key: &u32) -> bool {
        self.get_payload(*key).is_some()
    }

    fn insert(&mut self, key: u32, payload: &Payload) -> bool {
        !self.put_payload(key, payload)
    }

    fn remove(&mut self, key: &u32) -> bool {
        self.delete_payload(*key).is_some()
    }

    fn update(&mut self, key: &u32, payload: &Payload) -> bool {
        self.put_payload(*key, payload)
    }

    fn flush(&self) -> bool {
        self.flush().is_ok()
    }
}
