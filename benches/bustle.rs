//! Implementation of the Bustle API. For benchmarking purposes.

use std::{
    cell::OnceCell,
    collections::{hash_map::Entry, HashMap}, sync::{Arc, OnceLock},
};

use bustle::{Collection, CollectionHandle};
use parking_lot::RwLock;
use serde_json::json;

use mmap_payload_storage::{payload::Payload, PayloadStorage};

/// A storage that includes an external to internal id tracker.
pub struct StorageFixture {
    storage: PayloadStorage,
    id_tracker: HashMap<u64, u32>,
    max_internal_id: u32,
    payload_picker: PayloadPicker,
}

impl Collection for StorageFixture {
    type Handle = StorageFixture;

    fn with_capacity(capacity: usize) -> Self {
        let storage = PayloadStorage::new(tempfile::tempdir().path().to_path_buf(), None);
        StorageFixture {
            storage,
            id_tracker: HashMap::with_capacity(capacity),
            max_internal_id: 0,
            payload_picker: PayloadPicker::new(),
        }
    }

    fn pin(&self) -> Self::Handle {
        self
    }
}

impl CollectionHandle for StorageFixture {
    type Key = u64;

    fn get(&mut self, key: &Self::Key) -> bool {
        let Some(internal) = self.id_tracker.get(key) else {
            return false;
        };

        self.storage.get_payload(*internal).is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        let internal_id = match self.id_tracker.entry(*key) {
            Entry::Occupied(occupied_entry) => *occupied_entry.get(),
            Entry::Vacant(vacant_entry) => {
                let internal_id = self.max_internal_id;
                self.max_internal_id += 1;
                *vacant_entry.insert(internal_id)
            }
        };
        let payload = self.payload_picker.pick(internal_id);
        !self.storage.put_payload(internal_id, payload)
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        let internal_id = match self.id_tracker.remove(key) {
            Some(internal_id) => internal_id,
            None => return false,
        };

        self.storage.delete_payload(internal_id).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        let Some(internal_id) = self.id_tracker.get(key) else {
            return false;
        };
        let payload = self.payload_picker.pick(*internal_id);
        self.storage.put_payload(*internal_id, payload)
    }
}

struct PayloadPicker {
    payloads: OnceLock<Vec<Payload>>,
}

impl PayloadPicker {
    fn pick(&self, internal_id: u32) -> &Payload {
        let payloads = self.payloads.get_or_init(|| {
           [
                json!({"name": "Alice", "age": 30, "city": "Wonderland"}),
                json!({"name": "Bob", "age": 25, "city": "Builderland", "occupation": "Builder"}),
                json!({"name": "Charlie", "age": 35, "city": "Chocolate Factory", "hobbies": ["Inventing", "Exploring"]}),
                json!({"name": "Dave", "age": 40, "city": "Dinosaur Land", "favorite_dinosaur": "T-Rex"}),
                json!({"name": "Eve", "age": 28, "city": "Eden", "skills": ["Gardening", "Cooking", "Singing"]}),
                json!({"name": "Frank", "age": 33, "city": "Fantasy Island", "adventures": ["Treasure Hunt", "Dragon Slaying", "Rescue Mission"]}),
                json!({"name": "Grace", "age": 29, "city": "Gotham", "alias": "Batwoman", "gadgets": ["Batarang", "Grapple Gun", "Smoke Bomb"]}),
                json!({"name": "Hank", "age": 45, "city": "Hogwarts", "house": "Gryffindor", "patronus": "Stag", "wand": {"wood": "Holly", "core": "Phoenix Feather", "length": 11}}),
                json!({"name": "Ivy", "age": 27, "city": "Ivory Tower", "profession": "Scholar", "publications": ["Theories of Magic", "History of the Ancients", "Alchemy and Potions"]}),
                json!({"name": "Jack", "age": 32, "city": "Jack's Beanstalk", "adventures": ["Climbing the Beanstalk", "Meeting the Giant", "Stealing the Golden Goose", "Escaping the Giant", "Living Happily Ever After"]}),
            ].into_iter().map(|value| Payload(value.as_object().unwrap().clone())).collect()
        });

        let pick_idx = internal_id as usize % self.payloads.get().unwrap().len();

        &payloads[pick_idx]
    }
}
