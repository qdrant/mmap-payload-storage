use crate::page_tracker::{PagePointer, PageTracker, PointOffset};
use crate::payload::Payload;
use crate::slotted_page::{SlotHeader, SlottedPageMmap};
use lz4_flex::compress_prepend_size;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub struct PayloadStorage {
    page_tracker: PageTracker,
    pages: HashMap<u32, Arc<RwLock<SlottedPageMmap>>>, // page_id -> mmap page
    max_page_id: u32,
    base_path: PathBuf,
}

impl PayloadStorage {
    /// LZ4 compression
    fn compress(value: &[u8]) -> Vec<u8> {
        compress_prepend_size(value)
    }

    /// LZ4 decompression
    fn decompress(value: &[u8]) -> Vec<u8> {
        lz4_flex::decompress_size_prepended(value).unwrap()
    }

    pub fn new(path: PathBuf) -> Self {
        Self {
            page_tracker: PageTracker::new(&path),
            pages: HashMap::new(),
            max_page_id: 0,
            base_path: path,
        }
    }

    /// Open an existing PayloadStorage at the given path
    /// Returns None if the storage does not exist
    fn open(path: PathBuf) -> Option<Self> {
        let page_tracker = PageTracker::open(&path)?;
        let page_ids = page_tracker.all_page_ids();
        // load pages
        let mut pages = HashMap::new();
        let mut max_page_id: u32 = 0;
        for page_id in page_ids {
            let page_path = &path.join(format!("slotted-paged-{}.dat", page_id));
            if let Some(slotted_page) = SlottedPageMmap::open(page_path) {
                let page = Arc::new(RwLock::new(slotted_page));
                pages.insert(page_id, page);
                if page_id > max_page_id {
                    max_page_id = page_id;
                }
            } else {
                panic!("Page not found");
            }
        }
        Some(Self {
            page_tracker,
            pages,
            max_page_id: 0,
            base_path: path,
        })
    }

    /// Get the path for a given page id
    pub fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path
            .join(format!("slotted-paged-{}.dat", page_id))
    }

    /// Add a page to the storage. If it already exists, returns false
    fn add_page(&mut self, page_id: u32, page: SlottedPageMmap) -> bool {
        let page_exists = self.pages.contains_key(&page_id);
        if page_exists {
            return false;
        }
        let previous = self.pages.insert(page_id, Arc::new(RwLock::new(page)));

        assert!(previous.is_none());

        if page_id > self.max_page_id {
            self.max_page_id = page_id;
        }

        true
    }

    /// Get the payload for a given point offset
    pub fn get_payload(&self, point_offset: PointOffset) -> Option<Payload> {
        let PagePointer { page_id, slot_id } = self.get_mapping(point_offset)?;
        let page = self.pages.get(page_id).expect("page not found");
        let page_guard = page.read();
        let raw = page_guard.get_value(slot_id)?;
        let decompressed = Self::decompress(raw);
        let payload = Payload::from_bytes(&decompressed);
        Some(payload)
    }

    /// Find the best fitting page for a payload
    /// Returns Some(page_id) of the best fitting page or None if no page has enough space
    fn find_best_fitting_page(&self, payload_size: usize) -> Option<u32> {
        if self.pages.is_empty() {
            return None;
        }
        let needed_size =
            SlottedPageMmap::MIN_VALUE_SIZE_BYTES.max(payload_size) + SlotHeader::size_in_bytes();
        let mut best_page = 0;
        // best is the page with the lowest free space that fits the payload
        let mut best_fit_size = usize::MAX;

        for (page_id, page) in &self.pages {
            let free_space = page.read().free_space();
            if free_space >= needed_size && free_space < best_fit_size {
                best_page = *page_id;
                best_fit_size = free_space;
            }
        }

        // no page has enough capacity
        if best_page == 0 {
            None
        } else {
            Some(best_page)
        }
    }

    /// Create a new page and return its id.
    ///
    /// `size_hint` is used to create larger pages if necessary.
    fn create_new_page(&mut self, size_hint: Option<usize>) -> u32 {
        let new_page_id = self.max_page_id + 1;
        let path = self.page_path(new_page_id);
        let was_created = self.add_page(new_page_id, SlottedPageMmap::new(&path, size_hint));

        assert!(was_created);

        new_page_id
    }

    /// Get the mapping for a given point offset
    fn get_mapping(&self, point_offset: PointOffset) -> Option<&PagePointer> {
        self.page_tracker.get(point_offset)
    }

    /// Put a payload in the storage
    pub fn put_payload(&mut self, point_offset: PointOffset, payload: Payload) {
        let payload_bytes = payload.to_bytes();
        let comp_payload = Self::compress(&payload_bytes);
        let payload_size = comp_payload.len();

        if let Some(PagePointer { page_id, slot_id }) = self.get_mapping(point_offset).copied() {
            let page = self.pages.get_mut(&page_id).unwrap();
            let mut page_guard = page.write();
            let updated = page_guard.update_value(slot_id, &comp_payload);
            if !updated {
                // delete slot
                page_guard.delete_value(slot_id);
                drop(page_guard);

                // find a new page (or create a new one if all full)
                let new_page_id = self
                    .find_best_fitting_page(payload_size)
                    .unwrap_or_else(|| {
                        // create a new page
                        self.create_new_page(Some(payload_size))
                    });
                let mut page = self.pages.get_mut(&new_page_id).unwrap().write();
                let new_slot_id = page.insert_value(&comp_payload).unwrap();
                // update page_tracker
                self.page_tracker
                    .set(point_offset, PagePointer::new(new_page_id, new_slot_id));
            }
        } else {
            // this is a new payload
            let page_id = self
                .find_best_fitting_page(payload_size)
                .unwrap_or_else(|| {
                    // create a new page
                    self.create_new_page(Some(payload_size))
                });

            let page = self.pages.get_mut(&page_id).unwrap();
            let slot_id = page.write().insert_value(&comp_payload).unwrap();
            // ensure page_tracker is long enough
            self.page_tracker.ensure_length(point_offset as usize + 1);
            // update page_tracker
            self.page_tracker
                .set(point_offset, PagePointer::new(page_id, slot_id));
        }
    }

    /// Delete a payload from the storage
    /// Returns None if the point_offset, page, or payload was not found
    pub fn delete_payload(&mut self, point_offset: PointOffset) -> Option<()> {
        let PagePointer { page_id, slot_id } = self.get_mapping(point_offset).copied()?;
        let page = self.pages.get_mut(&page_id)?;
        // delete value from page
        page.write().delete_value(slot_id);
        // delete mapping
        self.page_tracker.clear_mapping(point_offset);
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tempfile::{Builder, TempDir};

    use rand::{distributions::Uniform, prelude::Distribution, seq::SliceRandom, Rng};

    fn empty_storage() -> (TempDir, PayloadStorage) {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let storage = PayloadStorage::new(dir.path().to_path_buf());

        assert!(storage.pages.is_empty());
        assert!(storage.page_tracker.is_empty());

        (dir, storage)
    }

    fn random_word(rng: &mut impl Rng) -> String {
        let len = rng.gen_range(1..10);
        let mut word = String::with_capacity(len);
        for _ in 0..len {
            word.push(rng.gen_range(b'a'..=b'z') as char);
        }
        word
    }

    fn one_random_payload_please(rng: &mut impl Rng, size_factor: usize) -> Payload {
        let mut payload = Payload::default();

        let word = random_word(rng);

        let sentence = (0..rng.gen_range(1..20 * size_factor))
            .map(|_| random_word(rng))
            .collect::<Vec<_>>()
            .join(" ");

        let distr = Uniform::new(0, 100000);
        let indices = (0..rng.gen_range(1..100 * size_factor))
            .map(|_| distr.sample(rng))
            .collect::<Vec<_>>();

        payload.0 = serde_json::json!(
            {
                "word": word, // string
                "sentence": sentence, // string
                "number": rng.gen_range(0..1000), // number
                "indices": indices // array of numbers
            }
        )
        .as_object()
        .unwrap()
        .clone();

        payload
    }

    #[test]
    fn test_empty_payload_storage() {
        let (_dir, storage) = empty_storage();
        let payload = storage.get_payload(0);
        assert!(payload.is_none());
    }

    #[test]
    fn test_put_single_empty_payload() {
        let (_dir, mut storage) = empty_storage();

        let payload = Payload::default();
        storage.put_payload(0, payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.raw_mapping_len(), 1);

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), Payload::default());
    }

    #[test]
    fn test_put_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        storage.put_payload(0, payload.clone());
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.raw_mapping_len(), 1);

        let page_mapping = storage.get_mapping(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    fn test_put_payload() {
        let (_dir, mut storage) = empty_storage();

        let rng = &mut rand::thread_rng();

        let mut payloads = (0..100000u32)
            .map(|point_offset| (point_offset, one_random_payload_please(rng, 2)))
            .collect::<Vec<_>>();

        for (point_offset, payload) in payloads.iter() {
            storage.put_payload(*point_offset, payload.clone());

            let stored_payload = storage.get_payload(*point_offset);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload.clone());
        }

        // read randomly
        payloads.shuffle(rng);
        for (point_offset, payload) in payloads.iter() {
            let stored_payload = storage.get_payload(*point_offset);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload.clone());
        }
    }

    #[test]
    fn test_delete_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        storage.put_payload(0, payload.clone());
        assert_eq!(storage.pages.len(), 1);

        let page_mapping = storage.get_mapping(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // delete payload
        storage.delete_payload(0);
        assert_eq!(storage.pages.len(), 1);

        // get payload again
        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_none());
    }

    #[test]
    fn test_update_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        storage.put_payload(0, payload.clone());
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.raw_mapping_len(), 1);

        let page_mapping = storage.get_mapping(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // update payload
        let mut updated_payload = Payload::default();
        updated_payload
            .0
            .insert("key".to_string(), Value::String("updated".to_string()));

        storage.put_payload(0, updated_payload.clone());
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.raw_mapping_len(), 1);

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), updated_payload);
    }

    enum Operation {
        Put(PointOffset, Payload),
        Delete(PointOffset),
        Update(PointOffset, Payload),
    }

    impl Operation {
        fn random(rng: &mut impl Rng, max_point_offset: u32) -> Self {
            let point_offset = rng.gen_range(0..=max_point_offset);
            let operation = rng.gen_range(0..3);
            match operation {
                0 => {
                    let payload = one_random_payload_please(rng, 2);
                    Operation::Put(point_offset, payload)
                }
                1 => Operation::Delete(point_offset),
                2 => {
                    let payload = one_random_payload_please(rng, 2);
                    Operation::Update(point_offset, payload)
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_behave_like_hashmap() {
        let (dir, mut storage) = empty_storage();

        let rng = &mut rand::thread_rng();
        let max_point_offset = 100000u32;

        let mut model_hashmap = HashMap::new();

        let operations = (0..10000u32)
            .map(|_| Operation::random(rng, max_point_offset))
            .collect::<Vec<_>>();

        // apply operations to storage and model_hashmap
        for operation in operations.iter() {
            match operation {
                Operation::Put(point_offset, payload) => {
                    storage.put_payload(*point_offset, payload.clone());
                    model_hashmap.insert(*point_offset, payload.clone());
                }
                Operation::Delete(point_offset) => {
                    storage.delete_payload(*point_offset);
                    model_hashmap.remove(point_offset);
                }
                Operation::Update(point_offset, payload) => {
                    storage.put_payload(*point_offset, payload.clone());
                    model_hashmap.insert(*point_offset, payload.clone());
                }
            }
        }

        // asset same length
        assert_eq!(storage.page_tracker.mapping_len(), model_hashmap.len());

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_payload(point_offset);
            let model_payload = model_hashmap.get(&point_offset);
            assert_eq!(stored_payload.as_ref(), model_payload);
        }

        // drop storage
        drop(storage);

        // reopen storage
        let storage = PayloadStorage::open(dir.path().to_path_buf()).unwrap();

        // asset same length
        assert_eq!(storage.page_tracker.mapping_len(), model_hashmap.len());

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_payload(point_offset);
            let model_payload = model_hashmap.get(&point_offset);
            assert_eq!(
                stored_payload.as_ref(),
                model_payload,
                "failed for point_offset: {}",
                point_offset
            );
        }
    }

    #[test]
    fn test_put_huge_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        let huge_payload_size = 1024 * 1024 * 50; // 50MB

        let distr = Uniform::new('a', 'z');
        let rng = rand::thread_rng();

        let huge_value = Value::String(distr.sample_iter(rng).take(huge_payload_size).collect());
        payload.0.insert("huge".to_string(), huge_value);

        storage.put_payload(0, payload.clone());
        assert_eq!(storage.pages.len(), 1);

        let page_mapping = storage.get_mapping(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        let page = storage.pages.get(&1).unwrap();

        // the fitting page should be 64MB, so we should still have about 14MB of free space
        let free_space = page.read().free_space();
        assert!(
            free_space > 1024 * 1024 * 13 && free_space < 1024 * 1024 * 15,
            "free space should be around 14MB, but it is: {}",
            free_space
        );
    }

    #[test]
    fn test_storage_persistence_basic() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        {
            let mut storage = PayloadStorage::new(path.clone());

            storage.put_payload(0, payload.clone());
            assert_eq!(storage.pages.len(), 1);

            let page_mapping = storage.get_mapping(0).unwrap();
            assert_eq!(page_mapping.page_id, 1); // first page
            assert_eq!(page_mapping.slot_id, 0); // first slot

            let stored_payload = storage.get_payload(0);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);

            // drop storage
            drop(storage);
        }

        // reopen storage
        let storage = PayloadStorage::open(path.clone()).unwrap();
        assert_eq!(storage.pages.len(), 1);

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }
}
