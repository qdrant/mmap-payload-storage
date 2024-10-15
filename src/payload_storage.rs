use crate::page::Page;
use crate::page_tracker::{PagePointer, PageTracker, PointOffset};
use crate::payload::Payload;
use lz4_flex::compress_prepend_size;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub struct PayloadStorage {
    page_tracker: PageTracker,
    pub(super) new_page_size: usize, // page size in bytes when creating new pages
    pub(super) pages: HashMap<u32, Page>, // page_id -> mmap page
    max_page_id: u32,
    base_path: PathBuf,
}

impl PayloadStorage {
    /// Default page size used when not specified
    const DEFAULT_PAGE_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB

    /// LZ4 compression
    fn compress(value: &[u8]) -> Vec<u8> {
        compress_prepend_size(value)
    }

    /// LZ4 decompression
    fn decompress(value: &[u8]) -> Vec<u8> {
        lz4_flex::decompress_size_prepended(value).unwrap()
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut paths = Vec::with_capacity(self.pages.len() + 1);
        // page tracker file
        for tracker_file in self.page_tracker.files() {
            paths.push(tracker_file);
        }
        // slotted pages files
        for pages in self.pages.keys() {
            paths.push(self.page_path(*pages));
        }
        paths
    }

    pub fn new(path: PathBuf, page_size: Option<usize>) -> Self {
        Self {
            page_tracker: PageTracker::new(&path, None),
            new_page_size: page_size.unwrap_or(Self::DEFAULT_PAGE_SIZE_BYTES),
            pages: HashMap::new(),
            max_page_id: 0,
            base_path: path,
        }
    }

    /// Open an existing PayloadStorage at the given path
    /// Returns None if the storage does not exist
    pub fn open(path: PathBuf, new_page_size: Option<usize>) -> Option<Self> {
        let page_tracker = PageTracker::open(&path)?;
        let page_ids = page_tracker.all_page_ids();
        // load pages
        let mut storage = Self {
            page_tracker,
            new_page_size: new_page_size.unwrap_or(Self::DEFAULT_PAGE_SIZE_BYTES),
            pages: Default::default(),
            max_page_id: 0,
            base_path: path.clone(),
        };
        for page_id in page_ids {
            let page_path = storage.page_path(page_id);
            let slotted_page = Page::open(&page_path).expect("Page not found");

            storage.add_page(page_id, slotted_page);
        }
        Some(storage)
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Get the path for a given page id
    pub fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path
            .join(format!("slotted_paged_{}.dat", page_id))
    }

    /// Add a page to the storage. If it already exists, returns false
    fn add_page(&mut self, page_id: u32, page: Page) -> bool {
        let page_exists = self.pages.contains_key(&page_id);
        if page_exists {
            return false;
        }

        let previous = self.pages.insert(page_id, page);
        debug_assert!(previous.is_none());

        if page_id > self.max_page_id {
            self.max_page_id = page_id;
        }

        true
    }

    /// Get the payload for a given point offset
    pub fn get_payload(&self, point_offset: PointOffset) -> Option<Payload> {
        let PagePointer { page_id, slot_id } = self.get_pointer(point_offset)?;
        let page = self.pages.get(&page_id).expect("Page not found");
        let raw = page.get_value(&slot_id).expect("Slot not found");
        let decompressed = Self::decompress(raw);
        let payload = Payload::from_bytes(&decompressed);
        Some(payload)
    }

    /// Create a new page and return its id.
    /// If size is None, the page will have the default size
    ///
    /// Returns the new page id
    fn create_new_page(&mut self, size: usize) -> u32 {
        let new_page_id = self.max_page_id + 1;
        let path = self.page_path(new_page_id);
        let was_created = self.add_page(new_page_id, Page::new(&path, size));

        assert!(was_created);

        new_page_id
    }

    /// Create a new page adapted to the payload size and return its id.
    /// The page size will be the next power of two of the payload size, with a minimum of `self.page_size`.
    fn create_new_page_for_payload(&mut self, payload_size: usize) -> u32 {
        let required_size = Page::new_page_size_for_value(payload_size);
        // return the smallest power of two greater than or equal to the required size if it is greater than the default page size
        let real_size = self.new_page_size.max(required_size).next_power_of_two();
        self.create_new_page(real_size)
    }

    /// Get the mapping for a given point offset
    fn get_pointer(&self, point_offset: PointOffset) -> Option<PagePointer> {
        self.page_tracker.get(point_offset)
    }

    /// Put a payload in the storage.
    ///
    /// Returns true if the payload existed previously and was updated, false if it was newly inserted.
    pub fn put_payload(&mut self, point_offset: PointOffset, payload: &Payload) -> bool {
        let payload_bytes = payload.to_bytes();
        let comp_payload = Self::compress(&payload_bytes);
        let payload_size = comp_payload.len();

        if let Some(PagePointer { page_id, slot_id }) = self.get_pointer(point_offset) {
            // this is an update

            // TODO:
            // - insert into a new cell
            // - update the pointer
            // - mark the old cell blocks as available in the bitmask
            // - mark the new cell blocks as used in the bitmask
            // - recompute bitmask gaps

            true
        } else {
            // this is a new payload

            // TODO:
            // - find page and offset which fits the payload
            // - insert the payload
            // - update the index
            // - mark the cell blocks as used in the bitmask
            // - recompute bitmask gaps

            false
        }
    }

    /// Delete a payload from the storage
    ///
    /// Returns None if the point_offset, page, or payload was not found
    ///
    /// Returns the deleted payload otherwise
    pub fn delete_payload(&mut self, point_offset: PointOffset) -> Option<Payload> {
        let PagePointer { page_id, slot_id } = self.get_pointer(point_offset)?;
        let page = self.pages.get_mut(&page_id).expect("Page not found");
        // get current value
        let raw = page.get_value(&slot_id).expect("Slot not found");
        let decompressed = Self::decompress(raw);
        let payload = Payload::from_bytes(&decompressed);
        // delete value from page
        page.delete_value(slot_id);
        // delete mapping
        self.page_tracker.unset(point_offset);
        Some(payload)
    }

    /// Wipe the storage, drop all pages and delete the base directory
    pub fn wipe(&mut self) {
        // clear pages
        self.pages.clear();
        // deleted base directory
        std::fs::remove_dir_all(&self.base_path).unwrap();
    }

    /// Flush all mmap pages to disk
    pub fn flush(&self) -> std::io::Result<()> {
        self.page_tracker.flush()?;
        for page in self.pages.values() {
            page.flush()?;
        }
        Ok(())
    }

    pub fn iter<F>(&self, mut callback: F) -> std::io::Result<()>
    where
        F: FnMut(PointOffset, &Payload) -> std::io::Result<bool>,
    {
        for page in self.pages.values() {
            for (slot, value) in page.iter_slot_values_starting_from(0) {
                if let Some(value) = value {
                    let decompressed = Self::decompress(value);
                    let payload = Payload::from_bytes(&decompressed);
                    let point_offset = slot.point_offset();
                    if !callback(point_offset, &payload)? {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    use crate::fixtures::{empty_storage, empty_storage_sized, random_payload, HM_FIELDS};
    use rand::{distributions::Uniform, prelude::Distribution, seq::SliceRandom, Rng};
    use rstest::rstest;
    use tempfile::Builder;

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
        storage.put_payload(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.mapping_len(), 1);

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

        storage.put_payload(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.mapping_len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    fn test_storage_files() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        storage.put_payload(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.mapping_len(), 1);
        let files = storage.files();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].file_name().unwrap(), "page_tracker.dat");
        assert_eq!(files[1].file_name().unwrap(), "slotted_paged_1.dat");
    }

    #[test]
    fn test_put_payload() {
        let (_dir, mut storage) = empty_storage();

        let rng = &mut rand::thread_rng();

        let mut payloads = (0..100000u32)
            .map(|point_offset| (point_offset, random_payload(rng, 2)))
            .collect::<Vec<_>>();

        for (point_offset, payload) in payloads.iter() {
            storage.put_payload(*point_offset, payload);

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

        storage.put_payload(0, &payload);
        assert_eq!(storage.pages.len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert_eq!(stored_payload, Some(payload));

        // delete payload
        let deleted = storage.delete_payload(0);
        assert_eq!(deleted, stored_payload);
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

        storage.put_payload(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.mapping_len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
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

        storage.put_payload(0, &updated_payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.mapping_len(), 1);

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
                    let size_factor = rng.gen_range(1..10);
                    let payload = random_payload(rng, size_factor);
                    Operation::Put(point_offset, payload)
                }
                1 => Operation::Delete(point_offset),
                2 => {
                    let size_factor = rng.gen_range(1..10);
                    let payload = random_payload(rng, size_factor);
                    Operation::Update(point_offset, payload)
                }
                _ => unreachable!(),
            }
        }
    }

    #[rstest]
    fn test_behave_like_hashmap(#[values(10, 10_000, 32_000_000)] page_size: usize) {
        let (dir, mut storage) = empty_storage_sized(page_size);

        let rng = &mut rand::thread_rng();
        let max_point_offset = 100000u32;

        let mut model_hashmap = HashMap::new();

        let operations = (0..100000u32)
            .map(|_| Operation::random(rng, max_point_offset))
            .collect::<Vec<_>>();

        // apply operations to storage and model_hashmap
        for operation in operations.into_iter() {
            match operation {
                Operation::Put(point_offset, payload) => {
                    storage.put_payload(point_offset, &payload);
                    model_hashmap.insert(point_offset, payload);
                }
                Operation::Delete(point_offset) => {
                    let old1 = storage.delete_payload(point_offset);
                    let old2 = model_hashmap.remove(&point_offset);
                    assert_eq!(
                        old1, old2,
                        "same deletion failed for point_offset: {} with {:?} vs {:?}",
                        point_offset, old1, old2
                    );
                }
                Operation::Update(point_offset, payload) => {
                    storage.put_payload(point_offset, &payload);
                    model_hashmap.insert(point_offset, payload);
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
        let storage = PayloadStorage::open(dir.path().to_path_buf(), Some(page_size)).unwrap();

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
    fn test_handle_huge_payload() {
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

        storage.put_payload(0, &payload);
        assert_eq!(storage.pages.len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        {
            let page = storage.pages.get(&1).unwrap();

            // the fitting page should be 64MB, so we should still have about 14MB of free space
            let free_space = page.free_space();
            assert!(
                free_space > 1024 * 1024 * 13 && free_space < 1024 * 1024 * 15,
                "free space should be around 14MB, but it is: {}",
                free_space
            );
        }

        {
            // delete payload
            let deleted = storage.delete_payload(0);
            assert!(deleted.is_some());
            assert_eq!(storage.pages.len(), 1);

            assert!(storage.get_payload(0).is_none());
        }

        // the page has been reclaimed completely
        assert!(!storage.pages.contains_key(&1));
        assert!(storage.page_tracker.get(0).is_none());
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
            let mut storage = PayloadStorage::new(path.clone(), None);

            storage.put_payload(0, &payload);
            assert_eq!(storage.pages.len(), 1);

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 1); // first page
            assert_eq!(page_mapping.slot_id, 0); // first slot

            let stored_payload = storage.get_payload(0);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);

            // drop storage
            drop(storage);
        }

        // reopen storage
        let storage = PayloadStorage::open(path.clone(), None).unwrap();
        assert_eq!(storage.pages.len(), 1);

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    fn test_with_real_hm_data() {
        const EXPECTED_LEN: usize = 105_542;

        fn write_data(storage: &mut PayloadStorage, init_offset: u32) -> u32 {
            let csv_data = include_str!("../data/h&m-articles.csv");
            let mut rdr = csv::Reader::from_reader(csv_data.as_bytes());
            let mut point_offset = init_offset;
            for result in rdr.records() {
                let record = result.unwrap();
                let mut payload = Payload::default();
                for (i, field) in HM_FIELDS.iter().enumerate() {
                    payload.0.insert(
                        field.to_string(),
                        Value::String(record.get(i).unwrap().to_string()),
                    );
                }
                storage.put_payload(point_offset, &payload);
                point_offset += 1;
            }
            point_offset
        }

        fn storage_double_pass_is_consistent(storage: &PayloadStorage, right_shift_offset: u32) {
            // validate storage value equality between the two writes
            let csv_data = include_str!("../data/h&m-articles.csv");
            let mut rdr = csv::Reader::from_reader(csv_data.as_bytes());
            for (row_index, result) in rdr.records().enumerate() {
                let record = result.unwrap();
                // apply shift offset
                let storage_index = row_index as u32 + right_shift_offset;
                let first = storage.get_payload(storage_index).unwrap();
                let second = storage
                    .get_payload(storage_index + EXPECTED_LEN as u32)
                    .unwrap();
                // assert the two payloads are equal
                assert_eq!(first, second);
                // validate the payload against record
                for (i, field) in HM_FIELDS.iter().enumerate() {
                    assert_eq!(
                        first.0.get(*field).unwrap().as_str().unwrap(),
                        record.get(i).unwrap(),
                        "failed for id {} with shift {} for field: {}",
                        row_index,
                        right_shift_offset,
                        field
                    );
                }
            }
        }

        let (dir, mut storage) = empty_storage();
        // load data into storage
        let point_offset = write_data(&mut storage, 0);
        assert_eq!(point_offset, EXPECTED_LEN as u32);
        assert_eq!(storage.page_tracker.mapping_len(), EXPECTED_LEN);
        assert_eq!(storage.pages.len(), 2);

        // write the same payload a second time
        let point_offset = write_data(&mut storage, point_offset);
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.pages.len(), 4);
        assert_eq!(storage.page_tracker.mapping_len(), EXPECTED_LEN * 2);

        // assert storage is consistent
        storage_double_pass_is_consistent(&storage, 0);

        // drop storage
        drop(storage);

        // reopen storage
        let mut storage = PayloadStorage::open(dir.path().to_path_buf(), None).unwrap();
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.pages.len(), 4);
        assert_eq!(storage.page_tracker.mapping_len(), EXPECTED_LEN * 2);

        // assert storage is consistent after reopening
        storage_double_pass_is_consistent(&storage, 0);

        // update values shifting point offset by 1 to the right
        // loop from the end to the beginning to avoid overwriting
        let offset: u32 = 1;
        for i in (0..EXPECTED_LEN).rev() {
            let payload = storage.get_payload(i as u32).unwrap();
            // move first write to the right
            storage.put_payload(i as u32 + offset, &payload);
            // move second write to the right
            storage.put_payload(i as u32 + offset + EXPECTED_LEN as u32, &payload);
        }

        // assert storage is consistent after updating
        storage_double_pass_is_consistent(&storage, offset);

        // wipe storage manually
        assert!(dir.path().exists());
        storage.wipe();
        assert!(!dir.path().exists());
    }

    #[test]
    fn test_payload_compression() {
        let payload = random_payload(&mut rand::thread_rng(), 2);
        let payload_bytes = payload.to_bytes();
        let compressed = PayloadStorage::compress(&payload_bytes);
        let decompressed = PayloadStorage::decompress(&compressed);
        let decompressed_payload = Payload::from_bytes(&decompressed);
        assert_eq!(payload, decompressed_payload);
    }
}
