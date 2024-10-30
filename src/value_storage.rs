use crate::bitmask::Bitmask;
use crate::page::Page;
use crate::tracker::{BlockOffset, PageId, PointOffset, Tracker, ValuePointer};
use crate::utils_copied::mmap_type;
use crate::value::Value;

use lz4_flex::compress_prepend_size;
use std::collections::HashMap;
use std::path::PathBuf;

/// Expect JSON values to have roughly 3–5 fields with mostly small values.
/// For 1M values, this would require 128MB of memory.
pub const BLOCK_SIZE_BYTES: usize = 128;

/// Default page size used when not specified
pub const PAGE_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB

#[derive(Debug)]
pub struct ValueStorage<V> {
    /// Holds mapping from `PointOffset` -> `ValuePointer`
    ///
    /// Stored in a separate file
    tracker: Tracker,
    /// Page size in bytes when creating new pages, normally 32MB
    pub(super) new_page_size: usize,
    // TODO: turn hashmap into vec, all page ids are sequential now
    /// Mapping from page_id -> mmap page
    pub(super) pages: HashMap<u32, Page>,
    /// Bitmask to represent which "blocks" of data in the pages are used and which are free.
    ///
    /// 0 is free, 1 is used.
    bitmask: Bitmask,
    /// Same as `pages.len()`, but used to create new pages
    next_page_id: u32,
    /// Path of the directory where the storage files are stored
    base_path: PathBuf,
    _value_type: std::marker::PhantomData<V>,
}

impl<V: Value> ValueStorage<V> {
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
        for tracker_file in self.tracker.files() {
            paths.push(tracker_file);
        }
        // pages files
        for pages in self.pages.keys() {
            paths.push(self.page_path(*pages));
        }
        // bitmask files
        for bitmask_file in self.bitmask.files() {
            paths.push(bitmask_file);
        }
        paths
    }

    /// Initializes a new storage with a single empty page.
    ///
    /// `base_path` is the directory where the storage files will be stored.
    /// It should exist already.
    pub fn new(base_path: PathBuf, page_size: Option<usize>) -> Self {
        assert!(base_path.exists(), "Base path does not exist");
        assert!(base_path.is_dir(), "Base path is not a directory");
        let page_size = page_size.unwrap_or(PAGE_SIZE_BYTES);
        let mut storage = Self {
            tracker: Tracker::new(&base_path, None),
            new_page_size: page_size,
            pages: HashMap::new(),
            bitmask: Bitmask::create(&base_path, page_size),
            next_page_id: 0,
            base_path,
            _value_type: std::marker::PhantomData,
        };

        // create first page to be covered by the bitmask
        let new_page_id = storage.next_page_id;
        let path = storage.page_path(new_page_id);
        let page = Page::new(&path, storage.new_page_size);
        let was_created = storage.add_page(new_page_id, page);
        assert!(was_created);

        storage
    }

    /// Open an existing storageo at the given path
    /// Returns None if the storage does not exist
    pub fn open(path: PathBuf, new_page_size: Option<usize>) -> Option<Self> {
        let new_page_size = new_page_size.unwrap_or(PAGE_SIZE_BYTES);

        let page_tracker = Tracker::open(&path)?;

        let bitmask = Bitmask::open(&path, new_page_size)?;

        let num_pages = bitmask.infer_num_pages();

        // load pages
        let mut storage = Self {
            tracker: page_tracker,
            new_page_size,
            pages: Default::default(),
            bitmask,
            next_page_id: 0,
            base_path: path.clone(),
            _value_type: std::marker::PhantomData,
        };
        for page_id in 0..num_pages as PageId {
            let page_path = storage.page_path(page_id);
            let slotted_page = Page::open(&page_path).expect("Page not found");

            storage.add_page(page_id, slotted_page);
        }
        Some(storage)
    }

    /// Get the path for a given page id
    pub fn page_path(&self, page_id: u32) -> PathBuf {
        self.base_path.join(format!("page_{}.dat", page_id))
    }

    /// Add a page to the storage. If it already exists, returns false
    fn add_page(&mut self, page_id: u32, page: Page) -> bool {
        let page_exists = self.pages.contains_key(&page_id);
        if page_exists {
            return false;
        }

        debug_assert_eq!(page_id, self.next_page_id, "Page id mismatch");

        let previous = self.pages.insert(page_id, page);
        debug_assert!(previous.is_none());

        self.next_page_id += 1;

        true
    }

    /// Read raw value from the pages. Considering that they can span more than one page.
    fn read_from_pages(
        &self,
        start_page_id: PageId,
        mut block_offset: BlockOffset,
        mut length: u32,
    ) -> Vec<u8> {
        let mut raw_sections = Vec::with_capacity(length as usize);

        for page_id in start_page_id.. {
            let page = self.pages.get(&page_id).expect("Page not found");
            let (raw, unread_bytes) = page.read_value(block_offset, length);
            raw_sections.extend(raw);

            if unread_bytes == 0 {
                break;
            }

            block_offset = 0;
            length = unread_bytes as u32;
        }

        raw_sections
    }

    /// Get the value for a given point offset
    pub fn get_value(&self, point_offset: PointOffset) -> Option<V> {
        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = self.get_pointer(point_offset)?;

        let raw = self.read_from_pages(page_id, block_offset, length);
        let decompressed = Self::decompress(&raw);
        let value = V::from_bytes(&decompressed);

        Some(value)
    }

    /// Create a new page and return its id.
    /// If size is None, the page will have the default size
    ///
    /// Returns the new page id
    fn create_new_page(&mut self) -> u32 {
        let new_page_id = self.next_page_id;
        let path = self.page_path(new_page_id);
        let page = Page::new(&path, self.new_page_size);
        let was_created = self.add_page(new_page_id, page);
        assert!(was_created);

        self.bitmask.cover_new_page();

        new_page_id
    }

    /// Get the mapping for a given point offset
    fn get_pointer(&self, point_offset: PointOffset) -> Option<ValuePointer> {
        self.tracker.get(point_offset)
    }

    /// The number of blocks needed for a given value bytes size
    fn blocks_for_value(value_size: usize) -> u32 {
        value_size.div_ceil(BLOCK_SIZE_BYTES).try_into().unwrap()
    }

    fn find_or_create_available_blocks(&mut self, num_blocks: u32) -> (PageId, BlockOffset) {
        debug_assert!(num_blocks > 0, "num_blocks must be greater than 0");

        if let Some((page_id, block_offset)) = self.bitmask.find_available_blocks(num_blocks) {
            return (page_id, block_offset);
        }
        // else we need new page(s)
        let trailing_free_blocks = self.bitmask.trailing_free_blocks();

        let missing_blocks = num_blocks.saturating_sub(trailing_free_blocks) as usize;

        let num_pages = (missing_blocks * BLOCK_SIZE_BYTES).div_ceil(self.new_page_size);
        for _ in 0..num_pages {
            self.create_new_page();
        }

        // At this point we are sure that we have enough free pages to allocate the blocks
        self.bitmask.find_available_blocks(num_blocks).unwrap()
    }

    /// Write value into a new cell, considering that it can span more than one page
    fn write_into_pages(
        &mut self,
        value: &[u8],
        start_page_id: PageId,
        mut block_offset: BlockOffset,
    ) {
        let value_size = value.len();

        // Track the number of bytes that still need to be written
        let mut unwritten_tail = value_size;

        for page_id in start_page_id.. {
            let page = self
                .pages
                .get_mut(&page_id)
                .unwrap_or_else(|| panic!("Page {page_id} not found"));

            let range = (value_size - unwritten_tail)..;
            unwritten_tail = page.write_value(block_offset, &value[range]);

            if unwritten_tail == 0 {
                break;
            }

            block_offset = 0;
        }
    }

    /// Put a value in the storage.
    ///
    /// Returns true if the value existed previously and was updated, false if it was newly inserted.
    pub fn put_value(&mut self, point_offset: PointOffset, value: &V) -> bool {
        let value_bytes = value.to_bytes();
        let comp_value = Self::compress(&value_bytes);
        let value_size = comp_value.len();

        let required_blocks = Self::blocks_for_value(value_size);
        let (start_page_id, block_offset) = self.find_or_create_available_blocks(required_blocks);

        // In case of crashing somewhere in the middle of this operation, the worst
        // that should happen is that we mark more cells as used than they actually are,
        // so will never reuse such space, but data will not be corrupted.

        // insert into a new cell, considering that it can span more than one page
        self.write_into_pages(&comp_value, start_page_id, block_offset);

        // mark new cell as used in the bitmask
        self.bitmask
            .mark_blocks(start_page_id, block_offset, required_blocks, true);

        // update the pointer
        let old_pointer_opt = self.get_pointer(point_offset);
        self.tracker.set(
            point_offset,
            ValuePointer::new(start_page_id, block_offset, value_size as u32),
        );

        // Check if it is an update.
        if let Some(old_pointer) = old_pointer_opt {
            // mark old cell as available in the bitmask
            self.bitmask.mark_blocks(
                old_pointer.page_id,
                old_pointer.block_offset,
                Self::blocks_for_value(old_pointer.length as usize),
                false,
            );
        }
        // return whether it was an update or not
        old_pointer_opt.is_some()
    }

    /// Delete a value from the storage
    ///
    /// Returns None if the point_offset, page, or value was not found
    ///
    /// Returns the deleted value otherwise
    pub fn delete_value(&mut self, point_offset: PointOffset) -> Option<V> {
        let ValuePointer {
            page_id,
            block_offset,
            length,
        } = self.get_pointer(point_offset)?;
        let raw = self.read_from_pages(page_id, block_offset, length);
        let decompressed = Self::decompress(&raw);
        let value = V::from_bytes(&decompressed);

        // delete mapping
        self.tracker.unset(point_offset);

        // mark cell as available in the bitmask
        self.bitmask.mark_blocks(
            page_id,
            block_offset,
            Self::blocks_for_value(length as usize),
            false,
        );

        Some(value)
    }

    /// Wipe the storage, drop all pages and delete the base directory
    pub fn wipe(&mut self) {
        // clear pages
        self.pages.clear();
        // deleted base directory
        std::fs::remove_dir_all(&self.base_path).unwrap();
    }

    /// Flush all mmap pages to disk
    pub fn flush(&self) -> Result<(), mmap_type::Error> {
        self.tracker.flush()?;
        for page in self.pages.values() {
            page.flush()?;
        }
        self.bitmask.flush()?;
        Ok(())
    }

    /// Iterate over all the values in the storage
    pub fn iter<F>(&self, mut callback: F) -> std::io::Result<()>
    where
        F: FnMut(PointOffset, &V) -> std::io::Result<bool>,
    {
        for pointer in self.tracker.iter_pointers().flatten() {
            let ValuePointer {
                page_id,
                block_offset,
                length,
            } = pointer;

            let raw = self.read_from_pages(page_id, block_offset, length);
            let decompressed = Self::decompress(&raw);
            let value = V::from_bytes(&decompressed);
            if !callback(block_offset, &value)? {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        bitmask::REGION_SIZE_BLOCKS,
        fixtures::{empty_storage, empty_storage_sized, random_payload, HM_FIELDS},
        payload::Payload,
        value::Value,
    };
    use rand::{distributions::Uniform, prelude::Distribution, seq::SliceRandom, Rng};
    use rstest::rstest;
    use tempfile::Builder;

    #[test]
    fn test_empty_payload_storage() {
        let (_dir, storage) = empty_storage();
        let payload = storage.get_value(0);
        assert!(payload.is_none());
    }

    #[test]
    fn test_put_single_empty_value() {
        let (_dir, mut storage) = empty_storage();

        // TODO: should we actually use the pages for empty values?
        let payload = Payload::default();
        storage.put_value(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.mapping_len(), 1);

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), Payload::default());
    }

    #[test]
    fn test_put_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.mapping_len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    fn test_storage_files() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.mapping_len(), 1);
        let files = storage.files();
        assert_eq!(files.len(), 4, "Expected 4 files, got {:?}", files);
        assert_eq!(files[0].file_name().unwrap(), "tracker.dat");
        assert_eq!(files[1].file_name().unwrap(), "page_0.dat");
        assert_eq!(files[2].file_name().unwrap(), "bitmask.dat");
        assert_eq!(files[3].file_name().unwrap(), "gaps.dat");
    }

    #[test]
    fn test_put_payload() {
        let (_dir, mut storage) = empty_storage();

        let rng = &mut rand::thread_rng();

        let mut payloads = (0..100000u32)
            .map(|point_offset| (point_offset, random_payload(rng, 2)))
            .collect::<Vec<_>>();

        for (point_offset, payload) in payloads.iter() {
            storage.put_value(*point_offset, payload);

            let stored_payload = storage.get_value(*point_offset);
            assert!(stored_payload.is_some());
            assert_eq!(&stored_payload.unwrap(), payload);
        }

        // read randomly
        payloads.shuffle(rng);
        for (point_offset, payload) in payloads.iter() {
            let stored_payload = storage.get_value(*point_offset);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload.clone());
        }
    }

    #[test]
    fn test_delete_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload);
        assert_eq!(storage.pages.len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert_eq!(stored_payload, Some(payload));

        // delete payload
        let deleted = storage.delete_value(0);
        assert_eq!(deleted, stored_payload);
        assert_eq!(storage.pages.len(), 1);

        // get payload again
        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_none());
    }

    #[test]
    fn test_update_single_payload() {
        let (_dir, mut storage) = empty_storage();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        storage.put_value(0, &payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.mapping_len(), 1);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // update payload
        let mut updated_payload = Payload::default();
        updated_payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("updated".to_string()),
        );

        storage.put_value(0, &updated_payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.tracker.mapping_len(), 1);

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), updated_payload);
    }

    #[test]
    fn test_write_across_pages() {
        let page_size = BLOCK_SIZE_BYTES * REGION_SIZE_BLOCKS;
        let (_dir, mut storage) = empty_storage_sized(page_size);

        storage.create_new_page();

        let value_len = 1000;

        // Value should span 8 blocks
        let value = (0..)
            .map(|i| (i % 24) as u8)
            .take(value_len)
            .collect::<Vec<_>>();

        // Let's write it near the end
        let block_offset = REGION_SIZE_BLOCKS - 10;
        storage.write_into_pages(&value, 0, block_offset as u32);

        let read_value = storage.read_from_pages(0, block_offset as u32, value_len as u32);
        assert_eq!(value, read_value);
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
    fn test_behave_like_hashmap(#[values(1_048_576, 2_097_152, PAGE_SIZE_BYTES)] page_size: usize) {
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
                    storage.put_value(point_offset, &payload);
                    model_hashmap.insert(point_offset, payload);
                }
                Operation::Delete(point_offset) => {
                    let old1 = storage.delete_value(point_offset);
                    let old2 = model_hashmap.remove(&point_offset);
                    assert_eq!(
                        old1, old2,
                        "same deletion failed for point_offset: {} with {:?} vs {:?}",
                        point_offset, old1, old2
                    );
                }
                Operation::Update(point_offset, payload) => {
                    storage.put_value(point_offset, &payload);
                    model_hashmap.insert(point_offset, payload);
                }
            }
        }

        // asset same length
        assert_eq!(storage.tracker.mapping_len(), model_hashmap.len());

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_value(point_offset);
            let model_payload = model_hashmap.get(&point_offset);
            assert_eq!(stored_payload.as_ref(), model_payload);
        }

        // drop storage
        drop(storage);

        // reopen storage
        let storage =
            ValueStorage::<Payload>::open(dir.path().to_path_buf(), Some(page_size)).unwrap();

        // asset same length
        assert_eq!(storage.tracker.mapping_len(), model_hashmap.len());

        // validate storage and model_hashmap are the same
        for point_offset in 0..=max_point_offset {
            let stored_payload = storage.get_value(point_offset);
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
        assert_eq!(storage.pages.len(), 1);

        let mut payload = Payload::default();

        let huge_payload_size = 1024 * 1024 * 50; // 50MB

        let distr = Uniform::new('a', 'z');
        let rng = rand::thread_rng();

        let huge_value =
            serde_json::Value::String(distr.sample_iter(rng).take(huge_payload_size).collect());
        payload.0.insert("huge".to_string(), huge_value);

        storage.put_value(0, &payload);
        assert_eq!(storage.pages.len(), 2);

        let page_mapping = storage.get_pointer(0).unwrap();
        assert_eq!(page_mapping.page_id, 0); // first page
        assert_eq!(page_mapping.block_offset, 0); // first cell

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        {
            // the fitting page should be 64MB, so we should still have about 14MB of free space
            let free_blocks = storage.bitmask.free_blocks_for_page(1);
            let min_expected = 1024 * 1024 * 13 / BLOCK_SIZE_BYTES;
            let max_expected = 1024 * 1024 * 15 / BLOCK_SIZE_BYTES;
            assert!((min_expected..max_expected).contains(&free_blocks));
        }

        {
            // delete payload
            let deleted = storage.delete_value(0);
            assert!(deleted.is_some());
            assert_eq!(storage.pages.len(), 2);

            assert!(storage.get_value(0).is_none());
        }
    }

    #[test]
    fn test_storage_persistence_basic() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );

        {
            let mut storage = ValueStorage::new(path.clone(), None);

            storage.put_value(0, &payload);
            assert_eq!(storage.pages.len(), 1);

            let page_mapping = storage.get_pointer(0).unwrap();
            assert_eq!(page_mapping.page_id, 0); // first page
            assert_eq!(page_mapping.block_offset, 0); // first cell

            let stored_payload = storage.get_value(0);
            assert!(stored_payload.is_some());
            assert_eq!(stored_payload.unwrap(), payload);

            // drop storage
            drop(storage);
        }

        // reopen storage
        let storage = ValueStorage::<Payload>::open(path.clone(), None).unwrap();
        assert_eq!(storage.pages.len(), 1);

        let stored_payload = storage.get_value(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    fn test_with_real_hm_data() {
        const EXPECTED_LEN: usize = 105_542;

        fn write_data(storage: &mut ValueStorage<Payload>, init_offset: u32) -> u32 {
            let csv_data = include_str!("../data/h&m-articles.csv");
            let mut rdr = csv::Reader::from_reader(csv_data.as_bytes());
            let mut point_offset = init_offset;
            for result in rdr.records() {
                let record = result.unwrap();
                let mut payload = Payload::default();
                for (i, field) in HM_FIELDS.iter().enumerate() {
                    payload.0.insert(
                        field.to_string(),
                        serde_json::Value::String(record.get(i).unwrap().to_string()),
                    );
                }
                storage.put_value(point_offset, &payload);
                point_offset += 1;
            }
            point_offset
        }

        fn storage_double_pass_is_consistent(
            storage: &ValueStorage<Payload>,
            right_shift_offset: u32,
        ) {
            // validate storage value equality between the two writes
            let csv_data = include_str!("../data/h&m-articles.csv");
            let mut rdr = csv::Reader::from_reader(csv_data.as_bytes());
            for (row_index, result) in rdr.records().enumerate() {
                let record = result.unwrap();
                // apply shift offset
                let storage_index = row_index as u32 + right_shift_offset;
                let first = storage.get_value(storage_index).unwrap();
                let second = storage
                    .get_value(storage_index + EXPECTED_LEN as u32)
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
        assert_eq!(storage.tracker.mapping_len(), EXPECTED_LEN);
        assert_eq!(storage.pages.len(), 2);

        // write the same payload a second time
        let point_offset = write_data(&mut storage, point_offset);
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.tracker.mapping_len(), EXPECTED_LEN * 2);
        assert_eq!(storage.pages.len(), 4);

        // assert storage is consistent
        storage_double_pass_is_consistent(&storage, 0);

        // drop storage
        drop(storage);

        // reopen storage
        let mut storage = ValueStorage::open(dir.path().to_path_buf(), None).unwrap();
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.pages.len(), 4);
        assert_eq!(storage.tracker.mapping_len(), EXPECTED_LEN * 2);

        // assert storage is consistent after reopening
        storage_double_pass_is_consistent(&storage, 0);

        // update values shifting point offset by 1 to the right
        // loop from the end to the beginning to avoid overwriting
        let offset: u32 = 1;
        for i in (0..EXPECTED_LEN).rev() {
            let payload = storage.get_value(i as u32).unwrap();
            // move first write to the right
            storage.put_value(i as u32 + offset, &payload);
            // move second write to the right
            storage.put_value(i as u32 + offset + EXPECTED_LEN as u32, &payload);
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
        let compressed = ValueStorage::<Payload>::compress(&payload_bytes);
        let decompressed = ValueStorage::<Payload>::decompress(&compressed);
        let decompressed_payload = <Payload as Value>::from_bytes(&decompressed);
        assert_eq!(payload, decompressed_payload);
    }
}
