use crate::page_tracker::{OffsetsToSlots, PageId, PagePointer, PageTracker, PointOffset};
use crate::payload::Payload;
use crate::slotted_page::{SlotHeader, SlotId, SlottedPageMmap};
use lz4_flex::compress_prepend_size;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::PathBuf;

#[derive(Debug)]
pub struct PayloadStorage {
    page_tracker: PageTracker,
    pub(super) new_page_size: usize, // page size in bytes when creating new pages
    pub(super) pages: HashMap<u32, SlottedPageMmap>, // page_id -> mmap page
    max_page_id: u32,
    pub(super) page_emptiness: PriorityQueue<PageId, usize>,
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
            page_emptiness: PriorityQueue::new(),
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
            page_emptiness: Default::default(),
            base_path: path.clone(),
        };
        for page_id in page_ids {
            let page_path = storage.page_path(page_id);
            let slotted_page = SlottedPageMmap::open(&page_path).expect("Page not found");

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
    fn add_page(&mut self, page_id: u32, page: SlottedPageMmap) -> bool {
        let page_exists = self.pages.contains_key(&page_id);
        if page_exists {
            return false;
        }

        let previous = self.page_emptiness.push(page_id, page.free_space());
        debug_assert!(previous.is_none());

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

    fn find_first_fitting_page(&self, payload_size: usize) -> Option<u32> {
        let needed_size = SlottedPageMmap::required_size_for_value(payload_size);

        #[cfg(debug_assertions)]
        {
            if let Some((page_id, free_space)) = self.page_emptiness.peek() {
                debug_assert_eq!(
                    self.pages.get(page_id).unwrap().free_space(),
                    *free_space,
                    "page id {} has a different computed free space than it is stored in pq",
                    page_id
                );
            };
        }

        self.page_emptiness
            .peek()
            .and_then(|(page_id, &free_space)| (free_space >= needed_size).then_some(*page_id))
    }

    /// Find the best fitting page for a payload
    /// Returns Some(page_id) of the best fitting page or None if no page has enough space
    fn find_best_fitting_page(&self, payload_size: usize) -> Option<u32> {
        if self.pages.is_empty() {
            return None;
        }
        let needed_size = SlottedPageMmap::required_size_for_value(payload_size);
        let mut best_page = 0;
        // best is the page with the lowest free space that fits the payload
        let mut best_fit_size = usize::MAX;

        for (page_id, page) in &self.pages {
            let free_space = page.free_space();
            // select the smallest page that fits the payload
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
    /// If size is None, the page will have the default size
    ///
    /// Returns the new page id
    fn create_new_page(&mut self, size: usize) -> u32 {
        let new_page_id = self.max_page_id + 1;
        let path = self.page_path(new_page_id);
        let was_created = self.add_page(new_page_id, SlottedPageMmap::new(&path, size));

        assert!(was_created);

        new_page_id
    }

    /// Create a new page adapted to the payload size and return its id.
    /// The page size will be the next power of two of the payload size, with a minimum of `self.page_size`.
    fn create_new_page_for_payload(&mut self, payload_size: usize) -> u32 {
        let required_size = SlottedPageMmap::new_page_size_for_value(payload_size);
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
            let page = self.pages.get_mut(&page_id).unwrap();
            let updated = page.update_value(slot_id, &comp_payload);
            if !updated {
                // delete slot
                page.delete_value(slot_id);

                // find a fitting page (or create a new one if all full)
                let other_page_id =
                    self.find_first_fitting_page(payload_size)
                        .unwrap_or_else(|| {
                            // create a new page
                            self.create_new_page_for_payload(payload_size)
                        });
                let other_page = self.pages.get_mut(&other_page_id).unwrap();
                let new_slot_id = other_page
                    .insert_value(point_offset, &comp_payload)
                    .unwrap();

                // update page_emptiness
                let previous_prio = self
                    .page_emptiness
                    .push(other_page_id, other_page.free_space());
                debug_assert!(previous_prio.is_some());

                // update page_tracker
                self.page_tracker
                    .set(point_offset, PagePointer::new(other_page_id, new_slot_id));
            }

            true
        } else {
            // this is a new payload
            let page_id = self
                .find_first_fitting_page(payload_size)
                .unwrap_or_else(|| {
                    // create a new page
                    self.create_new_page_for_payload(payload_size)
                });

            let page = self.pages.get_mut(&page_id).unwrap();

            let slot_id = page.insert_value(point_offset, &comp_payload).unwrap();

            // update page_emptiness
            self.page_emptiness
                .change_priority(&page_id, page.free_space());

            // update page_tracker
            self.page_tracker
                .set(point_offset, PagePointer::new(page_id, slot_id));

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

    /// Page ids with amount of fragmentation, ordered by most to least fragmentation
    fn pages_to_defrag(&self) -> Vec<(u32, usize)> {
        let mut fragmentation = self
            .pages
            .iter()
            .filter_map(|(page_id, page)| {
                let frag_space = page.fragmented_space();

                // check if we should defrag this page
                let frag_threshold =
                    SlottedPageMmap::FRAGMENTATION_THRESHOLD_RATIO * page.size() as f32;
                if frag_space < frag_threshold.ceil() as usize {
                    // page is not fragmented enough, skip
                    return None;
                }
                Some((*page_id, frag_space))
            })
            .collect::<Vec<_>>();

        // sort by most to least fragmented
        fragmentation.sort_unstable_by_key(|(_, fragmented_space)| Reverse(*fragmented_space));

        fragmentation
    }

    /// Compact the storage by defragmenting pages into new ones. Returns true if at least one page was defragmented.
    pub fn compact_all(&mut self) -> bool {
        // find out which pages should be compacted
        let pages_to_defrag = self.pages_to_defrag();

        if pages_to_defrag.is_empty() {
            return false;
        }

        let mut pages_to_defrag = pages_to_defrag.into_iter();
        let mut old_page_id = pages_to_defrag.next().unwrap().0;
        let mut last_slot_id = 0;

        // TODO: account for the fact that the first value could be larger than 32MB and the newly created page will
        // immediately not be used? we don't want to create empty pages. But it is a quite rare case, so maybe we can just ignore it
        let mut size_hint = self.new_page_size;

        // This is a loop because we might need to create more pages if the current new page is full
        'new_page: loop {
            // create new page
            let new_page_id = self.max_page_id + 1;
            let mut new_page = SlottedPageMmap::new(&self.page_path(new_page_id), size_hint);

            let mut pages_to_remove = Vec::new();
            let mut slots_to_update: OffsetsToSlots = Vec::new();
            // go over each page to defrag
            'defrag_pages: loop {
                match self.transfer_page_values(old_page_id, &mut new_page, last_slot_id) {
                    ControlFlow::Break((new_slots, slot_id, hint)) => {
                        // new page is full, insert this one and create a another one
                        self.insert_compacted_page(new_page, new_page_id, new_slots);

                        size_hint = hint.unwrap_or(self.new_page_size);
                        last_slot_id = slot_id;
                        continue 'new_page;
                    }
                    ControlFlow::Continue(new_slots) => slots_to_update.extend(new_slots),
                };

                pages_to_remove.push(old_page_id);

                match pages_to_defrag.next() {
                    Some((page_id, _defrag_space)) => {
                        last_slot_id = 0;
                        old_page_id = page_id;
                    }
                    // No more pages to defrag, end compaction
                    None => break 'defrag_pages,
                };
            }

            // insert page into hashmap, update page tracker
            self.insert_compacted_page(new_page, new_page_id, slots_to_update);

            for old_page_id in pages_to_remove {
                // delete old page, page tracker shouldn't point to this page anymore.
                self.pages.remove(&old_page_id).unwrap().delete_page();
            }

            break 'new_page;
        }

        true
    }

    /// Transfer all values from one page to the other, and update the page tracker.
    ///
    /// If the values do not fit, returns `ControlFlow::Break` with the pending slot id and a size hint to create another page.
    fn transfer_page_values(
        &mut self,
        old_page_id: PageId,
        new_page: &mut SlottedPageMmap,
        from_slot_id: SlotId,
    ) -> ControlFlow<(OffsetsToSlots, SlotId, Option<usize>), OffsetsToSlots> {
        let mut current_slot_id = from_slot_id;

        let old_page = self.pages.get(&old_page_id).unwrap();

        let mut new_slots = Vec::new();

        for (slot, value) in old_page.iter_slot_values_starting_from(current_slot_id) {
            match Self::move_value(slot, value, new_page) {
                ControlFlow::Break(size_hint) => {
                    return ControlFlow::Break((new_slots, current_slot_id, size_hint));
                }
                ControlFlow::Continue(Some((point_offset, slot_id))) => {
                    new_slots.push((point_offset, slot_id))
                }
                // point was skipped (deleted or empty slot)
                ControlFlow::Continue(None) => {}
            }

            // prepare for next iteration
            current_slot_id += 1;
        }

        ControlFlow::Continue(new_slots)
    }

    /// Move a value from one page to another.
    ///
    /// If the value does not fit in the new one, returns `ControlFlow::Break` with a size hint to create another page.
    fn move_value(
        current_slot: &SlotHeader,
        value: Option<&[u8]>,
        new_page: &mut SlottedPageMmap,
    ) -> ControlFlow<Option<usize>, Option<(PointOffset, SlotId)>> {
        let point_offset = current_slot.point_offset();

        if current_slot.deleted() {
            // value was deleted, skip
            return ControlFlow::Continue(None);
        }

        let was_inserted = if let Some(value) = value {
            new_page.insert_value(point_offset, value)
        } else {
            new_page.insert_placeholder_value(point_offset)
        };

        let Some(slot_id) = was_inserted else {
            // new page is full, request to create a new one
            let size_hint = value.map(|v| v.len());
            return ControlFlow::Break(size_hint);
        };

        ControlFlow::Continue(Some((point_offset, slot_id)))
    }

    fn insert_compacted_page(
        &mut self,
        page: SlottedPageMmap,
        page_id: PageId,
        new_slots: Vec<(PointOffset, SlotId)>,
    ) {
        // update page tracker
        for (point_offset, slot_id) in new_slots {
            let new_pointer = PagePointer { page_id, slot_id };
            self.page_tracker.set(point_offset, new_pointer);
        }

        // add page to hashmap
        self.add_page(page_id, page);
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

            let fragmented_space = page.fragmented_space();
            assert_eq!(fragmented_space, 0);
            assert_eq!(fragmented_space, page.calculate_fragmented_space());
        }

        {
            // delete payload
            let deleted = storage.delete_payload(0);
            assert!(deleted.is_some());
            assert_eq!(storage.pages.len(), 1);

            let page = storage.pages.get(&1).unwrap();

            // check fragmentation
            let fragmented_space = page.fragmented_space();
            assert!(
                fragmented_space > 1024 * 1024 * 49 && fragmented_space < 1024 * 1024 * 51,
                "free space should be around 50MB, but it is: {}",
                fragmented_space
            );
            assert_eq!(fragmented_space, page.calculate_fragmented_space());
        }

        // compact storage to remove fragmentation
        storage.compact_all();

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
        assert_eq!(storage.pages.len(), 3);
        assert_eq!(storage.page_tracker.mapping_len(), EXPECTED_LEN * 2);

        // assert storage is consistent
        storage_double_pass_is_consistent(&storage, 0);

        // drop storage
        drop(storage);

        // reopen storage
        let mut storage = PayloadStorage::open(dir.path().to_path_buf(), None).unwrap();
        assert_eq!(point_offset, EXPECTED_LEN as u32 * 2);
        assert_eq!(storage.pages.len(), 3);
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
    fn test_compaction() {
        let (_dir, mut storage) = empty_storage();

        let rng = &mut rand::thread_rng();
        let max_point_offset = 20000;

        let large_payloads = (0..max_point_offset)
            .map(|_| random_payload(rng, 10))
            .collect::<Vec<_>>();

        for (i, payload) in large_payloads.iter().enumerate() {
            storage.put_payload(i as u32, payload);
        }

        // sanity check
        for (i, payload) in large_payloads.iter().enumerate() {
            let stored_payload = storage.get_payload(i as u32);
            assert_eq!(stored_payload.as_ref(), Some(payload));
        }

        // check no fragmentation
        for page in storage.pages.values() {
            assert_eq!(page.fragmented_space(), 0);
            assert_eq!(page.calculate_fragmented_space(), 0);
        }

        // update with smaller values
        let small_payloads = (0..max_point_offset)
            .map(|_| random_payload(rng, 1))
            .collect::<Vec<_>>();

        for (i, payload) in small_payloads.iter().enumerate() {
            storage.put_payload(i as u32, payload);
        }

        // sanity check
        // check consistency
        for (i, payload) in small_payloads.iter().enumerate() {
            let stored_payload = storage.get_payload(i as u32);
            assert_eq!(stored_payload.as_ref(), Some(payload));
        }

        // check fragmentation
        assert!(!storage.pages_to_defrag().is_empty());

        // compaction
        storage.compact_all();

        // check consistency
        for (i, payload) in small_payloads.iter().enumerate() {
            let stored_payload = storage.get_payload(i as u32);
            assert_eq!(stored_payload.as_ref(), Some(payload));
        }

        // check no outstanding fragmentation
        assert!(storage.pages_to_defrag().is_empty());
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
