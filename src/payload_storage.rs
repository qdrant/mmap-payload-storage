use crate::payload::Payload;
use crate::slotted_page::{SlotId, SlottedPageMmap};
use lz4_flex::compress_prepend_size;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

type PointOffset = u32;
type PageId = u32;

#[derive(Clone, Copy)]
struct PagePointer {
    page_id: PageId,
    slot_id: SlotId,
}

struct PayloadStorage {
    page_tracker: Vec<Option<PagePointer>>, // points_offsets are contiguous
    pages: HashMap<u32, Arc<RwLock<SlottedPageMmap>>>, // page_id -> mmap page
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
            page_tracker: Vec::new(),
            pages: HashMap::new(),
            base_path: path,
        }
    }

    pub fn path_page(&self, page_id: u32) -> PathBuf {
        self.base_path
            .join(format!("slotted-paged-{}.dat", page_id))
    }

    /// Add a page to the storage. If it already exists, returns false
    fn add_page(&mut self, page_id: u32, page: SlottedPageMmap) -> bool {
        let page_exists = self.pages.contains_key(&page_id);
        if page_exists {
            return false;
        }
        self.pages.insert(page_id, Arc::new(RwLock::new(page)));

        true
    }

    /// Get the payload for a given point offset
    pub fn get_payload(&self, point_offset: PointOffset) -> Option<Payload> {
        let PagePointer { page_id, slot_id } =
            self.page_tracker.get(point_offset as usize)?.as_ref()?;
        let page = self.pages.get(page_id).expect("page not found");
        let page_guard = page.read();
        let raw = page_guard.get_value(slot_id);
        raw.map(|pb| {
            let decompressed = Self::decompress(pb);
            Payload::from_bytes(&decompressed)
        })
    }

    /// Find the best fitting page for a payload
    /// Returns Some(page_id) of the best fitting page or None if no page has enough space
    fn find_best_fitting_page(&self, payload_size: usize) -> Option<u32> {
        if self.pages.is_empty() {
            return None;
        }
        let mut best_page = 0;
        // best is the page with the lowest free space that fits the payload
        let mut best_fit_size = usize::MAX;

        for (page_id, page) in &self.pages {
            let free_space = page.read().free_space();
            if free_space >= payload_size && free_space < best_fit_size {
                best_page = *page_id;
                best_fit_size = free_space;
            }
        }

        // no capacity
        if best_fit_size == 0 {
            None
        } else {
            Some(best_page)
        }
    }

    /// Create a new page and return its id
    fn create_new_page(&mut self) -> u32 {
        let max_id = self.pages.keys().max().unwrap_or(&0);
        let new_page_id = max_id + 1;
        let path = self.path_page(new_page_id);
        self.add_page(
            new_page_id,
            SlottedPageMmap::new(&path, SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES),
        );
        new_page_id
    }

    /// Get the mapping for a given point offset
    fn get_mapping(&self, point_offset: PointOffset) -> Option<&PagePointer> {
        self.page_tracker.get(point_offset as usize)?.as_ref()
    }

    /// Put a payload in the storage
    pub fn put_payload(&mut self, point_offset: PointOffset, payload: Payload) {
        if self.pages.is_empty() {
            self.create_new_page();
        }

        let comp_payload = Self::compress(&payload.to_bytes());
        let payload_size = size_of_val(&comp_payload);

        if let Some(PagePointer { page_id, slot_id }) = self.get_mapping(point_offset).copied() {
            let page = self.pages.get_mut(&page_id).unwrap();
            let updated = page.write().update_value(slot_id, &comp_payload);
            if updated.is_none() {
                todo!("handle update in a new page")
                // TODO handle update in a new page
                // delete value from old page
                // find a new page (or create a new one if all full)
                // insert value in new page
            }
        } else {
            // this is a new payload
            let page_id = self
                .find_best_fitting_page(payload_size)
                .unwrap_or_else(|| {
                    // create a new page
                    self.create_new_page()
                });

            let page = self.pages.get_mut(&page_id).unwrap();
            let slot_id = page.write().insert_value(&comp_payload).unwrap();
            // ensure page_tracker is long enough
            if self.page_tracker.len() <= point_offset as usize {
                self.page_tracker.resize(point_offset as usize + 1, None);
            }
            // update page_tracker
            self.page_tracker[point_offset as usize] = Some(PagePointer {
                page_id,
                slot_id: slot_id as u32,
            });
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
        self.page_tracker[point_offset as usize] = None;
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tempfile::Builder;

    #[test]
    fn test_empty_payload_storage() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();

        let storage = PayloadStorage::new(dir.path().to_path_buf());
        let payload = storage.get_payload(0);
        assert!(payload.is_none());
    }

    #[test]
    fn test_put_empty_payload() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let mut storage = PayloadStorage::new(dir.path().to_path_buf());
        assert!(storage.pages.is_empty());
        assert!(storage.page_tracker.is_empty());

        let payload = Payload::default();
        storage.put_payload(0, payload);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.len(), 1);

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), Payload::default());
    }

    #[test]
    fn test_put_payload() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let mut storage = PayloadStorage::new(dir.path().to_path_buf());
        assert!(storage.pages.is_empty());
        assert!(storage.page_tracker.is_empty());

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        storage.put_payload(0, payload.clone());
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.len(), 1);

        let page_mapping = storage.get_mapping(0).unwrap();
        assert_eq!(page_mapping.page_id, 1); // first page
        assert_eq!(page_mapping.slot_id, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);
    }

    #[test]
    fn test_delete_payload() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let mut storage = PayloadStorage::new(dir.path().to_path_buf());
        assert!(storage.pages.is_empty());
        assert!(storage.page_tracker.is_empty());

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
    fn test_update_payload() {
        let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
        let mut storage = PayloadStorage::new(dir.path().to_path_buf());
        assert!(storage.pages.is_empty());
        assert!(storage.page_tracker.is_empty());

        let mut payload = Payload::default();
        payload
            .0
            .insert("key".to_string(), Value::String("value".to_string()));

        storage.put_payload(0, payload.clone());
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.len(), 1);

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
        assert_eq!(storage.page_tracker.len(), 1);

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), updated_payload);
    }
}
