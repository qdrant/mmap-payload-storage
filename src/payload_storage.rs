use crate::payload::Payload;
use crate::slotted_page::SlottedPageMmap;
use std::collections::HashMap;
use std::path::PathBuf;

type PointOffset = u32;
type PagePointer = (u32, u32); // (PageId, SlotId)

struct PayloadStorage {
    page_tracker: HashMap<PointOffset, PagePointer>, // TODO make Vec<(PageId, SlotId)> as point_offset are contiguous
    pages: HashMap<u32, SlottedPageMmap>, // TODO Arc<Mutex<SlottedPageMmap>> for concurrent reads
    base_path: PathBuf,
}

impl PayloadStorage {
    pub fn new(path: PathBuf) -> Self {
        Self {
            page_tracker: HashMap::new(),
            pages: HashMap::new(),
            base_path: path,
        }
    }

    pub fn path_page(&self, page_id: u32) -> PathBuf {
        self.base_path
            .join(format!("slotted-paged-{}.dat", page_id))
    }

    fn add_page(&mut self, page_id: u32, page: SlottedPageMmap) {
        let page_exists = self.pages.contains_key(&page_id);
        if page_exists {
            panic!("page already exists");
        }
        self.pages.insert(page_id, page);
    }

    pub fn get_payload(&self, point_offset: PointOffset) -> Option<Payload> {
        let (page_id, slot_id) = *self.page_tracker.get(&point_offset)?;
        let page = self.pages.get(&page_id).expect("page not found");
        let raw = page.read_raw(&slot_id);
        raw.map(Payload::from_binary)
    }

    // TODO needs dedicated tests
    fn find_best_fitting_page(&self, payload_size: usize) -> Option<u32> {
        if self.pages.is_empty() {
            return None;
        }
        let mut best_page = 0;
        // best is the page with the lowest free space that fits the payload
        let mut best_fit_size = usize::MAX;
        for (page_id, page) in &self.pages {
            let free_space = page.free_space();
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

    pub fn put_payload(&mut self, point_offset: PointOffset, payload: Payload) {
        if self.pages.is_empty() {
            self.create_new_page();
        }

        let payload_bin = payload.binary();
        let payload_size = size_of_val(&payload_bin);

        let mapping_exists = self.page_tracker.contains_key(&point_offset);
        if mapping_exists {
            let (page_id, slot_id) = self.page_tracker.get(&point_offset).unwrap();
            let page = self.pages.get_mut(page_id).unwrap();
            let updated = page.update_value(*slot_id as usize, &payload_bin);
            if updated.is_none() {
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
            let slot_id = page.push(Some(&payload_bin)).unwrap();
            self.page_tracker
                .insert(point_offset, (page_id, slot_id as u32));
        }
    }

    pub fn delete_payload(&mut self, point_offset: PointOffset) {
        let (page_id, slot_id) = self.page_tracker.remove(&point_offset).unwrap();
        let page = self.pages.get_mut(&page_id).unwrap();
        page.delete(slot_id as usize);
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

        let page_mapping = storage.page_tracker.get(&0).unwrap();
        assert_eq!(page_mapping.0, 1); // first page
        assert_eq!(page_mapping.1, 0); // first slot

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
        assert_eq!(storage.page_tracker.len(), 1);

        let page_mapping = storage.page_tracker.get(&0).unwrap();
        assert_eq!(page_mapping.0, 1); // first page
        assert_eq!(page_mapping.1, 0); // first slot

        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_some());
        assert_eq!(stored_payload.unwrap(), payload);

        // delete payload
        storage.delete_payload(0);
        assert_eq!(storage.pages.len(), 1);
        assert_eq!(storage.page_tracker.len(), 0);

        // get payload again
        let stored_payload = storage.get_payload(0);
        assert!(stored_payload.is_none());
    }
}
