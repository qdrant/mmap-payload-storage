use crate::slotted_page::SlotId;
use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_to_u8,
};
use memmap2::MmapMut;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub type PointOffset = u32;
pub type PageId = u32;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PagePointer {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

impl PagePointer {
    pub fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Self { page_id, slot_id }
    }
}

#[derive(Default, Clone)]
struct PageTrackerHeader {
    max_point_offset: u32,
}

pub struct PageTracker {
    path: PathBuf,                     // path to the file
    mapping: Vec<Option<PagePointer>>, // points_offsets are contiguous
    header: PageTrackerHeader,         // header of the file
    mmap: MmapMut,                     // mmap of the file
}

impl PageTracker {
    const FILE_NAME: &'static str = "page_tracker.dat";
    const DEFAULT_SIZE: usize = 1024 * 1024; // 1MB

    /// Create a new PageTracker at the given dir path
    /// The file is created with the default size if no size hint is given
    pub fn new(path: &Path, size_hint: Option<usize>) -> Self {
        let path = path.join(Self::FILE_NAME);
        let size = size_hint.unwrap_or(Self::DEFAULT_SIZE);
        create_and_ensure_length(&path, size).unwrap();
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header = PageTrackerHeader::default();
        let mut page_tracker = Self {
            path,
            mapping: Vec::new(),
            header,
            mmap,
        };
        page_tracker.write_header();
        page_tracker
    }

    /// Open an existing PageTracker at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Option<Self> {
        let path = path.join(Self::FILE_NAME);
        if !path.exists() {
            return None;
        }
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header: &PageTrackerHeader =
            transmute_from_u8(&mmap[0..size_of::<PageTrackerHeader>()]);
        let mut mapping = Vec::with_capacity(header.max_point_offset as usize);
        for i in 0..header.max_point_offset {
            let start_offset =
                size_of::<PageTrackerHeader>() + i as usize * size_of::<Option<PagePointer>>();
            let end_offset = start_offset + size_of::<Option<PagePointer>>();
            let page_pointer: &Option<PagePointer> =
                transmute_from_u8(&mmap[start_offset..end_offset]);
            mapping.push(*page_pointer);
        }
        Some(Self {
            path,
            mapping,
            header: header.clone(),
            mmap,
        })
    }

    /// Return the size of the underlying mmaped file
    pub fn mmap_file_size(&self) -> usize {
        self.mmap.len()
    }

    pub fn all_page_ids(&self) -> HashSet<PageId> {
        self.mapping
            .iter()
            .filter_map(|x| x.map(|y| y.page_id))
            .collect()
    }

    pub fn header_count(&self) -> u32 {
        self.header.max_point_offset
    }

    /// Write the current page header to the memory map
    fn write_header(&mut self) {
        self.mmap[0..size_of::<PageTrackerHeader>()].copy_from_slice(transmute_to_u8(&self.header));
    }

    /// Save the mapping at the given offset
    /// The file is resized if necessary
    fn persist_pointer(&mut self, point_offset: usize) {
        let start =
            size_of::<PageTrackerHeader>() + point_offset * size_of::<Option<PagePointer>>();
        // check if file is long enough
        if self.mmap.len() < start + size_of::<Option<PagePointer>>() {
            // flush the current mmap
            self.mmap.flush().unwrap();
            // reopen the file with a larger size (bump by DEFAULT_SIZE)
            let new_size = self.mmap.len() + Self::DEFAULT_SIZE;
            create_and_ensure_length(&self.path, new_size).unwrap();
            self.mmap = open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal)).unwrap();
        }
        let end = start + size_of::<Option<PagePointer>>();
        self.mmap[start..end].copy_from_slice(transmute_to_u8(&self.mapping[point_offset]));
    }

    pub fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }

    /// Get the length of the mapping
    /// Includes None values
    pub fn raw_mapping_len(&self) -> usize {
        self.mapping.len()
    }

    /// Get the length of the mapping
    /// Excludes None values
    #[cfg(test)]
    pub fn mapping_len(&self) -> usize {
        self.mapping.iter().filter(|x| x.is_some()).count()
    }

    /// Get the raw value at the given point offset
    /// For testing purposes
    #[cfg(test)]
    fn get_raw(&self, point_offset: u32) -> Option<&Option<PagePointer>> {
        self.mapping.get(point_offset as usize)
    }

    /// Get the page pointer at the given point offset
    pub fn get(&self, point_offset: u32) -> Option<&PagePointer> {
        self.mapping
            .get(point_offset as usize)
            .and_then(|x| x.as_ref())
    }

    /// Increment the header count if the given point offset is larger than the current count
    fn increment_max_point_offset(&mut self, point_offset: PointOffset) {
        if point_offset >= self.header.max_point_offset {
            self.header.max_point_offset = point_offset + 1;
            self.write_header();
        }
    }

    /// Set value at the given point offset
    /// If the point offset is larger than the current length, the mapping is resized
    pub fn set(&mut self, point_offset: u32, page_pointer: PagePointer) {
        let point_offset = point_offset as usize;
        // save in memory mapping vector
        self.ensure_mapping_length(point_offset + 1);
        self.mapping[point_offset] = Some(page_pointer);
        // save mapping to mmap
        self.persist_pointer(point_offset);
        // increment header count if necessary
        self.increment_max_point_offset(point_offset as u32);
    }

    pub fn unset(&mut self, point_offset: u32) {
        let point_offset = point_offset as usize;
        if point_offset < self.mapping.len() {
            // clear in memory mapping vector
            self.mapping[point_offset] = None;
            // save mapping to mmap
            self.persist_pointer(point_offset);
        }
    }

    fn resize_mapping(&mut self, new_len: usize) {
        self.mapping.resize_with(new_len, || None);
    }

    fn ensure_mapping_length(&mut self, new_len: usize) {
        if self.mapping.len() < new_len {
            self.resize_mapping(new_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::page_tracker::{PagePointer, PageTracker};
    use tempfile::Builder;

    #[test]
    fn test_new_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let tracker = PageTracker::new(path, None);
        assert!(tracker.is_empty());
        assert_eq!(tracker.raw_mapping_len(), 0);
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.header_count(), 0);
        assert_eq!(tracker.all_page_ids().len(), 0);
    }

    #[test]
    fn test_mapping_len_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let mut tracker = PageTracker::new(path, None);
        assert!(tracker.is_empty());
        tracker.set(0, PagePointer::new(1, 1));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.raw_mapping_len(), 1);
        assert_eq!(tracker.mapping_len(), 1);

        tracker.set(100, PagePointer::new(2, 2));
        assert_eq!(tracker.raw_mapping_len(), 101);
        assert_eq!(tracker.mapping_len(), 2);
    }

    #[test]
    fn test_set_get_clear_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let mut tracker = PageTracker::new(path, None);
        tracker.set(0, PagePointer::new(1, 1));
        tracker.set(1, PagePointer::new(2, 2));
        tracker.set(2, PagePointer::new(3, 3));
        tracker.set(10, PagePointer::new(10, 10));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 4);
        assert_eq!(tracker.raw_mapping_len(), 11); // accounts for empty slots
        assert_eq!(tracker.header_count(), 11);

        assert_eq!(tracker.get_raw(0), Some(&Some(PagePointer::new(1, 1))));
        assert_eq!(tracker.get_raw(1), Some(&Some(PagePointer::new(2, 2))));
        assert_eq!(tracker.get_raw(2), Some(&Some(PagePointer::new(3, 3))));
        assert_eq!(tracker.get_raw(3), Some(&None)); // intermediate empty slot
        assert_eq!(tracker.get_raw(10), Some(&Some(PagePointer::new(10, 10))));
        assert_eq!(tracker.get_raw(1000), None); // out of bounds

        tracker.unset(1);
        // the value has been cleared but the entry is still there
        assert_eq!(tracker.get_raw(1), Some(&None));
        assert_eq!(tracker.get(1), None);

        assert_eq!(tracker.mapping_len(), 3);
        assert_eq!(tracker.raw_mapping_len(), 11);
        assert_eq!(tracker.header_count(), 11);

        // overwrite some values
        tracker.set(0, PagePointer::new(10, 10));
        tracker.set(2, PagePointer::new(30, 30));

        assert_eq!(tracker.get(0), Some(&PagePointer::new(10, 10)));
        assert_eq!(tracker.get(2), Some(&PagePointer::new(30, 30)));
    }

    #[test]
    fn test_persist_and_open_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let value_count: usize = 1000;

        let mut tracker = PageTracker::new(path, None);

        for i in 0..value_count {
            // save only half of the values
            if i % 2 == 0 {
                tracker.set(i as u32, PagePointer::new(i as u32, i as u32));
            }
        }

        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.raw_mapping_len(), value_count - 1);
        assert_eq!(tracker.header_count(), value_count as u32 - 1);

        // drop the tracker
        drop(tracker);

        // reopen the tracker
        let tracker = PageTracker::open(path).unwrap();
        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.raw_mapping_len(), value_count - 1);
        assert_eq!(tracker.header_count(), value_count as u32 - 1);

        // check the values
        for i in 0..value_count {
            if i % 2 == 0 {
                assert_eq!(
                    tracker.get(i as u32),
                    Some(&PagePointer::new(i as u32, i as u32))
                );
            } else {
                assert_eq!(tracker.get(i as u32), None);
            }
        }
    }

    #[test]
    fn test_page_tracker_resize() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        // create a tracker with a tiny size to force a resize
        let small_size = 10;
        let mut tracker = PageTracker::new(path, Some(small_size));
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.mmap_file_size(), small_size);

        for i in 0..100000 {
            tracker.set(i as u32, PagePointer::new(i as u32, i as u32));
        }
        assert_eq!(tracker.mapping_len(), 100000);
        assert!(tracker.mmap_file_size() > small_size);
    }
}
