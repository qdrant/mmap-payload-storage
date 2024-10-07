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

/// When compacting, we collect the point offsets moved to the new page with the new slot id.
pub type OffsetsToSlots = Vec<(PointOffset, SlotId)>;

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

#[derive(Debug, Default, Clone)]
struct PageTrackerHeader {
    max_point_offset: u32,
}

#[derive(Debug)]
pub struct PageTracker {
    path: PathBuf,             // path to the file
    header: PageTrackerHeader, // header of the file
    mmap: MmapMut,             // mmap of the file
}

impl PageTracker {
    const FILE_NAME: &'static str = "page_tracker.dat";
    const DEFAULT_SIZE: usize = 1024 * 1024; // 1MB

    /// Create a new PageTracker at the given dir path
    /// The file is created with the default size if no size hint is given
    pub fn new(path: &Path, size_hint: Option<usize>) -> Self {
        let path = path.join(Self::FILE_NAME);
        let size = size_hint.unwrap_or(Self::DEFAULT_SIZE);
        assert!(
            size > size_of::<PageTrackerHeader>(),
            "Size hint is too small"
        );
        create_and_ensure_length(&path, size).unwrap();
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header = PageTrackerHeader::default();
        let mut page_tracker = Self { path, header, mmap };
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
        Some(Self {
            path,
            header: header.clone(),
            mmap,
        })
    }

    /// Return the size of the underlying mmaped file
    pub fn mmap_file_size(&self) -> usize {
        self.mmap.len()
    }

    pub fn all_page_ids(&self) -> HashSet<PageId> {
        let mut page_ids = HashSet::new();
        for i in 0..self.header.max_point_offset {
            let start_offset =
                size_of::<PageTrackerHeader>() + i as usize * size_of::<Option<PagePointer>>();
            let end_offset = start_offset + size_of::<Option<PagePointer>>();
            let page_pointer: &Option<PagePointer> =
                transmute_from_u8(&self.mmap[start_offset..end_offset]);
            if let Some(page_pointer) = page_pointer {
                page_ids.insert(page_pointer.page_id);
            }
        }
        page_ids
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
    fn persist_pointer(&mut self, point_offset: PointOffset, pointer: Option<PagePointer>) {
        let point_offset = point_offset as usize;
        let start_offset =
            size_of::<PageTrackerHeader>() + point_offset * size_of::<Option<PagePointer>>();
        let end_offset = start_offset + size_of::<Option<PagePointer>>();
        // check if file is long enough
        if self.mmap.len() < end_offset {
            // flush the current mmap
            self.mmap.flush().unwrap();
            let missing_space = end_offset - self.mmap.len();
            // reopen the file with a larger size
            // account for missing size + extra to avoid resizing too often
            let new_size = self.mmap.len() + missing_space + Self::DEFAULT_SIZE;
            create_and_ensure_length(&self.path, new_size).unwrap();
            self.mmap = open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal)).unwrap();
        }
        self.mmap[start_offset..end_offset].copy_from_slice(transmute_to_u8(&pointer));
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.mapping_len() == 0
    }

    /// Get the length of the mapping
    /// Excludes None values
    #[cfg(test)]
    pub fn mapping_len(&self) -> usize {
        (0..self.header.max_point_offset)
            .filter(|&i| {
                let start_offset =
                    size_of::<PageTrackerHeader>() + i as usize * size_of::<Option<PagePointer>>();
                let end_offset = start_offset + size_of::<Option<PagePointer>>();
                let page_pointer: &Option<PagePointer> =
                    transmute_from_u8(&self.mmap[start_offset..end_offset]);
                page_pointer.is_some()
            })
            .count()
    }

    /// Get the raw value at the given point offset
    fn get_raw(&self, point_offset: PointOffset) -> Option<&Option<PagePointer>> {
        let start_offset = size_of::<PageTrackerHeader>()
            + point_offset as usize * size_of::<Option<PagePointer>>();
        let end_offset = start_offset + size_of::<Option<PagePointer>>();
        if end_offset > self.mmap.len() {
            return None;
        }
        let page_pointer = transmute_from_u8(&self.mmap[start_offset..end_offset]);
        Some(page_pointer)
    }

    /// Get the page pointer at the given point offset
    pub fn get(&self, point_offset: PointOffset) -> Option<PagePointer> {
        self.get_raw(point_offset).and_then(|pointer| *pointer)
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
    pub fn set(&mut self, point_offset: PointOffset, page_pointer: PagePointer) {
        // save mapping to mmap
        self.persist_pointer(point_offset, Some(page_pointer));
        // increment header count if necessary
        self.increment_max_point_offset(point_offset);
    }

    pub fn unset(&mut self, point_offset: PointOffset) {
        if (point_offset as usize) < self.mmap.len() {
            // save mapping to mmap
            self.persist_pointer(point_offset, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::page_tracker::{PagePointer, PageTracker};
    use rstest::rstest;
    use tempfile::Builder;

    #[test]
    fn test_new_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let tracker = PageTracker::new(path, None);
        assert!(tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.header_count(), 0);
        assert_eq!(tracker.all_page_ids().len(), 0);
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_mapping_len_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let mut tracker = PageTracker::new(path, Some(initial_tracker_size));
        assert!(tracker.is_empty());
        tracker.set(0, PagePointer::new(1, 1));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 1);

        tracker.set(100, PagePointer::new(2, 2));
        assert_eq!(tracker.header_count(), 101);
        assert_eq!(tracker.mapping_len(), 2);
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_set_get_clear_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let mut tracker = PageTracker::new(path, Some(initial_tracker_size));
        tracker.set(0, PagePointer::new(1, 1));
        tracker.set(1, PagePointer::new(2, 2));
        tracker.set(2, PagePointer::new(3, 3));
        tracker.set(10, PagePointer::new(10, 10));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 4);
        assert_eq!(tracker.header_count(), 11); // accounts for empty slots

        assert_eq!(tracker.get_raw(0), Some(&Some(PagePointer::new(1, 1))));
        assert_eq!(tracker.get_raw(1), Some(&Some(PagePointer::new(2, 2))));
        assert_eq!(tracker.get_raw(2), Some(&Some(PagePointer::new(3, 3))));
        assert_eq!(tracker.get_raw(3), Some(&None)); // intermediate empty slot
        assert_eq!(tracker.get_raw(10), Some(&Some(PagePointer::new(10, 10))));
        assert_eq!(tracker.get_raw(100_000), None); // out of bounds

        tracker.unset(1);
        // the value has been cleared but the entry is still there
        assert_eq!(tracker.get_raw(1), Some(&None));
        assert_eq!(tracker.get(1), None);

        assert_eq!(tracker.mapping_len(), 3);
        assert_eq!(tracker.header_count(), 11);

        // overwrite some values
        tracker.set(0, PagePointer::new(10, 10));
        tracker.set(2, PagePointer::new(30, 30));

        assert_eq!(tracker.get(0), Some(PagePointer::new(10, 10)));
        assert_eq!(tracker.get(2), Some(PagePointer::new(30, 30)));
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_persist_and_open_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let value_count: usize = 1000;

        let mut tracker = PageTracker::new(path, Some(initial_tracker_size));

        for i in 0..value_count {
            // save only half of the values
            if i % 2 == 0 {
                tracker.set(i as u32, PagePointer::new(i as u32, i as u32));
            }
        }

        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.header_count(), value_count as u32 - 1);

        // drop the tracker
        drop(tracker);

        // reopen the tracker
        let tracker = PageTracker::open(path).unwrap();
        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.header_count(), value_count as u32 - 1);

        // check the values
        for i in 0..value_count {
            if i % 2 == 0 {
                assert_eq!(
                    tracker.get(i as u32),
                    Some(PagePointer::new(i as u32, i as u32))
                );
            } else {
                assert_eq!(tracker.get(i as u32), None);
            }
        }
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_page_tracker_resize(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let mut tracker = PageTracker::new(path, Some(initial_tracker_size));
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.mmap_file_size(), initial_tracker_size);

        for i in 0..100_000 {
            tracker.set(i as u32, PagePointer::new(i as u32, i as u32));
        }
        assert_eq!(tracker.mapping_len(), 100_000);
        assert!(tracker.mmap_file_size() > initial_tracker_size);
    }

    #[test]
    fn test_track_non_sequential_large_offset() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let mut tracker = PageTracker::new(path, None);
        assert_eq!(tracker.mapping_len(), 0);

        let page_pointer = PagePointer::new(1, 1);
        let key = 1_000_000;

        tracker.set(key, page_pointer);
        assert_eq!(tracker.get(key), Some(page_pointer));
    }
}
