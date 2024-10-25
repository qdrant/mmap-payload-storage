use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_to_u8,
};
use memmap2::MmapMut;
use std::path::{Path, PathBuf};

pub type PointOffset = u32;
pub type BlockOffset = u32;
pub type PageId = u32;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ValuePointer {
    /// Which page the value is stored in
    pub page_id: PageId,

    /// Start offset (in blocks) of the value
    pub block_offset: BlockOffset,

    /// Length in bytes of the value
    pub length: u32,
}

impl ValuePointer {
    pub fn new(page_id: PageId, block_offset: BlockOffset, length: u32) -> Self {
        Self {
            page_id,
            block_offset,
            length,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct TrackerHeader {
    max_point_offset: u32,
}

#[derive(Debug)]
pub struct Tracker {
    path: PathBuf,         // path to the file
    header: TrackerHeader, // header of the file
    mmap: MmapMut,         // mmap of the file
}

impl Tracker {
    const FILE_NAME: &'static str = "page_tracker.dat";
    const DEFAULT_SIZE: usize = 1024 * 1024; // 1MB

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn tracker_file_name(path: &Path) -> PathBuf {
        path.join(Self::FILE_NAME)
    }

    /// Create a new PageTracker at the given dir path
    /// The file is created with the default size if no size hint is given
    pub fn new(path: &Path, size_hint: Option<usize>) -> Self {
        let path = Self::tracker_file_name(path);
        let size = size_hint.unwrap_or(Self::DEFAULT_SIZE);
        assert!(size > size_of::<TrackerHeader>(), "Size hint is too small");
        create_and_ensure_length(&path, size).expect("Failed to create page tracker file");
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false)
            .expect("Failed to open page tracker mmap");
        let header = TrackerHeader::default();
        let mut page_tracker = Self { path, header, mmap };
        page_tracker.write_header();
        page_tracker
    }

    /// Open an existing PageTracker at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Option<Self> {
        let path = Self::tracker_file_name(path);
        if !path.exists() {
            return None;
        }
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let header: &TrackerHeader = transmute_from_u8(&mmap[0..size_of::<TrackerHeader>()]);
        Some(Self {
            path,
            header: header.clone(),
            mmap,
        })
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    /// Return the size of the underlying mmaped file
    #[cfg(test)]
    pub fn mmap_file_size(&self) -> usize {
        self.mmap.len()
    }

    #[cfg(test)]
    pub fn header_count(&self) -> u32 {
        self.header.max_point_offset
    }

    /// Write the current page header to the memory map
    fn write_header(&mut self) {
        self.mmap[0..size_of::<TrackerHeader>()].copy_from_slice(transmute_to_u8(&self.header));
    }

    /// Save the mapping at the given offset
    /// The file is resized if necessary
    fn persist_pointer(&mut self, point_offset: PointOffset, pointer: Option<ValuePointer>) {
        let point_offset = point_offset as usize;
        let start_offset =
            size_of::<TrackerHeader>() + point_offset * size_of::<Option<ValuePointer>>();
        let end_offset = start_offset + size_of::<Option<ValuePointer>>();
        // check if file is long enough
        if self.mmap.len() < end_offset {
            // flush the current mmap
            self.flush().unwrap();
            let missing_space = end_offset - self.mmap.len();
            // reopen the file with a larger size
            // account for missing size + extra to avoid resizing too often
            let new_size = self.mmap.len() + missing_space + Self::DEFAULT_SIZE;
            create_and_ensure_length(&self.path, new_size).unwrap();
            self.mmap =
                open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal), false).unwrap();
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
                    size_of::<TrackerHeader>() + i as usize * size_of::<Option<ValuePointer>>();
                let end_offset = start_offset + size_of::<Option<ValuePointer>>();
                let page_pointer: &Option<ValuePointer> =
                    transmute_from_u8(&self.mmap[start_offset..end_offset]);
                page_pointer.is_some()
            })
            .count()
    }

    /// Iterate over the pointers in the tracker
    pub fn iter_pointers(&self) -> impl Iterator<Item = Option<ValuePointer>> + '_ {
        (0..self.header.max_point_offset).map(move |i| self.get(i as PointOffset))
    }

    /// Get the raw value at the given point offset
    fn get_raw(&self, point_offset: PointOffset) -> Option<&Option<ValuePointer>> {
        let start_offset =
            size_of::<TrackerHeader>() + point_offset as usize * size_of::<Option<ValuePointer>>();
        let end_offset = start_offset + size_of::<Option<ValuePointer>>();
        if end_offset > self.mmap.len() {
            return None;
        }
        let page_pointer = transmute_from_u8(&self.mmap[start_offset..end_offset]);
        Some(page_pointer)
    }

    /// Get the page pointer at the given point offset
    pub fn get(&self, point_offset: PointOffset) -> Option<ValuePointer> {
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
    pub fn set(&mut self, point_offset: PointOffset, value_pointer: ValuePointer) {
        // save mapping to mmap
        self.persist_pointer(point_offset, Some(value_pointer));
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
    use crate::tracker::{Tracker, ValuePointer};
    use rstest::rstest;
    use std::path::PathBuf;
    use tempfile::Builder;

    #[test]
    fn test_file_name() {
        let path: PathBuf = "/tmp/test".into();
        let file_name = Tracker::tracker_file_name(&path);
        assert_eq!(file_name, path.join(Tracker::FILE_NAME));
    }

    #[test]
    fn test_page_tracker_files() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let tracker = Tracker::new(path, None);
        let files = tracker.files();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], path.join(Tracker::FILE_NAME));
    }

    #[test]
    fn test_new_tracker() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let tracker = Tracker::new(path, None);
        assert!(tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.header_count(), 0);
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_mapping_len_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();
        let mut tracker = Tracker::new(path, Some(initial_tracker_size));
        assert!(tracker.is_empty());
        tracker.set(0, ValuePointer::new(1, 1, 1));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 1);

        tracker.set(100, ValuePointer::new(2, 2, 2));
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
        let mut tracker = Tracker::new(path, Some(initial_tracker_size));
        tracker.set(0, ValuePointer::new(1, 1, 1));
        tracker.set(1, ValuePointer::new(2, 2, 2));
        tracker.set(2, ValuePointer::new(3, 3, 3));
        tracker.set(10, ValuePointer::new(10, 10, 10));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.mapping_len(), 4);
        assert_eq!(tracker.header_count(), 11); // accounts for empty slots

        assert_eq!(tracker.get_raw(0), Some(&Some(ValuePointer::new(1, 1, 1))));
        assert_eq!(tracker.get_raw(1), Some(&Some(ValuePointer::new(2, 2, 2))));
        assert_eq!(tracker.get_raw(2), Some(&Some(ValuePointer::new(3, 3, 3))));
        assert_eq!(tracker.get_raw(3), Some(&None)); // intermediate empty slot
        assert_eq!(
            tracker.get_raw(10),
            Some(&Some(ValuePointer::new(10, 10, 10)))
        );
        assert_eq!(tracker.get_raw(100_000), None); // out of bounds

        tracker.unset(1);
        // the value has been cleared but the entry is still there
        assert_eq!(tracker.get_raw(1), Some(&None));
        assert_eq!(tracker.get(1), None);

        assert_eq!(tracker.mapping_len(), 3);
        assert_eq!(tracker.header_count(), 11);

        // overwrite some values
        tracker.set(0, ValuePointer::new(10, 10, 10));
        tracker.set(2, ValuePointer::new(30, 30, 30));

        assert_eq!(tracker.get(0), Some(ValuePointer::new(10, 10, 10)));
        assert_eq!(tracker.get(2), Some(ValuePointer::new(30, 30, 30)));
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_persist_and_open_tracker(#[case] initial_tracker_size: usize) {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let value_count: usize = 1000;

        let mut tracker = Tracker::new(path, Some(initial_tracker_size));

        for i in 0..value_count {
            // save only half of the values
            if i % 2 == 0 {
                tracker.set(i as u32, ValuePointer::new(i as u32, i as u32, i as u32));
            }
        }

        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.header_count(), value_count as u32 - 1);

        // drop the tracker
        drop(tracker);

        // reopen the tracker
        let tracker = Tracker::open(path).unwrap();
        assert_eq!(tracker.mapping_len(), value_count / 2);
        assert_eq!(tracker.header_count(), value_count as u32 - 1);

        // check the values
        for i in 0..value_count {
            if i % 2 == 0 {
                assert_eq!(
                    tracker.get(i as u32),
                    Some(ValuePointer::new(i as u32, i as u32, i as u32))
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

        let mut tracker = Tracker::new(path, Some(initial_tracker_size));
        assert_eq!(tracker.mapping_len(), 0);
        assert_eq!(tracker.mmap_file_size(), initial_tracker_size);

        for i in 0..100_000 {
            tracker.set(i, ValuePointer::new(i, i, i));
        }
        assert_eq!(tracker.mapping_len(), 100_000);
        assert!(tracker.mmap_file_size() > initial_tracker_size);
    }

    #[test]
    fn test_track_non_sequential_large_offset() {
        let file = Builder::new().prefix("test-tracker").tempdir().unwrap();
        let path = file.path();

        let mut tracker = Tracker::new(path, None);
        assert_eq!(tracker.mapping_len(), 0);

        let page_pointer = ValuePointer::new(1, 1, 1);
        let key = 1_000_000;

        tracker.set(key, page_pointer);
        assert_eq!(tracker.get(key), Some(page_pointer));
    }
}
