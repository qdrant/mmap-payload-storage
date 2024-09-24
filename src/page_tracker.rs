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

#[derive(Debug, Clone, Copy)]
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
    count: u32,
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

    /// Create a new PageTracker at the given path
    pub fn new(path: &Path) -> Self {
        let path = path.join(Self::FILE_NAME);
        create_and_ensure_length(&path, Self::DEFAULT_SIZE).unwrap();
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
        let mut mapping = Vec::with_capacity(header.count as usize);
        for i in 0..header.count {
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

    pub fn all_page_ids(&self) -> HashSet<PageId> {
        self.mapping
            .iter()
            .filter_map(|x| x.map(|y| y.page_id))
            .collect()
    }

    pub fn header_len(&self) -> u32 {
        self.header.count
    }

    /// Write the current page header to the memory map
    fn write_header(&mut self) {
        self.mmap[0..size_of::<PageTrackerHeader>()].copy_from_slice(transmute_to_u8(&self.header));
    }

    /// Save the mapping at the given offset
    fn save_mapping_at(&mut self, offset: usize) {
        let start = size_of::<PageTrackerHeader>() + offset * size_of::<Option<PagePointer>>();
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
        self.mmap[start..end].copy_from_slice(transmute_to_u8(&self.mapping[offset]));
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
    pub fn mapping_len(&self) -> usize {
        self.mapping.iter().filter(|x| x.is_some()).count()
    }

    pub fn get(&self, point_offset: u32) -> Option<&Option<PagePointer>> {
        self.mapping.get(point_offset as usize)
    }

    fn increment_header_count(&mut self, point_offset: PointOffset) {
        if point_offset >= self.header.count {
            self.header.count = point_offset + 1;
            self.write_header();
        }
    }

    /// Set value at the given point offset
    /// If the point offset is larger than the current length, the mapping is resized
    pub fn set(&mut self, point_offset: u32, page_pointer: PagePointer) {
        let point_offset = point_offset as usize;
        // save in memory mapping vector
        self.ensure_length(point_offset + 1);
        self.mapping[point_offset] = Some(page_pointer);
        // save mapping to mmap
        self.save_mapping_at(point_offset);
        // increment header count if necessary
        self.increment_header_count(point_offset as u32);
    }

    pub fn clear_mapping(&mut self, point_offset: u32) {
        let point_offset = point_offset as usize;
        if point_offset < self.mapping.len() {
            // clear in memory mapping vector
            self.mapping[point_offset] = None;
            // save mapping to mmap
            self.save_mapping_at(point_offset);
        }
    }

    fn resize(&mut self, new_len: usize) {
        self.mapping.resize_with(new_len, || None);
    }

    pub fn ensure_length(&mut self, new_len: usize) {
        if self.mapping.len() < new_len {
            self.resize(new_len);
        }
    }
}
