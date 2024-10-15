use crate::page_tracker::PointOffset;
use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_to_u8,
};
use memmap2::MmapMut;
use std::path::{Path, PathBuf};

pub type SlotId = u32;

#[derive(Debug, Clone)]
struct SlottedPageHeader {
    /// How many slots are in the page
    slot_count: u64,

    /// The offset within the page where the data starts
    data_start_offset: u64,

    /// The page size in bytes.
    page_size: u64,

    /// The number of bytes in between the data.
    fragmented_bytes: u64,
}

impl SlottedPageHeader {
    fn new(required_size: usize) -> SlottedPageHeader {
        SlottedPageHeader {
            slot_count: 0,
            data_start_offset: required_size as u64,
            page_size: required_size as u64,
            fragmented_bytes: 0,
        }
    }

    fn page_size(&self) -> usize {
        self.page_size as usize
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SlotHeader {
    offset: u64,               // offset in the page (8 bytes)
    length: u64,               // length of the cell (8 bytes)
    point_offset: PointOffset, // point id (4 bytes)
    right_padding: u16, // right padding within the cell, which is not part of the value (2 byte)
    deleted: bool,      // whether the value has been deleted (1 byte)
    _align: [u8; 1],    // 1 byte for alignment
}

impl SlotHeader {
    fn new(
        point_offset: PointOffset,
        offset: u64,
        length: u64,
        right_padding: u16,
        deleted: bool,
    ) -> SlotHeader {
        assert!(
            length >= SlottedPageMmap::MIN_CELL_SIZE_BYTES as u64,
            "Value too small"
        );
        SlotHeader {
            point_offset,
            offset,
            length,
            right_padding,
            deleted,
            _align: [0; 1],
        }
    }

    pub fn point_offset(&self) -> u32 {
        self.point_offset
    }

    pub fn deleted(&self) -> bool {
        self.deleted
    }
}

#[derive(Debug)]
pub(crate) struct SlottedPageMmap {
    path: PathBuf,
    header: SlottedPageHeader,
    mmap: MmapMut,
}

impl SlottedPageMmap {
    /// Expect JSON values to have roughly 3–5 fields with mostly small values.
    /// Therefore, reserve 128 bytes for each value in order to avoid frequent reallocations.
    /// For 1M values, this would require 128MB of memory.
    const MIN_CELL_SIZE_BYTES: usize = 128;

    /// Placeholder value for empty slots
    const PLACEHOLDER_VALUE: [u8; SlottedPageMmap::MIN_CELL_SIZE_BYTES] =
        [0; SlottedPageMmap::MIN_CELL_SIZE_BYTES];

    pub const FRAGMENTATION_THRESHOLD_RATIO: f32 = 0.5;

    /// Minimum new page size required for a value of the given size.
    pub fn new_page_size_for_value(value_size: usize) -> usize {
        size_of::<SlottedPageHeader>() + Self::required_space_for_new_value(value_size)
    }

    /// Cell size required to store a value of the given size.
    fn cell_size_for_value(value_size: usize) -> usize {
        // The value size should be at least the minimum cell size, and always be a multiple of it.
        value_size.next_multiple_of(Self::MIN_CELL_SIZE_BYTES)
    }

    /// Minimum free space required for a value of the given size.
    pub fn required_space_for_new_value(value_size: usize) -> usize {
        Self::cell_size_for_value(value_size) + size_of::<SlotHeader>()
    }

    /// Flushes outstanding memory map modifications to disk.
    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    /// Return all values in the page with placeholder or deleted values as `None`
    #[cfg(test)]
    pub fn all_values(&self) -> Vec<Option<&[u8]>> {
        self.iter_slot_values_starting_from(0)
            .map(|(_, value)| value)
            .collect()
    }

    /// Iterate over all values in the page, starting at the provided slot id.
    ///
    /// `None` values can be either placeholders, or deleted values
    pub fn iter_slot_values_starting_from(
        &self,
        slot_id: SlotId,
    ) -> impl Iterator<Item = (&SlotHeader, Option<&[u8]>)> + '_ {
        if slot_id as u64 >= self.header.slot_count && self.header.slot_count > 0 {
            panic!("Slot id out of bounds")
        }

        (slot_id as u64..self.header.slot_count).map(move |i| {
            let slot = self.get_slot_ref(&(i as u32)).unwrap();
            let value = if slot.deleted {
                None
            } else {
                self.get_slot_value(slot)
            };

            (slot, value)
        })
    }

    /// Returns all non deleted values in the page. `None` values means that the slot is a placeholder
    #[cfg(test)]
    fn non_deleted_values(&self) -> Vec<&[u8]> {
        let mut values = Vec::new();
        for i in 0..self.header.slot_count {
            let slot = self.get_slot(&(i as u32)).unwrap();
            // skip values associated with deleted slots
            if slot.deleted {
                continue;
            }
            if let Some(value) = self.get_slot_value(&slot) {
                values.push(value)
            }
        }
        values
    }

    /// Write the current page header to the memory map
    fn write_page_header(&mut self) {
        self.mmap[0..size_of::<SlottedPageHeader>()].copy_from_slice(transmute_to_u8(&self.header));
    }

    /// Write the slot to the memory map
    fn write_slot(&mut self, slot_id: SlotId, slot_header: SlotHeader) {
        let (slot_start, slot_end) = self.offsets_for_slot(slot_id);
        self.mmap[slot_start..slot_end].copy_from_slice(transmute_to_u8(&slot_header));
    }

    /// Create a new page at the given path
    pub fn new(path: &Path, required_size: usize) -> SlottedPageMmap {
        let header = SlottedPageHeader::new(required_size);
        let page_size = header.page_size();
        create_and_ensure_length(path, page_size).unwrap();
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let path = path.to_path_buf();
        let mut slotted_mmap = SlottedPageMmap { path, header, mmap };
        slotted_mmap.write_page_header();
        slotted_mmap
    }

    /// Open an existing page at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Option<SlottedPageMmap> {
        if !path.exists() {
            return None;
        }
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header: &SlottedPageHeader =
            transmute_from_u8(&mmap[0..size_of::<SlottedPageHeader>()]);
        let header = header.clone();
        let path = path.to_path_buf();
        Some(SlottedPageMmap { path, header, mmap })
    }

    /// Get value associated with the slot id.
    /// Filters out:
    /// - deleted values
    /// - placeholder values
    pub fn get_value(&self, slot_id: &u32) -> Option<&[u8]> {
        let slot = self.get_slot(slot_id)?;
        self.get_slot_value(&slot)
    }

    /// Get the slot associated with the slot id.
    fn get_slot(&self, slot_id: &u32) -> Option<SlotHeader> {
        self.get_slot_ref(slot_id).cloned()
    }

    fn get_slot_ref(&self, slot_id: &SlotId) -> Option<&SlotHeader> {
        let slot_count = self.header.slot_count;
        if *slot_id >= slot_count as u32 {
            return None;
        }

        let slot_offset =
            size_of::<SlottedPageHeader>() + *slot_id as usize * size_of::<SlotHeader>();
        let start = slot_offset;
        let end = start + size_of::<SlotHeader>();
        let slot: &SlotHeader = transmute_from_u8(&self.mmap[start..end]);

        Some(slot)
    }

    /// Get value associated with the slot
    fn get_slot_value(&self, slot: &SlotHeader) -> Option<&[u8]> {
        let start = slot.offset;
        // adjust the end to account for the right padding
        let end = start
            .checked_add(slot.length)
            .expect("start + length should not overflow")
            - slot.right_padding as u64;

        let value = &self.mmap[start as usize..end as usize];
        if value == SlottedPageMmap::PLACEHOLDER_VALUE {
            None
        } else {
            Some(value)
        }
    }

    /// Check if there is enough space for a new slot + min value
    #[cfg(test)]
    fn has_capacity_for_min_value(&self) -> bool {
        self.free_space() >= size_of::<SlotHeader>() + SlottedPageMmap::MIN_CELL_SIZE_BYTES
    }

    /// Check if there is enough space for a new slot + cell
    fn has_capacity_for_cell(&self, cell_size: usize) -> bool {
        self.free_space() >= size_of::<SlotHeader>() + cell_size
    }

    /// Return the amount of free space in the page
    pub fn free_space(&self) -> usize {
        let slot_count = self.header.slot_count as usize;
        let last_slot_offset =
            size_of::<SlottedPageHeader>() + slot_count * size_of::<SlotHeader>();
        let data_start_offset = self.header.data_start_offset as usize;
        data_start_offset
            .checked_sub(last_slot_offset)
            .expect("this should never overflow, otherwise the page is corrupted")
    }

    pub fn size(&self) -> usize {
        self.header.page_size()
    }

    /// Return the stored amount of fragmentation in the page
    pub fn fragmented_space(&self) -> usize {
        self.header.fragmented_bytes as usize
    }

    /// Sums the amount of unused space in between the data.
    #[cfg(test)]
    pub fn calculate_fragmented_space(&self) -> usize {
        let mut fragmented_space = 0;

        let mut slot_id = 0;
        while let Some(slot) = self.get_slot_ref(&slot_id) {
            // if the slot is deleted, we can consider it empty space
            if slot.deleted {
                fragmented_space += slot.length;
            } else {
                // check if the padding would fit other values.
                let value_size = slot.length.saturating_sub(slot.right_padding as u64);
                let ideal_cell_size = SlottedPageMmap::cell_size_for_value(value_size as usize);
                fragmented_space += slot.length.saturating_sub(ideal_cell_size as u64);
            }

            // update for next iteration
            slot_id += 1;
        }

        fragmented_space as usize
    }

    /// Compute the start and end offsets for the slot
    fn offsets_for_slot(&self, slot_id: SlotId) -> (usize, usize) {
        let slot_offset =
            size_of::<SlottedPageHeader>() + slot_id as usize * size_of::<SlotHeader>();
        let start = slot_offset;
        let end = start + size_of::<SlotHeader>();
        (start, end)
    }

    /// Insert a new placeholder into the page
    pub fn insert_placeholder_value(&mut self, point_id: u32) -> Option<SlotId> {
        self.insert_value(point_id, &SlottedPageMmap::PLACEHOLDER_VALUE)
    }

    /// Insert a new value into the page
    ///
    /// Returns
    /// - None if there is not enough space for a new slot + value
    /// - Some(slot_id) if the value was successfully added
    pub fn insert_value(&mut self, point_offset: PointOffset, value: &[u8]) -> Option<SlotId> {
        // size of the value in bytes
        let value_size = value.len();

        // The size of the data cell containing the value
        let cell_size = Self::cell_size_for_value(value_size);

        // check if there is enough space for the new cell
        if !self.has_capacity_for_cell(cell_size) {
            return None;
        }

        // padding to align the value to the start of the cell
        let padding = cell_size.saturating_sub(value_size);

        // data grows from the end of the page
        let new_data_start_offset = self.header.data_start_offset as usize - cell_size;

        // add slot
        let slot_count = self.header.slot_count;
        let next_slot_id = slot_count as SlotId;
        let slot = SlotHeader::new(
            point_offset,
            new_data_start_offset as u64,
            cell_size as u64,
            padding as u16,
            false,
        );
        self.write_slot(next_slot_id, slot);

        // set value region
        let value_end = new_data_start_offset + value_size;
        self.mmap[new_data_start_offset..value_end].copy_from_slice(value);

        // set right padding to align with the minimum cell size
        if padding > 0 {
            self.mmap[value_end..value_end + padding].copy_from_slice(&vec![0; padding]);
        }

        // update header
        self.header.data_start_offset = new_data_start_offset as u64;
        self.header.slot_count += 1;
        self.write_page_header();
        Some(next_slot_id)
    }

    /// Mark a slot as deleted.
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        let slot_count = self.header.slot_count;
        if slot_id as u64 >= slot_count {
            return None;
        }

        // mark slot as deleted
        let (slot_start, slot_end) = self.offsets_for_slot(slot_id);
        let current_slot = self.get_slot(&slot_id).expect("Slot should exist");
        let updated_slot = SlotHeader {
            deleted: true,
            ..current_slot
        };
        self.mmap[slot_start..slot_end].copy_from_slice(transmute_to_u8(&updated_slot));

        // update fragmentation
        self.header.fragmented_bytes += updated_slot.length;
        self.write_page_header();

        Some(())
    }

    /// Update the value associated with the slot.
    /// The new value must have a size equal or less than the current value.
    ///
    /// Returns
    /// - false if the slot_id is out of bounds or the new value is larger than the current value (caller needs a new page)
    /// - true if the value was successfully updated
    pub fn update_value(&mut self, slot_id: SlotId, new_value: &[u8]) -> bool {
        let slot_count = self.header.slot_count;
        if slot_id as u64 >= slot_count {
            return false;
        }

        let Some(slot) = self.get_slot(&slot_id) else {
            return false;
        };

        // check if there is enough space for the new value
        let value_size = new_value.len();
        if value_size > slot.length as usize {
            return false;
        }

        // update value region
        let value_start = slot.offset as usize;
        let value_end = value_start + value_size;
        self.mmap[value_start..value_end].copy_from_slice(new_value);

        // update padded region
        let cell_size = slot.length as usize;
        let padding = cell_size
            .checked_sub(value_size)
            .expect("value_size should fit in cell_size");
        let padding_start = value_end;
        let padding_end = padding_start + padding;
        if padding > 0 {
            self.mmap[padding_start..padding_end].copy_from_slice(&vec![0; padding]);
        }

        // update slot
        let update_slot = SlotHeader::new(
            slot.point_offset,
            slot.offset as u64, // same offset value
            slot.length,        // same cell size
            padding as u16,     // new padding
            false,              // mark as non deleted
        );
        self.write_slot(slot_id, update_slot);

        // update fragmentation
        // When the new value is smaller than the previous one, it will create unused space in the data region.
        let ideal_cell_size = Self::cell_size_for_value(value_size);
        let unused_space = slot.length.saturating_sub(ideal_cell_size as u64);
        if unused_space > 0 {
            self.header.fragmented_bytes += unused_space;
            self.write_page_header();
        }

        true
    }

    /// Delete the page from the filesystem.
    pub fn delete_page(self) {
        drop(self.mmap);
        std::fs::remove_file(&self.path).unwrap();
    }
}

// tests
#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use serde::{Deserialize, Serialize};
    use tempfile::Builder;

    // Smaller page size for testing
    const TEST_PAGE_SIZE: usize = 2 * 1024 * 1024; // 2MB

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    struct Foo {
        bar: u64,
        qux: bool,
    }

    impl Foo {
        pub fn to_bytes(&self) -> Vec<u8> {
            let mut vec = Vec::new();
            ciborium::ser::into_writer(self, &mut vec).unwrap();
            vec
        }

        pub fn from_bytes(data: &[u8]) -> Self {
            ciborium::de::from_reader(data).unwrap()
        }
    }

    #[test]
    fn test_empty_slotted_page() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();

        let path = file.path();

        let mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);
        // contains only the header
        assert_eq!(
            mmap.free_space(),
            TEST_PAGE_SIZE - size_of::<SlottedPageHeader>()
        );
        assert_eq!(mmap.header.slot_count, 0);
        assert!(mmap.get_slot(&0).is_none());
        drop(mmap);

        // reopen
        let mmap = SlottedPageMmap::open(path).unwrap();
        assert_eq!(
            mmap.free_space(),
            TEST_PAGE_SIZE - size_of::<SlottedPageHeader>()
        );
        assert_eq!(mmap.header.slot_count, 0);
        assert!(mmap.get_slot(&0).is_none());
    }

    #[test]
    fn test_page_full_placeholder() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        let mut mmap = SlottedPageMmap::open(path).unwrap();

        let mut free_space = mmap.free_space();
        let mut sequence = 0u32..;
        // add placeholder values
        while mmap.has_capacity_for_min_value() {
            mmap.insert_placeholder_value(sequence.next().unwrap())
                .unwrap();
            let new_free_space = mmap.free_space();
            assert!(new_free_space < free_space);
            free_space = new_free_space;
        }

        let expected_slot_count = 13_796;
        assert_eq!(mmap.header.slot_count, expected_slot_count);
        assert_eq!(mmap.free_space(), 128); // not enough space for a new slot + placeholder value

        // can't add more values
        assert_eq!(
            mmap.insert_placeholder_value(sequence.next().unwrap()),
            None
        );

        // drop and reopen
        drop(mmap);
        let mmap = SlottedPageMmap::open(path).unwrap();
        assert_eq!(mmap.header.slot_count, expected_slot_count);
        assert_eq!(mmap.header.data_start_offset, 331_264);

        assert_eq!(mmap.all_values().len(), expected_slot_count as usize);
        assert_eq!(mmap.non_deleted_values().len(), 0);
    }

    #[test]
    fn test_read_placeholders() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // add 10 placeholder values
        for i in 0..10 {
            mmap.insert_placeholder_value(i).unwrap();
        }

        assert_eq!(mmap.header.slot_count, 10);
        assert_eq!(mmap.free_space(), 2_095_600);

        // read slots
        let slot = mmap.get_slot(&0).unwrap();
        assert_eq!(slot.offset, 2_097_024);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.get_slot_value(&slot), None);

        let slot = mmap.get_slot(&1).unwrap();
        assert_eq!(slot.offset, 2_096_896);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.get_slot_value(&slot), None);

        let slot = mmap.get_slot(&2).unwrap();
        assert_eq!(slot.offset, 2_096_768);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.get_slot_value(&slot), None);

        // query non-existing slot
        assert_eq!(mmap.get_slot(&10), None);
    }

    #[test]
    fn test_read_non_placeholders() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // add 100 placeholder values
        for i in 0..100 {
            let foo = Foo {
                bar: i,
                qux: i % 2 == 0,
            };
            mmap.insert_value(i as u32, foo.to_bytes().as_slice())
                .unwrap();
        }

        assert_eq!(mmap.header.slot_count, 100);
        assert_eq!(mmap.free_space(), 2_081_920);

        // read slots & values
        let slot = mmap.get_slot(&0).unwrap();
        assert_eq!(slot.offset, 2_097_024);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 0, qux: true };
        let actual = Foo::from_bytes(mmap.get_slot_value(&slot).unwrap());
        assert_eq!(actual, expected);

        let slot = mmap.get_slot(&1).unwrap();
        assert_eq!(slot.offset, 2_096_896);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 1, qux: false };
        let actual = Foo::from_bytes(mmap.get_slot_value(&slot).unwrap());
        assert_eq!(actual, expected);

        let slot = mmap.get_slot(&2).unwrap();
        assert_eq!(slot.offset, 2_096_768);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 2, qux: true };
        let actual = Foo::from_bytes(mmap.get_slot_value(&slot).unwrap());
        assert_eq!(actual, expected);

        // query non-existing slot
        assert_eq!(mmap.get_slot(&100), None);
    }

    #[test]
    fn test_delete_slot() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);

        // add 100 placeholder values
        for i in 0..100 {
            let foo = Foo {
                bar: i,
                qux: i % 2 == 0,
            };
            mmap.insert_value(i as u32, foo.to_bytes().as_slice())
                .unwrap();
        }

        // delete slot 10
        assert!(!mmap.get_slot(&10).unwrap().deleted);
        mmap.delete_value(10).unwrap();
        assert!(mmap.get_slot(&10).unwrap().deleted);

        assert_eq!(mmap.all_values().len(), 100);
        assert_eq!(mmap.non_deleted_values().len(), 99)
    }

    #[test]
    fn test_update() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // push one value
        let foo = Foo { bar: 1, qux: true };
        mmap.insert_value(0, foo.to_bytes().as_slice()).unwrap();

        // read slots & values
        let slot = mmap.get_slot(&0).unwrap();
        assert_eq!(slot.offset, 2_097_024);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 1, qux: true };
        let actual = Foo::from_bytes(mmap.get_slot_value(&slot).unwrap());
        assert_eq!(actual, expected);

        // update value
        let new_foo = Foo { bar: 2, qux: false };
        let updated = mmap.update_value(0, new_foo.to_bytes().as_slice());
        assert!(updated);

        // read slots & values
        let slot = mmap.get_slot(&0).unwrap();
        let actual = Foo::from_bytes(mmap.get_slot_value(&slot).unwrap());
        assert_eq!(actual, new_foo);
    }

    #[test]
    fn test_update_smaller_from_placeholder() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // push placeholder value
        mmap.insert_placeholder_value(0).unwrap();
        let values = mmap.all_values();
        assert_eq!(values.len(), 1);
        assert_eq!(mmap.get_value(&0), None);

        let slot = mmap.get_slot(&0).unwrap();
        assert_eq!(slot.offset, 2_097_024);
        assert_eq!(slot.length, 128);

        // update value from placeholder
        let foo = Foo { bar: 1, qux: true };
        let updated = mmap.update_value(0, foo.to_bytes().as_slice());
        assert!(updated);

        // read slots & values
        let slot = mmap.get_slot(&0).unwrap();
        assert_eq!(slot.offset, 2_097_024);
        assert_eq!(slot.length, 128);
        let actual = Foo::from_bytes(mmap.get_slot_value(&slot).unwrap());
        assert_eq!(actual, foo);
    }

    #[test]
    fn test_update_larger_from_placeholder() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // push placeholder value
        mmap.insert_placeholder_value(0).unwrap();
        let values = mmap.all_values();
        assert_eq!(values.len(), 1);
        assert_eq!(mmap.get_value(&0), None);

        let slot = mmap.get_slot(&0).unwrap();
        assert_eq!(slot.offset, 2097024);
        assert_eq!(slot.length, 128);

        // create random slice larger than the placeholder value
        let mut rng = rand::thread_rng();
        let large_value: Vec<u8> = (0..SlottedPageMmap::MIN_CELL_SIZE_BYTES + 42)
            .map(|_| rng.gen())
            .collect();

        // update value from placeholder
        assert!(large_value.len() > SlottedPageMmap::MIN_CELL_SIZE_BYTES);

        // None because the new value is larger than the current value
        // The caller must delete and create a new value
        assert!(!mmap.update_value(0, large_value.as_slice()));
    }

    #[test]
    fn test_fragmentation_calculation() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, TEST_PAGE_SIZE);

        let big_value = [1; 200];
        for i in 0..500 {
            mmap.insert_value(i, &big_value);
        }

        let mut fragmented_space = mmap.fragmented_space();

        assert_eq!(fragmented_space, 0);
        assert_eq!(fragmented_space, mmap.calculate_fragmented_space());

        // delete some values
        for i in 0..500 {
            if i % 2 == 0 {
                mmap.delete_value(i);
            }
        }

        fragmented_space = mmap.fragmented_space();

        // 250 values are deleted, so 250 * cell_size bytes are fragmented
        let big_cell_size = SlottedPageMmap::cell_size_for_value(big_value.len());
        assert_eq!(fragmented_space, 250 * big_cell_size);
        assert_eq!(fragmented_space, mmap.calculate_fragmented_space());

        // update some values
        let min_value = [1; SlottedPageMmap::MIN_CELL_SIZE_BYTES];
        for i in 0..500 {
            if i % 2 == 1 {
                mmap.update_value(i, &min_value);
            }
        }

        fragmented_space = mmap.fragmented_space();

        // 250 values are updated, so 250 * (big_cell_size - MIN_VALUE_SIZE_BYTES) bytes are fragmented.
        // Plus the ones that were deleted before.
        let expected_fragmentation =
            250 * (big_cell_size - SlottedPageMmap::MIN_CELL_SIZE_BYTES) + 250 * big_cell_size;
        assert_eq!(fragmented_space, expected_fragmentation);
        assert_eq!(fragmented_space, mmap.calculate_fragmented_space());
    }

    #[test]
    fn test_page_size_for_value() {
        let value_size = 128;
        let page_size = SlottedPageMmap::new_page_size_for_value(value_size);
        assert_eq!(
            page_size,
            128 + size_of::<SlottedPageHeader>() + size_of::<SlotHeader>()
        );
    }
}
