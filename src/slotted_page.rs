use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_to_u8,
};
use memmap2::MmapMut;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct SlottedPageHeader {
    slot_count: u64,
    data_start_offset: u64,
}

impl SlottedPageHeader {
    const fn size_in_bytes() -> usize {
        8 + 8
    }

    fn read_from_mmap(mmap: &MmapMut) -> SlottedPageHeader {
        let slot_count = u64::from_le_bytes(mmap[0..8].try_into().unwrap());
        let data_start_offset = u64::from_le_bytes(mmap[8..16].try_into().unwrap());

        SlottedPageHeader {
            slot_count,
            data_start_offset,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Slot {
    offset: u64, // 8 bytes
    length: u64, // 8 bytes
    deleted: bool, // 1 byte
                 // 7 bytes padding for alignment
}

impl Slot {
    const fn size_in_bytes() -> usize {
        24 // 8 + 8 + 1 + 7 padding
    }
}

#[derive(Debug)]
pub struct SlottedPageMmap {
    path: PathBuf,
    header: SlottedPageHeader,
    mmap: MmapMut,
}

// TODOs
// - [ ] update value
// - [ ] delete value
// - [ ] compact page

impl SlottedPageMmap {
    // Slotted page is a page with a fixed size that contains slots and values.
    pub const SLOTTED_PAGE_SIZE_BYTES: usize = 32 * 1024 * 1024; // 32MB

    // Expect JSON values to have roughly 3–5 fields with mostly small values.
    // Therefore, reserve 100 bytes for each value in order to avoid frequent reallocations.
    // For 1M values, this would require 128MB of memory.
    const MIN_VALUE_SIZE_BYTES: usize = 128;

    // Placeholder value for empty slots
    const PLACEHOLDER_VALUE: [u8; SlottedPageMmap::MIN_VALUE_SIZE_BYTES] =
        [0; SlottedPageMmap::MIN_VALUE_SIZE_BYTES];

    // Return all values in the page with deleted values as None
    fn all_values(&self) -> Vec<Option<&[u8]>> {
        let mut values = Vec::new();
        for i in 0..self.header.slot_count {
            let slot = self.read_slot(&(i as u32)).unwrap();
            // skip values associated with deleted slots
            if slot.deleted {
                values.push(None);
                continue;
            }
            match self.read_raw_value(&slot) {
                Some(value) => values.push(Some(value)),
                None => values.push(None),
            }
        }
        // values are stored in reverse order
        values.reverse();
        values
    }

    fn values(&self) -> Vec<&[u8]> {
        let mut values = Vec::new();
        for i in 0..self.header.slot_count {
            let slot = self.read_slot(&(i as u32)).unwrap();
            // skip values associated with deleted slots
            if slot.deleted {
                continue;
            }
            if let Some(value) = self.read_raw_value(&slot) {
                values.push(value)
            }
        }
        // values are stored in reverse order
        values.reverse();
        values
    }

    pub(crate) fn new(path: &Path, page_size: usize) -> SlottedPageMmap {
        create_and_ensure_length(path, page_size).unwrap();
        let mut mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header = SlottedPageHeader {
            slot_count: 0,
            data_start_offset: page_size as u64,
        };
        // save header
        mmap[0..16].copy_from_slice(transmute_to_u8(&header));
        let path = path.to_path_buf();
        SlottedPageMmap { path, header, mmap }
    }

    pub(crate) fn open(path: &Path) -> SlottedPageMmap {
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header = SlottedPageHeader::read_from_mmap(&mmap);
        let path = path.to_path_buf();
        SlottedPageMmap { path, header, mmap }
    }

    // Read raw value associated with the slot id.
    pub fn read_raw(&self, slot_id: &u32) -> Option<&[u8]> {
        let slot = self.read_slot(slot_id)?;
        self.read_raw_value(&slot)
    }

    fn read_slot(&self, slot_id: &u32) -> Option<Slot> {
        let slot_count = self.header.slot_count;
        if *slot_id >= slot_count as u32 {
            return None;
        }

        let slot_offset =
            SlottedPageHeader::size_in_bytes() + *slot_id as usize * Slot::size_in_bytes();
        let start = slot_offset;
        let end = start + Slot::size_in_bytes();
        let slot: &Slot = transmute_from_u8(&self.mmap[start..end]);
        Some(slot.clone())
    }

    // Read value associated with the slot
    fn read_raw_value(&self, slot: &Slot) -> Option<&[u8]> {
        let start = slot.offset as usize;
        let end = start + slot.length as usize;
        let value = &self.mmap[start..end];
        if value == SlottedPageMmap::PLACEHOLDER_VALUE {
            None
        } else {
            Some(value)
        }
    }

    // check if there is enough space for a new slot + placeholder value
    fn has_capacity(&self) -> bool {
        self.free_space()
            .saturating_sub(Slot::size_in_bytes() + SlottedPageMmap::MIN_VALUE_SIZE_BYTES)
            != 0
    }

    pub(crate) fn free_space(&self) -> usize {
        let slot_count = self.header.slot_count as usize;
        if slot_count == 0 {
            // contains only the header
            return self.mmap.len() - SlottedPageHeader::size_in_bytes();
        }
        let last_slot_offset =
            SlottedPageHeader::size_in_bytes() + slot_count * Slot::size_in_bytes();
        let data_start_offset = self.header.data_start_offset as usize;
        data_start_offset.saturating_sub(last_slot_offset)
    }

    fn offsets_for_slot(&self, slot_id: usize) -> (usize, usize) {
        let slot_offset = SlottedPageHeader::size_in_bytes() + slot_id * Slot::size_in_bytes();
        let start = slot_offset;
        let end = start + Slot::size_in_bytes();
        (start, end)
    }

    pub fn push_placeholder(&mut self) -> Option<usize> {
        self.push(None)
    }

    // Push a new value to the page
    //
    // Returns
    // - None if there is not enough space for a new slot + value
    // - Some(slot_id) if the value was successfully added
    pub fn push(&mut self, value: Option<&[u8]>) -> Option<usize> {
        // check if there is enough space for a new slot + placeholder value
        if !self.has_capacity() {
            return None;
        }

        // size of the value in bytes
        let value_size = value
            .map(|v| v.len())
            .unwrap_or(SlottedPageMmap::MIN_VALUE_SIZE_BYTES);

        let value_bytes = value.unwrap_or(&SlottedPageMmap::PLACEHOLDER_VALUE);

        // add slot
        let slot_count = self.header.slot_count;
        let next_slot_id = slot_count as usize;
        let (slot_start, slot_end) = self.offsets_for_slot(next_slot_id);

        // data grows from the end of the page
        let new_data_start_offset = self.header.data_start_offset as usize - value_size;

        let slot = Slot {
            offset: new_data_start_offset as u64,
            length: value_size as u64,
            deleted: false,
        };
        self.mmap[slot_start..slot_end].copy_from_slice(transmute_to_u8(&slot));

        // set value region
        let value_end = new_data_start_offset + value_size;
        self.mmap[new_data_start_offset..value_end].copy_from_slice(value_bytes);

        // update header
        self.header.data_start_offset = new_data_start_offset as u64;
        self.header.slot_count += 1;
        self.mmap[0..16].copy_from_slice(transmute_to_u8(&self.header));
        Some(next_slot_id)
    }

    /// Mark a slot as deleted logically.
    pub fn delete(&mut self, slot_id: usize) -> Option<()> {
        let slot_count = self.header.slot_count;
        if slot_id >= slot_count as usize {
            return None;
        }

        // mark slot as deleted
        let (slot_start, slot_end) = self.offsets_for_slot(slot_id);
        let slot = self.read_slot(&(slot_id as u32))?;
        let slot = Slot {
            offset: slot.offset,
            length: slot.length,
            deleted: true,
        };
        self.mmap[slot_start..slot_end].copy_from_slice(transmute_to_u8(&slot));
        Some(())
    }
}

// tests
#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::Builder;

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
    struct Foo {
        bar: u64,
        qux: bool,
    }

    impl Foo {
        pub fn binary(&self) -> Vec<u8> {
            serde_cbor::to_vec(self).unwrap()
        }

        pub fn from_binary(data: &[u8]) -> Self {
            serde_cbor::from_slice(data).unwrap()
        }
    }

    #[test]
    fn test_header_size() {
        assert_eq!(
            SlottedPageHeader::size_in_bytes(),
            size_of::<SlottedPageHeader>()
        );
    }

    #[test]
    fn test_slot_size() {
        assert_eq!(Slot::size_in_bytes(), size_of::<Slot>());
    }

    #[test]
    fn test_empty_slotted_page() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();

        let path = file.path();

        let mmap = SlottedPageMmap::new(path, SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES);
        // contains only the header
        assert_eq!(
            mmap.free_space(),
            SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES - SlottedPageHeader::size_in_bytes()
        );
        assert_eq!(mmap.header.slot_count, 0);
        assert!(mmap.read_slot(&0).is_none());
        drop(mmap);

        // reopen
        let mmap = SlottedPageMmap::open(path);
        assert_eq!(
            mmap.free_space(),
            SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES - SlottedPageHeader::size_in_bytes()
        );
        assert_eq!(mmap.header.slot_count, 0);
        assert!(mmap.read_slot(&0).is_none());
    }

    #[test]
    fn test_page_full_placeholder() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mmap = SlottedPageMmap::new(path, SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        let mut mmap = SlottedPageMmap::open(path);

        let mut free_space = mmap.free_space();
        // add placeholder values
        while mmap.has_capacity() {
            mmap.push_placeholder().unwrap();
            let new_free_space = mmap.free_space();
            assert!(new_free_space < free_space);
            free_space = new_free_space;
        }

        let expected_slot_count = 220_752;
        assert_eq!(mmap.header.slot_count, expected_slot_count);
        assert_eq!(mmap.free_space(), 112); // not enough space for a new slot + placeholder value

        // can't add more values
        assert_eq!(mmap.push_placeholder(), None);

        // drop and reopen
        drop(mmap);
        let mmap = SlottedPageMmap::open(path);
        assert_eq!(mmap.header.slot_count, expected_slot_count);
        assert_eq!(mmap.header.data_start_offset, 5_298_176);

        assert_eq!(mmap.all_values().len(), expected_slot_count as usize);
        assert_eq!(mmap.values().len(), 0);
    }

    #[test]
    fn test_read_placeholders() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // add 10 placeholder values
        for _ in 0..10 {
            mmap.push_placeholder().unwrap();
        }

        assert_eq!(mmap.header.slot_count, 10);
        assert_eq!(mmap.free_space(), 33_552_896);

        // read slots
        let slot = mmap.read_slot(&0).unwrap();
        assert_eq!(slot.offset, 33_554_304);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.read_raw_value(&slot), None);

        let slot = mmap.read_slot(&1).unwrap();
        assert_eq!(slot.offset, 33_554_176);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.read_raw_value(&slot), None);

        let slot = mmap.read_slot(&2).unwrap();
        assert_eq!(slot.offset, 33_554_048);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.read_raw_value(&slot), None);

        // query non-existing slot
        assert_eq!(mmap.read_slot(&10), None);
    }

    #[test]
    fn test_read_non_placeholders() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // add 100 placeholder values
        for i in 0..100 {
            let foo = Foo {
                bar: i,
                qux: i % 2 == 0,
            };
            mmap.push(Some(foo.binary().as_slice())).unwrap();
        }

        assert_eq!(mmap.header.slot_count, 100);
        assert_eq!(mmap.free_space(), 33_550_840);

        // read slots & values
        let slot = mmap.read_slot(&0).unwrap();
        assert_eq!(slot.offset, 33_554_421);
        assert_eq!(slot.length, 11);
        let expected = Foo { bar: 0, qux: true };
        let actual = Foo::from_binary(mmap.read_raw_value(&slot).unwrap());
        assert_eq!(actual, expected);

        let slot = mmap.read_slot(&1).unwrap();
        assert_eq!(slot.offset, 33_554_410);
        assert_eq!(slot.length, 11);
        let expected = Foo { bar: 1, qux: false };
        let actual = Foo::from_binary(mmap.read_raw_value(&slot).unwrap());
        assert_eq!(actual, expected);

        let slot = mmap.read_slot(&2).unwrap();
        assert_eq!(slot.offset, 33_554_399);
        assert_eq!(slot.length, 11);
        let expected = Foo { bar: 2, qux: true };
        let actual = Foo::from_binary(mmap.read_raw_value(&slot).unwrap());
        assert_eq!(actual, expected);

        // query non-existing slot
        assert_eq!(mmap.read_slot(&100), None);
    }

    #[test]
    fn test_delete_slot() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES);

        // add 100 placeholder values
        for i in 0..100 {
            let foo = Foo {
                bar: i,
                qux: i % 2 == 0,
            };
            mmap.push(Some(foo.binary().as_slice())).unwrap();
        }

        // delete slot 10
        assert!(!mmap.read_slot(&10).unwrap().deleted);
        mmap.delete(10).unwrap();
        assert!(mmap.read_slot(&10).unwrap().deleted);

        assert_eq!(mmap.all_values().len(), 100);
        assert_eq!(mmap.values().len(), 99)
    }
}
