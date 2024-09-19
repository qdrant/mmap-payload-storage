use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{
    create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_to_u8,
};
use memmap2::MmapMut;
use std::cmp::Ordering;
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
    offset: u64,      // offset in the page (8 bytes)
    length: u64,      // length of the value (8 bytes)
    left_padding: u8, // padding within the value for small values (1 byte)
    deleted: bool,    // whether the value has been deleted (1 byte)
                      // 6 bytes padding for alignment
}

impl Slot {
    const fn size_in_bytes() -> usize {
        24 // 8 + 8 + 1 + + 1 + 6 padding
    }

    fn new(offset: u64, length: u64, left_padding: u8, deleted: bool) -> Slot {
        assert!(
            length >= SlottedPageMmap::MIN_VALUE_SIZE_BYTES as u64,
            "Value too small"
        );
        Slot {
            offset,
            length,
            left_padding,
            deleted,
        }
    }
}

#[derive(Debug)]
pub struct SlottedPageMmap {
    path: PathBuf,
    header: SlottedPageHeader,
    mmap: MmapMut,
}

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

    //Flushes outstanding memory map modifications to disk.
    fn flush(&self) {
        self.mmap.flush().unwrap();
    }

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
            match self.read_slot_value(&slot) {
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
            if let Some(value) = self.read_slot_value(&slot) {
                values.push(value)
            }
        }
        // values are stored in reverse order
        values.reverse();
        values
    }

    fn persist_page_header(&mut self) {
        self.mmap[0..16].copy_from_slice(transmute_to_u8(&self.header));
    }

    pub fn new(path: &Path, page_size: usize) -> SlottedPageMmap {
        create_and_ensure_length(path, page_size).unwrap();
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header = SlottedPageHeader {
            slot_count: 0,
            data_start_offset: page_size as u64,
        };
        let path = path.to_path_buf();
        let mut slotted_mmap = SlottedPageMmap { path, header, mmap };
        slotted_mmap.persist_page_header();
        slotted_mmap
    }

    pub fn open(path: &Path) -> SlottedPageMmap {
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let header = SlottedPageHeader::read_from_mmap(&mmap);
        let path = path.to_path_buf();
        SlottedPageMmap { path, header, mmap }
    }

    // Read raw value associated with the slot id.
    pub fn read_value(&self, slot_id: &u32) -> Option<&[u8]> {
        let slot = self.read_slot(slot_id)?;
        self.read_slot_value(&slot)
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
    fn read_slot_value(&self, slot: &Slot) -> Option<&[u8]> {
        let start = slot.offset;
        // adjust the end to account for the left padding
        let end = start + slot.length - slot.left_padding as u64;
        let value = &self.mmap[start as usize..end as usize];
        if value == SlottedPageMmap::PLACEHOLDER_VALUE {
            None
        } else {
            Some(value)
        }
    }

    // check if there is enough space for a new slot + min value
    fn has_capacity_for_min_value(&self) -> bool {
        self.free_space()
            .saturating_sub(Slot::size_in_bytes() + SlottedPageMmap::MIN_VALUE_SIZE_BYTES)
            != 0
    }

    fn has_capacity_for_value(&self, extra_len: usize) -> bool {
        self.free_space()
            .saturating_sub(Slot::size_in_bytes() + extra_len)
            != 0
    }

    pub fn free_space(&self) -> usize {
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
        // check if there is enough space the value
        match value {
            Some(v) => {
                if !self.has_capacity_for_value(v.len()) {
                    return None;
                }
            }
            None => {
                if !self.has_capacity_for_min_value() {
                    return None;
                }
            }
        }

        // size of the value in bytes
        let real_value_size = value
            .map(|v| v.len())
            .unwrap_or(SlottedPageMmap::MIN_VALUE_SIZE_BYTES);

        // value bytes
        let real_value_bytes = value.unwrap_or(&SlottedPageMmap::PLACEHOLDER_VALUE);

        // padding to align the value to the end of the page
        let padding = SlottedPageMmap::MIN_VALUE_SIZE_BYTES.saturating_sub(real_value_size);

        // actual value size accounting for the minimum value size
        let value_len = real_value_size + padding;

        // data grows from the end of the page
        let new_data_start_offset = self.header.data_start_offset as usize - value_len;

        // add slot
        let slot_count = self.header.slot_count;
        let next_slot_id = slot_count as usize;
        let (slot_start, slot_end) = self.offsets_for_slot(next_slot_id);
        let slot = Slot::new(
            new_data_start_offset as u64,
            value_len as u64,
            padding as u8,
            false,
        );
        self.mmap[slot_start..slot_end].copy_from_slice(transmute_to_u8(&slot));

        // set value region
        let value_end = new_data_start_offset + real_value_size;
        self.mmap[new_data_start_offset..value_end].copy_from_slice(real_value_bytes);

        // set left padding for values that are smaller than the minimum value size
        if padding > 0 {
            self.mmap[value_end..value_end + padding].copy_from_slice(&vec![0; padding]);
        }

        // update header
        self.header.data_start_offset = new_data_start_offset as u64;
        self.header.slot_count += 1;
        self.persist_page_header();
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
        let current_slot = self.read_slot(&(slot_id as u32))?;
        let updated_slot = Slot {
            deleted: true,
            ..current_slot
        };
        self.mmap[slot_start..slot_end].copy_from_slice(transmute_to_u8(&updated_slot));
        Some(())
    }

    /// Update the value associated with the slot.
    /// The new value must have a size equal or less than the current value.
    ///
    /// Returns
    /// - None if the slot_id is out of bounds or the new value is larger than the current value (caller needs a new page)
    /// - Some(()) if the value was successfully updated
    pub fn update_value(&mut self, slot_id: usize, new_value: &[u8]) -> Option<()> {
        let slot_count = self.header.slot_count;
        if slot_id >= slot_count as usize {
            return None;
        }

        let slot = self.read_slot(&(slot_id as u32))?;

        let real_value_size = new_value.len();

        // check if there is enough space for the new value
        if real_value_size > slot.length as usize {
            return None;
        }

        // update value region
        let value_start = slot.offset as usize;
        let value_end = value_start + real_value_size;
        self.mmap[value_start..value_end].copy_from_slice(new_value);
        let left_padding = SlottedPageMmap::MIN_VALUE_SIZE_BYTES.saturating_sub(real_value_size);
        if left_padding > 0 {
            self.mmap[value_end..value_end + left_padding].copy_from_slice(&vec![0; left_padding]);
        }

        // update slot
        let (slot_start, slot_end) = self.offsets_for_slot(slot_id);

        // actual value size accounting for the minimum value size
        let value_len = real_value_size + left_padding;
        let update_slot = Slot::new(
            value_start as u64, // new offset value
            value_len as u64,   // new value size
            left_padding as u8, // new padding
            true,               // mark as non deleted
        );
        self.mmap[slot_start..slot_end].copy_from_slice(transmute_to_u8(&update_slot));

        Some(())
    }

    pub fn compact(&mut self) {
        // TODO
        // - remove deleted slots and values
        // - shift all values to the end of the page
        // - update slot offsets
        // - update header
        // - the page tracker needs to be updated accordingly!
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
        while mmap.has_capacity_for_min_value() {
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
        assert_eq!(mmap.read_slot_value(&slot), None);

        let slot = mmap.read_slot(&1).unwrap();
        assert_eq!(slot.offset, 33_554_176);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.read_slot_value(&slot), None);

        let slot = mmap.read_slot(&2).unwrap();
        assert_eq!(slot.offset, 33_554_048);
        assert_eq!(slot.length, 128);
        assert_eq!(mmap.read_slot_value(&slot), None);

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
        assert_eq!(mmap.free_space(), 33_539_216);

        // read slots & values
        let slot = mmap.read_slot(&0).unwrap();
        assert_eq!(slot.offset, 33_554_304);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 0, qux: true };
        let actual = Foo::from_binary(mmap.read_slot_value(&slot).unwrap());
        assert_eq!(actual, expected);

        let slot = mmap.read_slot(&1).unwrap();
        assert_eq!(slot.offset, 33_554_176);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 1, qux: false };
        let actual = Foo::from_binary(mmap.read_slot_value(&slot).unwrap());
        assert_eq!(actual, expected);

        let slot = mmap.read_slot(&2).unwrap();
        assert_eq!(slot.offset, 33_554_048);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 2, qux: true };
        let actual = Foo::from_binary(mmap.read_slot_value(&slot).unwrap());
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

    #[test]
    fn test_update() {
        let file = Builder::new()
            .prefix("test-pages")
            .suffix(".data")
            .tempfile()
            .unwrap();
        let path = file.path();

        let mut mmap = SlottedPageMmap::new(path, SlottedPageMmap::SLOTTED_PAGE_SIZE_BYTES);
        let values = mmap.all_values();
        assert_eq!(values.len(), 0);

        // push one value
        let foo = Foo { bar: 1, qux: true };
        mmap.push(Some(foo.binary().as_slice())).unwrap();

        // read slots & values
        let slot = mmap.read_slot(&0).unwrap();
        assert_eq!(slot.offset, 33_554_304);
        assert_eq!(slot.length, 128);
        let expected = Foo { bar: 1, qux: true };
        let actual = Foo::from_binary(mmap.read_slot_value(&slot).unwrap());
        assert_eq!(actual, expected);

        // update value
        let new_foo = Foo { bar: 2, qux: false };
        mmap.update_value(0, new_foo.binary().as_slice()).unwrap();

        // read slots & values
        let slot = mmap.read_slot(&0).unwrap();
        let actual = Foo::from_binary(mmap.read_slot_value(&slot).unwrap());
        assert_eq!(actual, new_foo);
    }
}
