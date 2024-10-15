use crate::payload_storage::BLOCK_SIZE_BYTES;
use crate::tracker::BlockOffset;
use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memmap2::MmapMut;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct Page {
    path: PathBuf,
    mmap: MmapMut,
}

impl Page {
    /// Cell size required to store a value of the given size.
    pub fn cell_size_for_value(value_size: usize) -> usize {
        // The value size should be at least the minimum cell size, and always be a multiple of it.
        value_size.next_multiple_of(BLOCK_SIZE_BYTES)
    }

    /// Minimum free space required for a value of the given size.
    pub fn required_space_for_new_value(value_size: usize) -> usize {
        Self::cell_size_for_value(value_size)
    }

    /// Flushes outstanding memory map modifications to disk.
    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    /// Create a new page at the given path
    pub fn new(path: &Path, size: usize) -> Page {
        create_and_ensure_length(path, size).unwrap();
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let path = path.to_path_buf();
        Page { path, mmap }
    }

    /// Open an existing page at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Option<Page> {
        if !path.exists() {
            return None;
        }
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let path = path.to_path_buf();
        Some(Page { path, mmap })
    }

    /// Write a value into the page
    ///
    /// # Returns
    /// Amount of bytes that didn't fit into the page
    ///
    /// # Corruption
    ///
    /// If the block_offset and length of the value are already taken, this function will still overwrite the data.
    pub fn write_value(&mut self, block_offset: u32, value: &[u8]) -> usize {
        // The size of the data cell containing the value
        let value_size = value.len();

        let value_start = block_offset as usize * BLOCK_SIZE_BYTES;

        let value_end = value_start + value_size;
        // only write what fits in the page
        let unwritten_bytes = value_end.saturating_sub(self.mmap.len());

        // set value region
        self.mmap[value_start..value_end - unwritten_bytes].copy_from_slice(value);

        unwritten_bytes
    }

    /// Read a value from the page
    ///
    /// # Arguments
    /// - block_offset: The offset of the value in blocks
    /// - length: The number of blocks the value occupies
    ///
    /// # Returns
    /// - None if the value is not within the page
    /// - Some(slice) if the value was successfully read
    pub fn read_value(&self, block_offset: BlockOffset, length: u32) -> Option<&[u8]> {
        let value_start = block_offset as usize * BLOCK_SIZE_BYTES;
        // check that the value is within the page
        if value_start + length as usize > self.mmap.len() {
            return None;
        }

        // read value region
        let value_end = value_start + length as usize;
        Some(&self.mmap[value_start..value_end])
    }

    /// Delete the page from the filesystem.
    pub fn delete_page(self) {
        drop(self.mmap);
        std::fs::remove_file(&self.path).unwrap();
    }
}
