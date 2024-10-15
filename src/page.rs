use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memmap2::MmapMut;
use std::path::{Path, PathBuf};

pub type SlotId = u32;

#[derive(Debug)]
pub(crate) struct Page {
    path: PathBuf,
    mmap: MmapMut,
}

impl Page {
    /// Expect JSON values to have roughly 3–5 fields with mostly small values.
    /// Therefore, reserve 128 bytes for each value in order to avoid frequent reallocations.
    /// For 1M values, this would require 128MB of memory.
    const BLOCK_SIZE_BYTES: usize = 128;

    /// Cell size required to store a value of the given size.
    fn cell_size_for_value(value_size: usize) -> usize {
        // The value size should be at least the minimum cell size, and always be a multiple of it.
        value_size.next_multiple_of(Self::BLOCK_SIZE_BYTES)
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
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let path = path.to_path_buf();
        Page { path, mmap }
    }

    /// Open an existing page at the given path
    /// If the file does not exist, return None
    pub fn open(path: &Path) -> Option<Page> {
        if !path.exists() {
            return None;
        }
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let path = path.to_path_buf();
        Some(Page { path, mmap })
    }

    /// Write a cell into the page
    ///
    /// # Returns
    /// - None if there is not enough space for the new value
    /// - Some(()) if the value was successfully added
    ///
    /// # Corruption
    ///
    /// If the block_offset and length of the value are already taken, this function will still overwrite the data.
    pub fn write_cell(&mut self, block_offset: u32, cell: &[u8]) -> Option<()> {
        // The size of the data cell containing the value
        let cell_size = cell.len();

        // check the the cell size is a multiple of the block size
        assert_eq!(cell_size % Self::BLOCK_SIZE_BYTES, 0);

        // check that the value fits in the page
        let cell_start = block_offset as usize * Self::BLOCK_SIZE_BYTES;
        if cell_start + cell_size > self.mmap.len() {
            return None;
        }

        // set value region
        let cell_end = cell_start + cell_size;
        self.mmap[cell_start..cell_end].copy_from_slice(cell);

        Some(())
    }

    /// Read a cell from the page
    ///
    /// # Arguments
    /// - block_offset: The offset of the value in blocks
    /// - num_blocks: The number of blocks the value occupies
    ///
    /// # Returns
    /// - None if the value is not within the page
    /// - Some(slice) if the value was successfully read
    pub fn read_cell(&self, block_offset: u32, num_blocks: u32) -> Option<&[u8]> {
        // The size of the data cell containing the value
        let cell_size = num_blocks as usize * Self::BLOCK_SIZE_BYTES;

        // check that the value is within the page
        let cell_start = block_offset as usize * Self::BLOCK_SIZE_BYTES;
        if cell_start + cell_size > self.mmap.len() {
            return None;
        }

        // read value region
        let cell_end = cell_start + cell_size;
        Some(&self.mmap[cell_start..cell_end])
    }

    /// Delete the page from the filesystem.
    pub fn delete_page(self) {
        drop(self.mmap);
        std::fs::remove_file(&self.path).unwrap();
    }
}
