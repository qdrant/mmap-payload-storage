use std::ops::Range;
use std::path::PathBuf;

use crate::payload_storage::BLOCK_SIZE_BYTES;
use crate::tracker::{BlockOffset, PageId};
use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{create_and_ensure_length, open_write_mmap};
use crate::utils_copied::mmap_type::MmapBitSlice;

const BITMASK_NAME: &str = "bitmask.dat";

#[derive(Debug)]
pub struct Bitmask {
    bitslice: MmapBitSlice,
    path: PathBuf,
}

impl Bitmask {
    /// Calculate the amount of bytes needed for covering the blocks of a page.
    fn length_for_page(page_size: usize) -> usize {
        assert!(
            page_size % BLOCK_SIZE_BYTES == 0,
            "Page size must be a multiple of block size"
        );

        // one bit per block
        let bits = page_size / BLOCK_SIZE_BYTES;

        // length in bytes
        let length = bits / 8;
        length
    }

    /// Create a bitmask for one page
    pub fn with_capacity(dir: PathBuf, page_size: usize) -> Self {
        let length = Self::length_for_page(page_size);

        let path = Self::bitmask_path(dir);
        create_and_ensure_length(&path, length).unwrap();
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        assert_eq!(mmap_bitslice.len(), length * 8, "Bitmask length mismatch");

        Self {
            bitslice: mmap_bitslice,
            path,
        }
    }

    pub fn open(dir: PathBuf) -> Self {
        let path = Self::bitmask_path(dir);
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);
        Self {
            bitslice: mmap_bitslice,
            path,
        }
    }

    fn bitmask_path(dir: PathBuf) -> PathBuf {
        dir.join(BITMASK_NAME)
    }

    pub fn infer_max_page_id(&self, page_size: usize) -> usize {
        let length = self.bitslice.len();
        let bits = length * 8;
        let covered_bytes = bits * BLOCK_SIZE_BYTES;
        covered_bytes / page_size
    }

    /// Extend the bitslice to cover another page
    pub fn cover_new_page(&mut self, page_size: usize) {
        let extra_length = Self::length_for_page(page_size);

        // flush outstanding changes
        self.bitslice.flusher()().unwrap();

        // reopen the file with a larger size
        let new_length = (self.bitslice.len() / 8) + extra_length;
        create_and_ensure_length(&self.path, new_length).unwrap();
        let mmap = open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal), false).unwrap();

        self.bitslice = MmapBitSlice::from(mmap, 0);
    }

    fn range_of_page(page_id: PageId, page_size: usize) -> Range<usize> {
        debug_assert!(
            page_size % BLOCK_SIZE_BYTES == 0,
            "Page size must be a multiple of block size"
        );
        let page_blocks = page_size / BLOCK_SIZE_BYTES;
        let start = page_id as usize * page_blocks;
        let end = start + page_blocks;
        start..end
    }

    /// The amount of blocks that have never been used in the page.
    pub(crate) fn free_blocks_for_page(&self, page_id: PageId, page_size: usize) -> usize {
        let range_of_page = Self::range_of_page(page_id, page_size);
        self.bitslice[range_of_page].trailing_zeros()
    }

    /// The amount of blocks that are available for reuse in the page.
    pub(crate) fn fragmented_blocks_for_page(&self, page_id: PageId, page_size: usize) -> usize {
        let range_of_page = Self::range_of_page(page_id, page_size);
        let bitslice = &self.bitslice[range_of_page];

        bitslice.count_zeros() - bitslice.trailing_zeros()
    }

    pub(crate) fn find_available_blocks(
        &self,
        num_blocks: u32,
        page_size: usize,
    ) -> Option<(PageId, BlockOffset)> {
        let mut block_cursor = 0;
        while (block_cursor + num_blocks as usize) < self.bitslice.len() {
            let bitslice = &self.bitslice[block_cursor..block_cursor + num_blocks as usize];
            if let Some(offset) = bitslice.last_one() {
                // skip the whole part which has ones in the middle
                block_cursor += offset + 1;
            } else {
                // bingo - we found a free cell of num_blocks
                let page_id = block_cursor.div_euclid(page_size);
                let block_offset = block_cursor.rem_euclid(page_size);
                return Some((page_id as PageId, block_offset as BlockOffset));
            }
        }
        None
    }

    pub(crate) fn mark_blocks(
        &mut self,
        page_id: PageId,
        block_offset: BlockOffset,
        num_blocks: u32,
        used: bool,
    ) {
        let page_start = Self::range_of_page(page_id, BLOCK_SIZE_BYTES).start;

        let offset = page_start + block_offset as usize;
        let blocks_range = offset..offset + num_blocks as usize;

        self.bitslice[blocks_range].fill(used);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_length_for_page() {
        assert_eq!(super::Bitmask::length_for_page(8192), 8);
    }

    #[test]
    fn test_find_available_blocks() {
        let page_size = 8192;
        let dir = tempfile::tempdir().unwrap();
        let mut bitmask = super::Bitmask::with_capacity(dir.path().to_path_buf(), page_size);
        // 1..=10
        bitmask.mark_blocks(0, 1, 9, true);

        // 15..=20
        bitmask.mark_blocks(0, 15, 5, true);

        let (page_id, block_offset) = bitmask.find_available_blocks(1, page_size).unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(block_offset, 0);

        let (page_id, block_offset) = bitmask.find_available_blocks(2, page_size).unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(block_offset, 10);

        let (page_id, block_offset) = bitmask.find_available_blocks(6, page_size).unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(block_offset, 20);

        let found_large = bitmask.find_available_blocks(100, page_size);
        assert_eq!(found_large, None);
    }

    // TODO: proptest!!! (for find_available blocks)
}
