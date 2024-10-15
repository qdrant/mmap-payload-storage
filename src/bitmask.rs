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
    mmap: MmapBitSlice,
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
    pub fn new(dir: PathBuf, page_size: usize) -> Self {
        let length = Self::length_for_page(page_size);
        let path = Self::bitmask_path(dir);
        create_and_ensure_length(&path, length).unwrap();
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);
        Self {
            mmap: mmap_bitslice,
            path,
        }
    }

    pub fn open(dir: PathBuf) -> Self {
        let path = Self::bitmask_path(dir);
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);
        Self {
            mmap: mmap_bitslice,
            path,
        }
    }

    fn bitmask_path(dir: PathBuf) -> PathBuf {
        dir.join(BITMASK_NAME)
    }

    pub fn infer_max_page_id(&self, page_size: usize) -> usize {
        let length = self.mmap.len();
        let bits = length * 8;
        let covered_bytes = bits * BLOCK_SIZE_BYTES;
        covered_bytes / page_size
    }

    /// Extend the bitslice to cover another page
    pub fn cover_new_page(&mut self, page_size: usize) {
        let extra_length = Self::length_for_page(page_size);

        // reopen the file with a larger size
        let new_length = self.mmap.len() + extra_length;
        create_and_ensure_length(&self.path, new_length).unwrap();
        let mmap = open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal), false).unwrap();

        self.mmap = MmapBitSlice::from(mmap, 0);
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
        self.mmap[range_of_page].trailing_zeros()
    }

    /// The amount of blocks that are available for reuse in the page.
    pub(crate) fn fragmented_blocks_for_page(&self, page_id: PageId, page_size: usize) -> usize {
        let range_of_page = Self::range_of_page(page_id, page_size);
        let bitslice = &self.mmap[range_of_page];

        bitslice.count_zeros() - bitslice.trailing_zeros()
    }

    pub(crate) fn find_available_blocks(&self, num_blocks: u32) -> Option<(PageId, BlockOffset)> {
        todo!();
    }

    pub(crate) fn mark_blocks(
        &mut self,
        page_id: PageId,
        block_offset: BlockOffset,
        num_blocks: u32,
        used: bool,
    ) {
        let page_start = Self::range_of_page(page_id, BLOCK_SIZE_BYTES).start;

        let blocks_range = page_start + block_offset as usize..num_blocks as usize;

        self.mmap[blocks_range].fill(used);
    }
}
