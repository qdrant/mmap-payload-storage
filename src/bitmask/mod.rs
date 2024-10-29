mod gaps;

use std::ops::Range;
use std::path::{Path, PathBuf};

use bitvec::slice::BitSlice;
use gaps::{BitmaskGaps, RegionGaps};
use itertools::Itertools;

use crate::tracker::{BlockOffset, PageId};
use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{create_and_ensure_length, open_write_mmap};
use crate::utils_copied::mmap_type::{self, MmapBitSlice};
use crate::value_storage::BLOCK_SIZE_BYTES;

const BITMASK_NAME: &str = "bitmask.dat";
pub const REGION_SIZE_BLOCKS: usize = 8_192;

type RegionId = u32;

#[derive(Debug)]
pub struct Bitmask {
    /// The size in bytes of each page for which this bitmask is used.
    page_size: usize,

    /// A summary of every 1KB (8_192 bits) of contiguous zeros in the bitmask, or less if it is the last region.
    regions_gaps: BitmaskGaps,

    /// The actual bitmask. Each bit represents a block. A 1 means the block is used, a 0 means it is free.
    bitslice: MmapBitSlice,

    /// The path to the file containing the bitmask.
    path: PathBuf,
}

/// Access pattern to the bitmask is always random reads by the already calculated page id.
/// We never need to iterate over multiple bitmask file pages in a row, therefore we can use random access.
const DEFAULT_ADVICE: Advice = Advice::Random;

impl Bitmask {
    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone(), self.regions_gaps.path()]
    }

    /// Calculate the amount of trailing free blocks in the bitmask.
    pub fn trailing_free_blocks(&self) -> u32 {
        self.regions_gaps.trailing_free_blocks()
    }

    /// Calculate the amount of bytes needed for covering the blocks of a page.
    fn length_for_page(page_size: usize) -> usize {
        assert_eq!(
            page_size % BLOCK_SIZE_BYTES,
            0,
            "Page size must be a multiple of block size"
        );

        // one bit per block
        let bits = page_size / BLOCK_SIZE_BYTES;

        // length in bytes
        bits / u8::BITS as usize
    }

    /// Create a bitmask for one page
    pub fn create(dir: &Path, page_size: usize) -> Self {
        debug_assert!(
            page_size % BLOCK_SIZE_BYTES * REGION_SIZE_BLOCKS == 0,
            "Page size must be a multiple of block size * region size"
        );

        let length = Self::length_for_page(page_size);

        // create bitmask mmap
        let path = Self::bitmask_path(dir);
        create_and_ensure_length(&path, length).unwrap();
        let mmap = open_write_mmap(&path, AdviceSetting::from(DEFAULT_ADVICE), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        assert_eq!(mmap_bitslice.len(), length * 8, "Bitmask length mismatch");

        // create regions gaps mmap
        let num_regions = mmap_bitslice.len() / REGION_SIZE_BLOCKS;
        let region_gaps = vec![RegionGaps::all_free(REGION_SIZE_BLOCKS as u16); num_regions];

        let mmap_region_gaps = BitmaskGaps::create(dir, region_gaps.into_iter());

        Self {
            page_size,
            regions_gaps: mmap_region_gaps,
            bitslice: mmap_bitslice,
            path,
        }
    }

    pub fn open(dir: &Path, page_size: usize) -> Option<Self> {
        debug_assert!(
            page_size % BLOCK_SIZE_BYTES == 0,
            "Page size must be a multiple of block size"
        );

        let path = Self::bitmask_path(dir);
        if !path.exists() {
            return None;
        }
        let mmap = open_write_mmap(&path, AdviceSetting::from(DEFAULT_ADVICE), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        let bitmask_gaps = BitmaskGaps::open(dir);

        Some(Self {
            regions_gaps: bitmask_gaps,
            page_size,
            bitslice: mmap_bitslice,
            path,
        })
    }

    fn bitmask_path(dir: &Path) -> PathBuf {
        dir.join(BITMASK_NAME)
    }

    pub fn flush(&self) -> Result<(), mmap_type::Error> {
        self.bitslice.flusher()()?;
        self.regions_gaps.flush()?;

        Ok(())
    }

    pub fn infer_num_pages(&self) -> usize {
        let bits = self.bitslice.len();
        let covered_bytes = bits * BLOCK_SIZE_BYTES;
        covered_bytes.div_euclid(self.page_size)
    }

    /// Extend the bitslice to cover another page
    pub fn cover_new_page(&mut self) {
        let extra_length = Self::length_for_page(self.page_size);

        // flush outstanding changes
        self.bitslice.flusher()().unwrap();

        // reopen the file with a larger size
        let previous_bitslice_len = self.bitslice.len();
        let new_length = (previous_bitslice_len / u8::BITS as usize) + extra_length;
        create_and_ensure_length(&self.path, new_length).unwrap();
        let mmap = open_write_mmap(&self.path, AdviceSetting::from(DEFAULT_ADVICE), false).unwrap();

        self.bitslice = MmapBitSlice::from(mmap, 0);

        // extend the region gaps
        let current_total_regions = self.regions_gaps.len();
        let expected_total_full_regions = self.bitslice.len().div_euclid(REGION_SIZE_BLOCKS);
        debug_assert!(
            self.bitslice.len() % REGION_SIZE_BLOCKS == 0,
            "Bitmask length must be a multiple of region size"
        );
        let new_regions = expected_total_full_regions.saturating_sub(current_total_regions);
        let new_gaps = vec![RegionGaps::all_free(REGION_SIZE_BLOCKS as u16); new_regions];
        self.regions_gaps.extend(new_gaps.into_iter());

        // update the previous last region gaps
        self.update_region_gaps(previous_bitslice_len - 1..previous_bitslice_len + 2);

        assert_eq!(
            self.regions_gaps.len() * REGION_SIZE_BLOCKS,
            self.bitslice.len(),
            "Bitmask length mismatch",
        );
    }

    fn range_of_page(&self, page_id: PageId) -> Range<usize> {
        let page_blocks = self.page_size / BLOCK_SIZE_BYTES;
        let start = page_id as usize * page_blocks;
        let end = start + page_blocks;
        start..end
    }

    /// The amount of blocks that have never been used in the page.
    #[cfg(test)]
    pub(crate) fn free_blocks_for_page(&self, page_id: PageId) -> usize {
        let range_of_page = self.range_of_page(page_id);
        self.bitslice[range_of_page].trailing_zeros()
    }

    /// The amount of blocks that are available for reuse in the page.
    #[allow(dead_code)]
    pub(crate) fn fragmented_blocks_for_page(&self, page_id: PageId) -> usize {
        let range_of_page = self.range_of_page(page_id);
        let bitslice = &self.bitslice[range_of_page];

        bitslice.count_zeros() - bitslice.trailing_zeros()
    }

    pub(crate) fn find_available_blocks(&self, num_blocks: u32) -> Option<(PageId, BlockOffset)> {
        let region_id_range = self.regions_gaps.find_fitting_gap(num_blocks)?;
        let regions_start_offset = region_id_range.start as usize * REGION_SIZE_BLOCKS;
        let regions_end_offset = region_id_range.end as usize * REGION_SIZE_BLOCKS;

        let translate_to_answer = |local_index: u32| {
            let page_size_in_blocks = self.page_size / BLOCK_SIZE_BYTES;

            let global_cursor_offset = local_index as usize + regions_start_offset;

            // Calculate the page id and the block offset within the page
            let page_id = global_cursor_offset.div_euclid(page_size_in_blocks);
            let page_block_offset = global_cursor_offset.rem_euclid(page_size_in_blocks);

            (page_id as PageId, page_block_offset as BlockOffset)
        };

        let regions_bitslice = &self.bitslice[regions_start_offset..regions_end_offset];

        Self::find_available_blocks_in_slice(regions_bitslice, num_blocks, translate_to_answer)
    }

    pub fn find_available_blocks_in_slice<F>(
        bitslice: &BitSlice,
        num_blocks: u32,
        translate_local_index: F,
    ) -> Option<(PageId, BlockOffset)>
    where
        F: FnOnce(u32) -> (PageId, BlockOffset),
    {
        // Get raw memory region
        let (head, raw_region, tail) = bitslice
            .domain()
            .region()
            .expect("Regions cover more than one usize");

        // We expect the regions to not use partial usizes
        debug_assert!(head.is_none());
        debug_assert!(tail.is_none());

        let mut current_size: u32 = 0;
        let mut current_start: u32 = 0;
        let mut num_shifts = 0;
        // Iterate over the integers that compose the bitvec. So that we can perform bitwise operations.
        const BITS_IN_CHUNK: u32 = usize::BITS;
        for (chunk_idx, chunk) in raw_region.iter().enumerate() {
            let mut chunk = *chunk;
            while chunk != 0 {
                let num_zeros = chunk.trailing_zeros();
                current_size += num_zeros;
                if current_size >= num_blocks {
                    // bingo - we found a free cell of num_blocks
                    return Some(translate_local_index(current_start));
                }

                // shift by the number of zeros
                chunk >>= num_zeros as usize;
                num_shifts += num_zeros;

                // skip consecutive ones
                let num_ones = chunk.trailing_ones();
                if num_ones < BITS_IN_CHUNK {
                    chunk >>= num_ones;
                } else {
                    // all ones
                    debug_assert!(chunk == !0);
                    chunk = 0;
                }
                num_shifts += num_ones;

                current_size = 0;
                current_start = chunk_idx as u32 * BITS_IN_CHUNK + num_shifts;
            }
            // no more ones in the chunk
            current_size += BITS_IN_CHUNK - num_shifts;
            num_shifts = 0;
        }
        if current_size >= num_blocks {
            // bingo - we found a free cell of num_blocks
            return Some(translate_local_index(current_start));
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
        let page_start = self.range_of_page(page_id).start;

        let offset = page_start + block_offset as usize;
        let blocks_range = offset..offset + num_blocks as usize;

        self.bitslice[blocks_range.clone()].fill(used);

        self.update_region_gaps(blocks_range);
    }

    fn update_region_gaps(&mut self, blocks_range: Range<usize>) {
        let region_start_id = blocks_range.start / REGION_SIZE_BLOCKS;
        let region_end_id = (blocks_range.end - 1) / REGION_SIZE_BLOCKS;

        for region_id in region_start_id..=region_end_id {
            let region_start = region_id * REGION_SIZE_BLOCKS;
            let region_end = region_start + REGION_SIZE_BLOCKS;

            let bitslice = &self.bitslice[region_start..region_end];

            let gaps = Self::calculate_gaps(bitslice);

            *self.regions_gaps.get_mut(region_id) = gaps;
        }
    }

    pub fn calculate_gaps(region: &BitSlice) -> RegionGaps {
        debug_assert_eq!(region.len(), REGION_SIZE_BLOCKS, "Unexpected region size");
        // Get raw memory region
        let (head, raw_region, tail) = region
            .domain()
            .region()
            .expect("Region covers more than one usize");

        // We expect the region to not use partial usizes
        debug_assert!(head.is_none());
        debug_assert!(tail.is_none());

        // Iterate over the integers that compose the bitslice. So that we can perform bitwise operations.
        let mut max = 0;
        let mut current = 0;
        const BITS_IN_CHUNK: u16 = usize::BITS as u16;
        let mut num_shifts = 0;
        for chunk in raw_region {
            let mut chunk = *chunk;
            while chunk != 0 {
                // count consecutive zeros
                let num_zeros = chunk.trailing_zeros() as u16;
                current += num_zeros;
                if current > max {
                    max = current;
                }
                current = 0;

                // shift by the number of zeros
                chunk >>= num_zeros as usize;
                num_shifts += num_zeros;

                // skip consecutive ones
                let num_ones = chunk.trailing_ones() as u16;
                if num_ones < BITS_IN_CHUNK {
                    chunk >>= num_ones;
                } else {
                    // all ones
                    debug_assert!(chunk == !0);
                    chunk = 0;
                }
                num_shifts += num_ones;
            }

            // no more ones in the chunk
            current += BITS_IN_CHUNK - num_shifts;
            num_shifts = 0;
        }

        if current > max {
            max = current;
        }

        let leading;
        let trailing;
        if max == REGION_SIZE_BLOCKS as u16 {
            leading = max;
            trailing = max;
        } else {
            leading = raw_region
                .iter()
                .take_while_inclusive(|chunk| chunk == &&0)
                .map(|chunk| chunk.trailing_zeros())
                .sum::<u32>() as u16;
            trailing = raw_region
                .iter()
                .rev()
                .take_while_inclusive(|chunk| chunk == &&0)
                .map(|chunk| chunk.leading_zeros())
                .sum::<u32>() as u16;
        }

        RegionGaps::new(leading, trailing, max)
    }
}

#[cfg(test)]
mod tests {

    use bitvec::{bits, vec::BitVec};
    use proptest::prelude::*;
    use rand::thread_rng;

    use crate::{bitmask::REGION_SIZE_BLOCKS, value_storage::BLOCK_SIZE_BYTES};

    #[test]
    fn test_length_for_page() {
        assert_eq!(super::Bitmask::length_for_page(8192), 8);
    }

    #[test]
    fn test_find_available_blocks() {
        let page_size = BLOCK_SIZE_BYTES * REGION_SIZE_BLOCKS;

        let blocks_per_page = (page_size / BLOCK_SIZE_BYTES) as u32;

        let dir = tempfile::tempdir().unwrap();
        let mut bitmask = super::Bitmask::create(dir.path(), page_size);
        bitmask.cover_new_page();

        assert_eq!(bitmask.bitslice.len() as u32, blocks_per_page * 2);

        // 1..10
        bitmask.mark_blocks(0, 1, 9, true);

        // 15..20
        bitmask.mark_blocks(0, 15, 5, true);

        // 30..blocks_per_page
        bitmask.mark_blocks(0, 30, blocks_per_page - 30, true);

        // blocks_per_page..blocks_per_page + 1
        bitmask.mark_blocks(1, 0, 1, true);

        let (page_id, block_offset) = bitmask.find_available_blocks(1).unwrap();
        assert_eq!(block_offset, 0);
        assert_eq!(page_id, 0);

        let (page_id, block_offset) = bitmask.find_available_blocks(2).unwrap();
        assert_eq!(block_offset, 10);
        assert_eq!(page_id, 0);

        let (page_id, block_offset) = bitmask.find_available_blocks(5).unwrap();
        assert_eq!(block_offset, 10);
        assert_eq!(page_id, 0);

        let (page_id, block_offset) = bitmask.find_available_blocks(6).unwrap();
        assert_eq!(block_offset, 20);
        assert_eq!(page_id, 0);

        // first free block of the next page
        let (page_id, block_offset) = bitmask.find_available_blocks(30).unwrap();
        assert_eq!(block_offset, 1);
        assert_eq!(page_id, 1);

        // not fitting cell
        let found_large = bitmask.find_available_blocks(blocks_per_page);
        assert_eq!(found_large, None);
    }

    #[test]
    fn test_raw_bitvec() {
        use bitvec::prelude::Lsb0;
        let bits = bits![
            0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1
        ];

        let mut bitvec = BitVec::<usize, Lsb0>::new();
        bitvec.extend_from_bitslice(bits);

        assert_eq!(bitvec.len(), 64);

        let raw = bitvec.as_raw_slice();
        assert_eq!(raw.len() as u32, 64 / usize::BITS);

        assert_eq!(raw[0].trailing_zeros(), 4);
        assert_eq!(raw[0].leading_zeros(), 0);
        assert_eq!((raw[0] >> 1).trailing_zeros(), 3)
    }

    prop_compose! {
        /// Creates a fixture bitvec which has gaps of a specific size
        fn regions_bitvec_with_max_gap(max_gap_size: usize) (len in 0..REGION_SIZE_BLOCKS*4) -> (BitVec, usize) {
            assert!(max_gap_size > 0);
            let len = len.next_multiple_of(REGION_SIZE_BLOCKS);

            let mut bitvec = BitVec::new();
            bitvec.resize(len, true);

            let mut rng = thread_rng();

            let mut i = 0;
            let mut max_gap = 0;
            while i < len {
                let run = rng.gen_range(1..max_gap_size).min(len - i);
                let skip = rng.gen_range(1..max_gap_size);

                for j in 0..run {
                    bitvec.set(i + j, false);
                }

                if run > max_gap {
                    max_gap = run;
                }

                i += run + skip;
            }

            (bitvec, max_gap)
        }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        #[test]
        fn test_find_available_blocks_properties((bitvec, max_gap) in regions_bitvec_with_max_gap(120)) {
            let bitslice = bitvec.as_bitslice();

            // Helper to check if a range is all zeros
            let is_free_range = |start: usize, len: usize| {
                let range = start..(start + len);
                bitslice.get(range)
                    .map(|slice| slice.not_any())
                    .unwrap_or(false)
            };

            // For different requested block sizes
            for req_blocks in 1..=max_gap {
                if let Some((_, block_offset)) = super::Bitmask::find_available_blocks_in_slice(
                    bitslice,
                    req_blocks as u32,
                    |idx| (0, idx),
                ) {
                    // The found position should have enough free blocks
                    prop_assert!(is_free_range(block_offset as usize, req_blocks));
                } else {
                    prop_assert!(false, "Should've found a free range")
                }
            }

            // For a block size that doesn't fit
            let req_blocks = max_gap + 1;
            prop_assert!(super::Bitmask::find_available_blocks_in_slice(
                bitslice,
                req_blocks as u32,
                |idx| (0, idx),
            ).is_none());
        }
    }
}
