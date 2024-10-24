mod gaps;

use std::ops::Range;
use std::path::{Path, PathBuf};

use bitvec::slice::BitSlice;
use gaps::{BitmaskGaps, RegionGaps};
use itertools::Itertools;

use crate::payload_storage::BLOCK_SIZE_BYTES;
use crate::tracker::{BlockOffset, PageId};
use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{create_and_ensure_length, open_write_mmap};
use crate::utils_copied::mmap_type::{self, MmapBitSlice};

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

impl Bitmask {
    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone(), self.regions_gaps.path.clone()]
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
        bits / 8
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
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        assert_eq!(mmap_bitslice.len(), length * 8, "Bitmask length mismatch");

        // create regions gaps mmap
        let num_regions = mmap_bitslice.len() / REGION_SIZE_BLOCKS;
        let region_gaps = vec![RegionGaps::all_free(REGION_SIZE_BLOCKS as u16); num_regions];

        let mmap_region_gaps = BitmaskGaps::create(dir.to_owned(), region_gaps.into_iter());

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
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        let bitmask_gaps = BitmaskGaps::open(dir.to_owned());

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
        let new_length = (previous_bitslice_len / 8) + extra_length;
        create_and_ensure_length(&self.path, new_length).unwrap();
        let mmap = open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal), false).unwrap();

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
    pub(crate) fn free_blocks_for_page(&self, page_id: PageId) -> usize {
        let range_of_page = self.range_of_page(page_id);
        self.bitslice[range_of_page].trailing_zeros()
    }

    /// The amount of blocks that are available for reuse in the page.
    pub(crate) fn fragmented_blocks_for_page(&self, page_id: PageId) -> usize {
        let range_of_page = self.range_of_page(page_id);
        let bitslice = &self.bitslice[range_of_page];

        bitslice.count_zeros() - bitslice.trailing_zeros()
    }

    /// Find a gap in the bitmask that is large enough to fit `num_blocks` blocks.
    /// Returns the region id of the gap.
    /// In case of boundary gaps, returns the region id of the left gap.
    fn find_fitting_gap(&self, num_blocks: u32) -> Option<Range<RegionId>> {
        let regions_needed = num_blocks.div_ceil(REGION_SIZE_BLOCKS as u32) as usize;

        let window_size = regions_needed + 1;

        if self.regions_gaps.len() == 1 {
            if self.regions_gaps.get(0).max as usize >= num_blocks as usize {
                return Some(0..1);
            } else {
                return None;
            }
        }

        self.regions_gaps.as_slice()[..]
            .windows(window_size)
            .enumerate()
            .find_map(|(start_region_id, gaps)| {
                // cover the case of large number of blocks
                if window_size >= 3 {
                    // check that the middle regions are empty
                    for gap in gaps.iter().take(window_size - 1).skip(1) {
                        if gap.max as usize != REGION_SIZE_BLOCKS {
                            return None;
                        }
                    }
                    let trailing = gaps[0].trailing;
                    let leading = gaps[window_size - 1].leading;
                    let merged_gap =
                        (trailing + leading) as usize + (window_size - 2) * REGION_SIZE_BLOCKS;

                    if merged_gap as u32 >= num_blocks {
                        return Some(
                            start_region_id as RegionId
                                ..(start_region_id + window_size) as RegionId,
                        );
                    } else {
                        return None;
                    }
                }

                // windows of 2
                debug_assert!(window_size == 2, "Unexpected window size");
                let left = &gaps[0];
                let right = &gaps[1];

                // check it fits in the left region
                if left.max as u32 >= num_blocks {
                    // if both gaps are large enough, choose the smaller one
                    if right.max as u32 >= num_blocks {
                        if left.max <= right.max {
                            return Some(
                                start_region_id as RegionId..start_region_id as RegionId + 1,
                            );
                        } else {
                            return Some(
                                start_region_id as RegionId + 1..start_region_id as RegionId + 2,
                            );
                        }
                    }
                    return Some(start_region_id as RegionId..start_region_id as RegionId + 1);
                }

                // check it fits in the right region
                if right.max as u32 >= num_blocks {
                    return Some(start_region_id as RegionId + 1..start_region_id as RegionId + 2);
                }

                // Otherwise, check if the gap in between them is large enough
                let in_between = left.trailing + right.leading;

                if in_between as u32 >= num_blocks {
                    Some(start_region_id as RegionId..start_region_id as RegionId + 2)
                } else {
                    None
                }
            })
    }

    pub(crate) fn find_available_blocks(&self, num_blocks: u32) -> Option<(PageId, BlockOffset)> {
        let region_id_range = self.find_fitting_gap(num_blocks)?;
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

    use crate::{bitmask::REGION_SIZE_BLOCKS, payload_storage::BLOCK_SIZE_BYTES};

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
    // TODO: proptest!!! (for find_available blocks)
}
