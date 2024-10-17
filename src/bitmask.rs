use std::cmp::Ordering;
use std::ops::Range;
use std::path::PathBuf;

use bitvec::slice::BitSlice;

use crate::payload_storage::BLOCK_SIZE_BYTES;
use crate::tracker::{BlockOffset, PageId};
use crate::utils_copied::madvise::{Advice, AdviceSetting};
use crate::utils_copied::mmap_ops::{create_and_ensure_length, open_write_mmap};
use crate::utils_copied::mmap_type::MmapBitSlice;

const BITMASK_NAME: &str = "bitmask.dat";
pub const REGION_SIZE_BLOCKS: usize = 8_192;

type RegionId = u32;

#[derive(Debug, Clone)]
struct Gaps {
    max: u16,
    leading: u16,
    trailing: u16,
}

impl Gaps {
    fn all_free(blocks: u16) -> Self {
        Self {
            max: blocks,
            leading: blocks,
            trailing: blocks,
        }
    }
}

#[derive(Debug)]
pub struct Bitmask {
    /// The size in bytes of each page for which this bitmask is used.
    page_size: usize,

    /// A summary of every 1KB (8_192 bits) of contiguous zeros in the bitmask, or less if it is the last region.
    region_gaps: Vec<Gaps>,

    /// The actual bitmask. Each bit represents a block. A 1 means the block is used, a 0 means it is free.
    bitslice: MmapBitSlice,

    /// The path to the file containing the bitmask.
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
        debug_assert!(
            page_size % BLOCK_SIZE_BYTES * REGION_SIZE_BLOCKS == 0,
            "Page size must be a multiple of block size"
        );

        let length = Self::length_for_page(page_size);

        let path = Self::bitmask_path(dir);
        create_and_ensure_length(&path, length).unwrap();
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        assert_eq!(mmap_bitslice.len(), length * 8, "Bitmask length mismatch");

        let regions = mmap_bitslice.len().div_euclid(REGION_SIZE_BLOCKS);
        let mut region_gaps = vec![Gaps::all_free(REGION_SIZE_BLOCKS as u16); regions];

        let last_region_blocks = mmap_bitslice.len().rem_euclid(REGION_SIZE_BLOCKS);
        if last_region_blocks > 0 {
            region_gaps.push(Gaps::all_free(last_region_blocks as u16));
        }

        Self {
            page_size,
            region_gaps,
            bitslice: mmap_bitslice,
            path,
        }
    }

    pub fn open(dir: PathBuf, page_size: usize) -> Self {
        debug_assert!(
            page_size % BLOCK_SIZE_BYTES == 0,
            "Page size must be a multiple of block size"
        );

        let path = Self::bitmask_path(dir);
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        // TODO: persist?
        let mut region_gaps = Vec::new();
        for region_blocks in mmap_bitslice.chunks(REGION_SIZE_BLOCKS) {
            let gaps = Self::calculate_gaps(region_blocks);
            region_gaps.push(gaps);
        }

        Self {
            region_gaps,
            page_size,
            bitslice: mmap_bitslice,
            path,
        }
    }

    fn bitmask_path(dir: PathBuf) -> PathBuf {
        dir.join(BITMASK_NAME)
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
        let current_total_regions = self.region_gaps.len();
        let expected_total_full_regions = self.bitslice.len().div_euclid(REGION_SIZE_BLOCKS);
        debug_assert!(
            self.bitslice.len() % REGION_SIZE_BLOCKS == 0,
            "Bitmask length must be a multiple of region size"
        );
        let new_regions = expected_total_full_regions.saturating_sub(current_total_regions);
        let new_gaps = vec![Gaps::all_free(REGION_SIZE_BLOCKS as u16); new_regions];
        self.region_gaps.extend(new_gaps);

        // update the previous last region gaps
        self.update_region_gaps(previous_bitslice_len - 1..previous_bitslice_len + 2);

        assert_eq!(
            self.region_gaps.len() * REGION_SIZE_BLOCKS,
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

        if self.region_gaps.len() == 1 {
            if self.region_gaps[0].max as usize >= num_blocks as usize {
                return Some(0..1);
            } else {
                return None;
            }
        }

        (&self.region_gaps[..])
            .windows(window_size)
            .enumerate()
            .find_map(|(start_region_id, gaps)| {
                // cover the case of large number of blocks
                if window_size >= 3 {
                    // check that the middle regions are empty
                    for i in 1..window_size - 1 {
                        if gaps[i].max as usize != REGION_SIZE_BLOCKS {
                            return None;
                        }
                    }
                    let trailing = gaps[0].trailing;
                    let leading = gaps[window_size - 1].leading;
                    let merged_gap = (trailing + leading) as usize
                        + (window_size - 2) * REGION_SIZE_BLOCKS as usize;

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

        let regions_bitslice = &self.bitslice[regions_start_offset..regions_end_offset];

        let mut cursor = 0;
        while (cursor + num_blocks as usize) <= regions_bitslice.len() {
            let bitslice = &regions_bitslice[cursor..cursor + num_blocks as usize];
            if let Some(offset) = bitslice.last_one() {
                // skip the whole part which has ones in the middle
                cursor += offset + 1;
            } else {
                // bingo - we found a free cell of num_blocks
                let page_size_in_blocks = self.page_size / BLOCK_SIZE_BYTES;

                let global_cursor_offset = cursor + regions_start_offset;

                // Calculate the page id and the block offset within the page
                let page_id = global_cursor_offset.div_euclid(page_size_in_blocks);
                let page_block_offset = global_cursor_offset.rem_euclid(page_size_in_blocks);

                return Some((page_id as PageId, page_block_offset as BlockOffset));
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

            self.region_gaps[region_id] = gaps;
        }
    }

    fn calculate_gaps(region: &BitSlice) -> Gaps {
        debug_assert!(
            region.len() <= REGION_SIZE_BLOCKS as usize,
            "Unexpected region size"
        );
        let mut max = 0;
        let mut current = 0;
        // TODO: capture leading on the same loop?
        for b in region {
            if !b {
                current += 1;
                if current > max {
                    max = current;
                }
            } else {
                current = 0;
            }
        }

        assert!(max <= REGION_SIZE_BLOCKS as u16, "Unexpected max gap size");

        let leading;
        let trailing;
        if max == REGION_SIZE_BLOCKS as u16 {
            leading = max;
            trailing = max;
        } else {
            leading = region.iter().take_while(|b| *b == false).count() as u16;
            trailing = region.iter().rev().take_while(|b| *b == false).count() as u16;
        }

        Gaps {
            max,
            leading,
            trailing,
        }
    }
}

#[cfg(test)]
mod tests {
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
        let mut bitmask = super::Bitmask::with_capacity(dir.path().to_path_buf(), page_size);
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

    // TODO: proptest!!! (for find_available blocks)
}
