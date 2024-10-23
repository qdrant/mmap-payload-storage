use std::path::PathBuf;

use itertools::Itertools;

use crate::utils_copied::{
    madvise::{Advice, AdviceSetting},
    mmap_ops::{create_and_ensure_length, open_write_mmap},
    mmap_type::{self, MmapSlice},
};

use super::REGION_SIZE_BLOCKS;

/// Gaps of contiguous zeros in a bitmask region.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RegionGaps {
    pub max: u16,
    pub leading: u16,
    pub trailing: u16,
}

impl RegionGaps {
    pub fn new(leading: u16, trailing: u16, max: u16) -> Self {
        #[cfg(debug_assertions)]
        {
            let maximum_possible = REGION_SIZE_BLOCKS as u16;

            assert!(max <= maximum_possible, "Unexpected max gap size");

            assert!(
                leading <= max,
                "Invalid gaps: leading is {}, but max is {}",
                leading,
                max
            );

            assert!(
                trailing <= max,
                "Invalid gaps: trailing is {}, but max is {}",
                trailing,
                max
            );

            if leading == maximum_possible || trailing == maximum_possible {
                assert_eq!(leading, trailing);
            }
        }

        Self {
            max,
            leading,
            trailing,
        }
    }

    pub fn all_free(blocks: u16) -> Self {
        Self {
            max: blocks,
            leading: blocks,
            trailing: blocks,
        }
    }

    fn is_empty(&self) -> bool {
        self.leading == REGION_SIZE_BLOCKS as u16
    }
}

/// An overview of contiguous free blocks covered by the bitmask.
#[derive(Debug)]
pub(super) struct BitmaskGaps {
    path: PathBuf,
    mmap_slice: MmapSlice<RegionGaps>,
}

impl BitmaskGaps {
    fn file_path(dir: PathBuf) -> PathBuf {
        dir.join("gaps.dat")
    }

    pub fn create(dir: PathBuf, mut iter: impl ExactSizeIterator<Item = RegionGaps>) -> Self {
        let path = Self::file_path(dir);

        let length_in_bytes = iter.len() * size_of::<RegionGaps>();
        create_and_ensure_length(&path, length_in_bytes).unwrap();

        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), true).unwrap();
        let mut mmap_slice = unsafe { MmapSlice::from(mmap) };

        debug_assert_eq!(mmap_slice.len(), iter.len());

        mmap_slice.fill_with(|| iter.next().unwrap());

        Self { path, mmap_slice }
    }

    pub fn open(dir: PathBuf) -> Self {
        let path = Self::file_path(dir);
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false).unwrap();
        let mmap_slice = unsafe { MmapSlice::from(mmap) };

        Self { path, mmap_slice }
    }

    pub fn flush(&self) -> Result<(), mmap_type::Error> {
        self.mmap_slice.flusher()()
    }

    /// Extends the mmap file to fit the new regions
    pub fn extend(&mut self, mut iter: impl ExactSizeIterator<Item = RegionGaps>) {
        if iter.len() == 0 {
            return;
        }

        // reopen the file with a larger size
        let prev_len = self.mmap_slice.len();
        let new_slice_len = prev_len + iter.len();
        let new_length_in_bytes = new_slice_len * size_of::<RegionGaps>();

        create_and_ensure_length(&self.path, new_length_in_bytes).unwrap();

        let mmap = open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal), false).unwrap();

        self.mmap_slice = unsafe { MmapSlice::from(mmap) };

        debug_assert_eq!(self.mmap_slice[prev_len..].len(), iter.len());

        self.mmap_slice[prev_len..].fill_with(|| iter.next().unwrap())
    }

    pub fn trailing_free_blocks(&self) -> u32 {
        self.mmap_slice
            .iter()
            .rev()
            .take_while_inclusive(|gap| gap.trailing == REGION_SIZE_BLOCKS as u16)
            .map(|gap| gap.trailing as u32)
            .sum()
    }

    pub fn len(&self) -> usize {
        self.mmap_slice.len()
    }

    pub fn get(&self, idx: usize) -> &RegionGaps {
        &self.mmap_slice[idx]
    }

    pub fn get_mut(&mut self, idx: usize) -> &mut RegionGaps {
        &mut self.mmap_slice[idx]
    }

    pub fn as_slice(&self) -> &[RegionGaps] {
        &self.mmap_slice
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_gaps_persistence() {
        use std::fs;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();

        let gaps = vec![
            RegionGaps::new(1, 2, 3),
            RegionGaps::new(4, 5, 6),
            RegionGaps::new(7, 8, 9),
        ];

        // Create RegionGaps and write gaps
        {
            let region_gaps = BitmaskGaps::create(dir_path.clone(), gaps.clone().into_iter());
            assert_eq!(region_gaps.len(), gaps.len());
            for (i, gap) in gaps.iter().enumerate() {
                assert_eq!(region_gaps.get(i), gap);
            }
        }

        // Reopen RegionGaps and verify gaps
        {
            let region_gaps = BitmaskGaps::open(dir_path.clone());
            assert_eq!(region_gaps.len(), gaps.len());
            for (i, gap) in gaps.iter().enumerate() {
                assert_eq!(region_gaps.get(i), gap);
            }
        }

        // Extend RegionGaps with more gaps
        let more_gaps = vec![RegionGaps::new(10, 11, 12), RegionGaps::new(13, 14, 15)];

        {
            let mut region_gaps = BitmaskGaps::open(dir_path.clone());
            region_gaps.extend(more_gaps.clone().into_iter());
            assert_eq!(region_gaps.len(), gaps.len() + more_gaps.len());
            for (i, gap) in gaps.iter().chain(more_gaps.iter()).enumerate() {
                assert_eq!(region_gaps.get(i), gap);
            }
        }

        // Reopen RegionGaps and verify all gaps
        {
            let region_gaps = BitmaskGaps::open(dir_path.clone());
            assert_eq!(region_gaps.len(), gaps.len() + more_gaps.len());
            for (i, gap) in gaps.iter().chain(more_gaps.iter()).enumerate() {
                assert_eq!(region_gaps.get(i), gap);
            }
        }

        // Clean up
        fs::remove_file(BitmaskGaps::file_path(dir_path)).unwrap();
    }
}
