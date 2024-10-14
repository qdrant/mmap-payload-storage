use std::path::{Path, PathBuf};

use memmap2::MmapMut;

use crate::utils_copied::{madvise::{Advice, AdviceSetting}, mmap_ops::{create_and_ensure_length, open_write_mmap}};

/// Offset within a page, in bytes.
pub type PageOffset = u32;

/// Length of a payload, in bytes.
pub type DataLength = u32;

/// Headerless page which only contains data.
#[derive(Debug)]
pub(crate) struct PageMmap {
    path: PathBuf,
    mmap: MmapMut,
}

impl PageMmap {
    /// Fixed size for pages. 32MB.
    const PAGE_SIZE: usize = 32 * 1024 * 1024;
    
    /// Create a new page at the given path
    pub fn new(path: &Path) -> Self {
        create_and_ensure_length(path, Self::PAGE_SIZE).unwrap();
        let mmap = open_write_mmap(path, AdviceSetting::from(Advice::Normal)).unwrap();
        let path = path.to_path_buf();
        Self { path, mmap }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn get_value(&self, offset: PageOffset, length: DataLength) -> &[u8] {
        let start = offset as usize;
        let end = (offset + length) as usize;
        &self.mmap[start..end]
    }
    
    pub fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }
}
