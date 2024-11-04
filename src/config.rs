use serde::{Deserialize, Serialize};

use crate::bitmask::DEFAULT_REGION_SIZE_BLOCKS;
use crate::value_storage::{DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_PAGE_SIZE_BYTES};

#[derive(Debug, Default)]
pub struct StorageOptions {
    /// Size of a page in bytes. Must be a multiple of (`block_size` * `region_size`).
    ///
    /// Default is 32MB
    pub page_size_bytes: Option<usize>,

    /// Size of a block in bytes
    ///
    /// Default is 128 bytes
    pub block_size_bytes: Option<usize>,

    /// Size of a region in blocks
    ///
    /// Default is 8192 blocks
    pub region_size_blocks: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StorageConfig {
    /// Size of a page in bytes
    ///
    /// Default is 32MB
    pub page_size_bytes: usize,

    /// Size of a block in bytes
    ///
    /// Default is 128 bytes
    pub block_size_bytes: usize,

    /// Size of a region in blocks
    ///
    /// Default is 8192 blocks
    pub region_size_blocks: usize,
}

impl From<StorageOptions> for StorageConfig {
    fn from(options: StorageOptions) -> Self {
        let page_size_bytes = options.page_size_bytes.unwrap_or(DEFAULT_PAGE_SIZE_BYTES);
        let block_size_bytes = options.block_size_bytes.unwrap_or(DEFAULT_BLOCK_SIZE_BYTES);
        let region_size_blocks = options
            .region_size_blocks
            .map(|x| x as usize)
            .unwrap_or(DEFAULT_REGION_SIZE_BLOCKS);
        Self {
            page_size_bytes,
            block_size_bytes,
            region_size_blocks,
        }
    }
}
