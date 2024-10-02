use crate::PayloadStorage;

pub struct StorageStats {
    /// The number of pages in the storage
    pub pages_count: usize,

    /// The default size of pages in bytes
    pub default_page_bytes: usize,

    /// The number of bytes unused in between values
    pub fragmented_bytes: usize,

    /// The number of bytes available in all pages.
    pub available_bytes: usize,

    /// The total size of the storage in bytes
    pub total_size_bytes: usize,
}

impl PayloadStorage {
    /// Storage-wide statistics
    pub fn get_stats(&self) -> StorageStats {
        let pages_count = self.pages.len();
        let default_page_bytes = self.new_page_size;
        let available_bytes = self
            .page_emptiness
            .iter()
            .map(|(_, &free_space)| free_space)
            .sum();

        let mut fragmented_bytes = 0;
        let mut total_size_bytes = 0;
        for page in self.pages.values() {
            fragmented_bytes += page.fragmented_space();
            total_size_bytes += page.size();
        }
        StorageStats {
            pages_count,
            default_page_bytes,
            fragmented_bytes,
            total_size_bytes,
            available_bytes,
        }
    }
}
