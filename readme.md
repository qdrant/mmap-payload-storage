# mmap-payload-storage

New experimental storage for vector payloads using mmap.

## Design

- The storage is divided into pages of fixed minimum size (32MB), but can be larger if a single payload needs more space.
- Each payload fits within a single page.
- Those pages are mapped into memory using mmap
- Those pages are following the Slotted Page structure
- Slots are fixed size.
- Values have a dynamic size and a min size (128 bytes)
- Values are compressed with lz4
- Deletes mark the slot with a tombstone
- Updates:
  - can be done in place if the new value fits
  - otherwise, the old value is marked as deleted and a new value is inserted
  - if the value is smaller, the container is shrunk down to at least the min size
- Supports multiple threads reading and single thread writing

![Slotted Page](./slotted%20pages.svg)

## TODOs

- [ ] compact on write
- [ ] reuse deleted slots on write
- [ ] non write blocking compaction
- [ ] benchmarking with realistic data payload (CPU + memory usage)
- [ ] run unit tests different page sizes
- [ ] test best fitting page logic
- [ ] test data consistency on panic
- [ ] test new payload storage vs Rockdb equivalent (speed & storage size)
- [ ] dictionary compression to optimize payload key repetition
- [ ] validate the usage with a block storage via HTTP range requests
- [ ] handle all surviving mutants
  - MISSED   src/page_tracker.rs:117:44: replace + with * in PageTracker::persist_pointer in 0.6s build + 29.6s test
  - MISSED   src/slotted_page.rs:352:26: replace > with == in SlottedPageMmap::update_value in 0.5s build + 29.8s test
  - MISSED   src/payload_storage.rs:50:24: replace > with < in PayloadStorage::open in 0.5s build + 29.6s test
  - MISSED   src/payload_storage.rs:107:69: replace + with - in PayloadStorage::find_best_fitting_page in 0.9s build + 29.9s test
  - MISSED   src/payload_storage.rs:103:9: replace PayloadStorage::find_best_fitting_page -> Option<u32> with None in 0.6s build + 60.0s test
  - MISSED   src/payload_storage.rs:107:69: replace + with * in PayloadStorage::find_best_fitting_page in 0.5s build + 30.5s test
  - MISSED   src/payload_storage.rs:63:9: replace PayloadStorage::is_empty -> bool with true in 0.5s build + 29.5s test
  - MISSED   src/payload_storage.rs:63:31: replace && with || in PayloadStorage::is_empty in 0.9s build + 30.5s test
  - MISSED   src/page_tracker.rs:113:36: replace + with * in PageTracker::persist_pointer in 0.9s build + 30.4s test
  - MISSED   src/slotted_page.rs:293:20: replace > with < in SlottedPageMmap::insert_value in 0.5s build + 29.4s test
  - MISSED   src/slotted_page.rs:352:26: replace > with < in SlottedPageMmap::update_value in 0.4s build + 28.8s test
  - MISSED   src/slotted_page.rs:92:9: replace SlottedPageMmap::flush with () in 0.4s build + 28.6s test
  - MISSED   src/slotted_page.rs:293:20: replace > with == in SlottedPageMmap::insert_value in 0.5s build + 29.0s test