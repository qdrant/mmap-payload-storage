# mmap-payload-storage

New experimental storage for vector payloads using mmap.

## Design

- The storage is divided into pages of fixed minimum size (32MB).
- Larger payload are stored in multiple pages.
- Those pages are mapped into memory using mmap
- Values have a dynamic size and a min size (128 bytes)
- Values are compressed with lz4
- Deletes mark the block as deleted & update region
- Updates:
  - not done in place, always a new value is inserted
- Supports multiple threads reading and single thread writing

## TODOs

- [ ] test data consistency on panic
- [ ] dictionary compression to optimize payload key repetition
- [ ] validate the usage with a block storage via HTTP range requests
