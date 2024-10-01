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

- [ ] reuse deleted slots on write (very nice to have)
- [ ] test best fitting page logic
- [ ] add to page header defragmentation stats
- [ ] improve page selection on write (maybe binary search)
- [ ] report storage wide fragmentation stats (for optimizer)
- [ ] test data consistency on panic
- [ ] test new payload storage vs RocksDB equivalent (speed & storage size)
- [ ] dictionary compression to optimize payload key repetition
- [ ] validate the usage with a block storage via HTTP range requests
