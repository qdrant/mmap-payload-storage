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

![Slotted Page](./slotted%20pages.svg)

## TODOs

- [ ] compaction to decrease fragmentation
- [ ] benchmarking with realistic data payload