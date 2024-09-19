# mmap-payload-storage

New experimental storage for vector payloads using mmap.

## Design

- The storage is divided into pages of fixed size (32MB)
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

- [ ] test update from placeholder
- [ ] test update with larger data not fitting
- [ ] test update with smaller data fitting
- [ ] persist page tracker (as a flat vector)
- [ ] load payload_storage from disk
- [ ] how to handle very large payloads?
- [ ] optimize to decrease fragmentation