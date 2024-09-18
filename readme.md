# mmap-payload-storage

New experimental storage for vector payloads using mmap.

https://www.notion.so/qdrant/Mmap-payload-storage-d4734f1f64e141fb9a249486e7cde314


## TODOs

- [ ] implement deletion with tombstones
- [ ] test deletion
- [ ] implement update
- [ ] test update from placeholder
- [ ] test update with larger data not fitting
- [ ] test update with smaller data fitting
- [ ] persist page tracker
- [ ] optimize to decrease fragmentation