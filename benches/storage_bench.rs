use criterion::{criterion_group, criterion_main, Criterion};
use mmap_payload_storage::fixtures::{empty_storage, one_random_payload_please};

const PAYLOAD_COUNT: usize = 10_000;

pub fn storage_bench(c: &mut Criterion) {
    let (_dir, mut storage) = empty_storage();
    let mut rng = rand::thread_rng();
    c.bench_function("write/read payload", |b| {
        let payload = one_random_payload_please(&mut rng, 1);
        b.iter(|| {
            for i in 0..PAYLOAD_COUNT {
                storage.put_payload(i as u32, payload.clone());
                let res = storage.get_payload(i as u32);
                assert!(res.is_some());
            }
        });
    });
}

criterion_group!(benches, storage_bench);
criterion_main!(benches);
