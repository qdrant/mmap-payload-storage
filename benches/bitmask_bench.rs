use bitvec::vec::BitVec;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mmap_payload_storage::bitmask::{Bitmask, REGION_SIZE_BLOCKS};
use rand::{thread_rng, Rng};

pub fn bench_calculate_gaps(c: &mut Criterion) {
    let distr = rand::distributions::Standard;
    let rng = thread_rng();
    let random_bitvec = rng
        .sample_iter::<bool, _>(distr)
        .take(1000 * REGION_SIZE_BLOCKS as usize)
        .collect::<BitVec>();

    let mut bitslice_iter = random_bitvec.windows(REGION_SIZE_BLOCKS as usize).cycle();

    c.bench_function("calculate_gaps", |b| {
        b.iter(|| {
            let bitslice = bitslice_iter.next().unwrap();
            Bitmask::calculate_gaps(black_box(bitslice))
        })
    });
    
    c.bench_function("find_available_blocks", |b| {
        let mut rng = thread_rng();
        b.iter(|| {
            let bitslice = bitslice_iter.next().unwrap();
            let num_blocks = rng.gen_range(1..10);
            Bitmask::find_available_blocks_in_slice(black_box(bitslice), num_blocks, |_| (0,0))
        })
    });
}

criterion_group!(benches, bench_calculate_gaps);
criterion_main!(benches);
