use criterion::{criterion_group, criterion_main, Criterion};
use mmap_payload_storage::fixtures::{empty_storage, HM_FIELDS};
use mmap_payload_storage::payload::Payload;
use serde_json::Value;

pub fn real_data_data_bench(c: &mut Criterion) {
    let (_dir, mut storage) = empty_storage();

    let csv_data = include_str!("../data/h&m-articles.csv");
    let mut rdr = csv::Reader::from_reader(csv_data.as_bytes());
    let mut point_offset = 0;

    c.bench_function("write real payload", |b| {
        b.iter(|| {
            for result in rdr.records() {
                let record = result.unwrap();
                let mut payload = Payload::default();
                for (i, field) in HM_FIELDS.iter().enumerate() {
                    payload.0.insert(
                        field.to_string(),
                        Value::String(record.get(i).unwrap().to_string()),
                    );
                }
                storage.put_payload(point_offset, &payload);
                point_offset += 1;
            }
        });
    });
    assert_eq!(point_offset, 105_542);

    c.bench_function("read real payload", |b| {
        b.iter(|| {
            for i in 0..point_offset {
                let res = storage.get_payload(i).unwrap();
                assert!(res.0.contains_key("article_id"));
            }
        });
    });
}

criterion_group!(benches, real_data_data_bench);
criterion_main!(benches);
