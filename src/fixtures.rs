use crate::payload::Payload;
use crate::PayloadStorage;
use rand::distributions::{Distribution, Uniform};
use rand::Rng;
use tempfile::{Builder, TempDir};

pub fn empty_storage() -> (TempDir, PayloadStorage) {
    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let storage = PayloadStorage::new(dir.path().to_path_buf());
    assert!(storage.is_empty());
    (dir, storage)
}

pub fn random_word(rng: &mut impl Rng) -> String {
    let len = rng.gen_range(1..10);
    let mut word = String::with_capacity(len);
    for _ in 0..len {
        word.push(rng.gen_range(b'a'..=b'z') as char);
    }
    word
}

pub fn one_random_payload_please(rng: &mut impl Rng, size_factor: usize) -> Payload {
    let mut payload = Payload::default();

    let word = random_word(rng);

    let sentence = (0..rng.gen_range(1..20 * size_factor))
        .map(|_| random_word(rng))
        .collect::<Vec<_>>()
        .join(" ");

    let distr = Uniform::new(0, 100000);
    let indices = (0..rng.gen_range(1..100 * size_factor))
        .map(|_| distr.sample(rng))
        .collect::<Vec<_>>();

    payload.0 = serde_json::json!(
        {
            "word": word, // string
            "sentence": sentence, // string
            "number": rng.gen_range(0..1000), // number
            "indices": indices // array of numbers
        }
    )
    .as_object()
    .unwrap()
    .clone();

    payload
}
