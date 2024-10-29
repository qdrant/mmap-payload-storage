pub trait Value {
    fn to_bytes<'a>(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Self;
}


