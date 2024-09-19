use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Payload(pub Map<String, Value>);

impl Default for Payload {
    fn default() -> Self {
        Payload(serde_json::Map::new())
    }
}

impl Payload {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(self).unwrap()
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        serde_cbor::from_slice(data).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::payload::Payload;

    #[test]
    fn test_serde_symmetry() {
        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );
        let bytes = payload.to_bytes();

        let deserialized = Payload::from_bytes(&bytes);
        assert_eq!(payload, deserialized);
    }
}
