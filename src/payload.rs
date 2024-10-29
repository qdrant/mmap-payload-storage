use serde::{Deserialize, Serialize};
use serde_json::Map;

use crate::value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Payload(pub Map<String, serde_json::Value>);

impl Default for Payload {
    fn default() -> Self {
        Payload(serde_json::Map::new())
    }
}

impl Value for Payload {
    fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        ciborium::ser::into_writer(self, &mut vec).unwrap();
        vec
    }

    fn from_bytes(data: &[u8]) -> Self {
        ciborium::de::from_reader(data).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::value::Value;
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
