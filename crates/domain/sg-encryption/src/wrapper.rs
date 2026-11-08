//! Port of `internal/encryption/wrapper`: the storable representation of an
//! encrypted value, `base64(json(StorableEncryptedKey))`.
//!
//! JSON field order and `[]byte`-as-base64 encoding match Go's
//! `encoding/json`, so values written by either implementation are readable by
//! the other.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/// `wrapper.StorableEncryptedKey`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorableEncryptedKey {
    /// The encryption mechanism, e.g. `envelope`; empty is the legacy
    /// `encrypt` mechanism.
    #[serde(rename = "Mechanism", default)]
    pub mechanism: String,
    /// The configured key name.
    #[serde(rename = "KeyName", default)]
    pub key_name: String,
    /// The wrapped per-value key (`nonce || ciphertext`) for the
    /// `envelope` mechanism.
    #[serde(
        rename = "WrappedKey",
        default,
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    pub wrapped_key: Vec<u8>,
    /// The encrypted payload (or `nonce || ciphertext` for `encrypt`).
    #[serde(
        rename = "Ciphertext",
        default,
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    pub ciphertext: Vec<u8>,
    /// The nonce used for `Ciphertext` in the `envelope` mechanism.
    #[serde(
        rename = "Nonce",
        default,
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    pub nonce: Vec<u8>,
}

impl StorableEncryptedKey {
    /// `wrapper.FromCiphertext`.
    pub fn from_ciphertext(ciphertext: &[u8]) -> Result<Self, Error> {
        let data = STANDARD.decode(ciphertext).map_err(|err| {
            Error::msg(format!(
                "failed to decode ciphertext: {}",
                go_base64_error(&err, ciphertext.len())
            ))
        })?;
        serde_json::from_slice(&data).map_err(|err| {
            Error::msg(format!(
                "failed to unmarshal ciphertext into StorableEncryptedKey: {err}"
            ))
        })
    }

    /// `StorableEncryptedKey.Serialize`.
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let json = serde_json::to_vec(self)
            .map_err(|err| Error::msg(format!("marshaling StorableEncryptedKey to JSON: {err}")))?;
        Ok(STANDARD.encode(json).into_bytes())
    }
}

/// Go's `encoding/base64.CorruptInputError` text, which the baseline surfaces
/// verbatim when a stored value is malformed.
fn go_base64_error(err: &base64::DecodeError, input_len: usize) -> String {
    match err {
        base64::DecodeError::InvalidByte(offset, _)
        | base64::DecodeError::InvalidLength(offset)
        | base64::DecodeError::InvalidLastSymbol(offset, _) => {
            format!("illegal base64 data at input byte {offset}")
        }
        // Go reports the offset of the offending padding character; the
        // decoder does not expose it, so point at the last input byte.
        base64::DecodeError::InvalidPadding => {
            format!(
                "illegal base64 data at input byte {}",
                input_len.saturating_sub(1)
            )
        }
    }
}

fn serialize_bytes<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&STANDARD.encode(bytes))
}

fn deserialize_bytes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    // Go marshals a nil slice as `null`; accept that (and a missing field,
    // handled by `default`) as empty bytes.
    let encoded = Option::<String>::deserialize(deserializer)?;
    match encoded {
        None => Ok(Vec::new()),
        Some(encoded) => STANDARD
            .decode(encoded.as_bytes())
            .map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_with_go_field_names_and_base64_bytes() {
        let sek = StorableEncryptedKey {
            mechanism: "envelope".to_owned(),
            key_name: "key".to_owned(),
            wrapped_key: vec![1, 2, 3],
            ciphertext: vec![4, 5],
            nonce: vec![6],
        };
        let serialized = sek.serialize().expect("serialize");
        let json: serde_json::Value =
            serde_json::from_slice(&STANDARD.decode(&serialized).expect("b64")).expect("json");
        assert_eq!(
            json,
            serde_json::json!({
                "Mechanism": "envelope",
                "KeyName": "key",
                "WrappedKey": "AQID",
                "Ciphertext": "BAU=",
                "Nonce": "Bg==",
            })
        );
        assert_eq!(
            StorableEncryptedKey::from_ciphertext(&serialized).expect("parse"),
            sek
        );
    }

    #[test]
    fn accepts_null_byte_fields() {
        let json = r#"{"Mechanism":"","KeyName":"key","WrappedKey":null,"Ciphertext":"BAU=","Nonce":null}"#;
        let sek =
            StorableEncryptedKey::from_ciphertext(STANDARD.encode(json).as_bytes()).expect("parse");
        assert_eq!(sek.wrapped_key, Vec::<u8>::new());
        assert_eq!(sek.ciphertext, vec![4, 5]);
        assert_eq!(sek.nonce, Vec::<u8>::new());
    }

    #[test]
    fn reports_go_base64_errors() {
        let err = StorableEncryptedKey::from_ciphertext(b"!!!!").expect_err("bad base64");
        assert_eq!(
            err.to_string(),
            "failed to decode ciphertext: illegal base64 data at input byte 0"
        );
    }
}
