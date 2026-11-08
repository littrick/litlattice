//! Port of `internal/encryption/envelope`: encrypt the payload with a random
//! 32-byte key that is itself encrypted by the long-lived key secret.

use aes_gcm::aead::{rand_core::RngCore, OsRng};

use crate::aeshelper;
use crate::Error;

pub(crate) struct Envelope {
    pub key: Vec<u8>,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

/// `envelope.Encrypt`.
pub(crate) fn encrypt(plaintext: &[u8]) -> Result<Envelope, Error> {
    let mut key = vec![0u8; 32];
    OsRng.fill_bytes(&mut key);

    let (ciphertext, nonce) = aeshelper::encrypt(&key, plaintext)
        .map_err(|err| Error::msg(format!("encrypt plaintext: {err}")))?;

    Ok(Envelope {
        key,
        nonce,
        ciphertext,
    })
}

/// `envelope.Decrypt`.
pub(crate) fn decrypt(envelope: &Envelope) -> Result<Vec<u8>, Error> {
    aeshelper::decrypt(&envelope.key, &envelope.ciphertext, &envelope.nonce)
        .map_err(|err| Error::msg(format!("decrypt ciphertext: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_plaintext() {
        let envelope = encrypt(b"payload").expect("encrypt");
        assert_eq!(envelope.key.len(), 32);
        assert_eq!(decrypt(&envelope).expect("decrypt"), b"payload");
    }
}
