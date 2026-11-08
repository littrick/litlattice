//! Port of `internal/encryption/aeshelper`: AES-256-GCM with a 12-byte nonce.

use aes_gcm::aead::{rand_core::RngCore, Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};

use crate::Error;

/// `aeshelper.Encrypt`: returns `(ciphertext, nonce)`.
pub(crate) fn encrypt(secret: &[u8], plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let cipher = cipher(secret)?;
    let mut nonce = vec![0u8; 12];
    OsRng.fill_bytes(&mut nonce);

    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| Error::msg("cipher: message authentication failed"))?;
    Ok((ciphertext, nonce))
}

/// `aeshelper.Decrypt`.
pub(crate) fn decrypt(secret: &[u8], ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>, Error> {
    if nonce.len() != 12 {
        return Err(Error::msg(
            "crypto/cipher: incorrect nonce length given to GCM",
        ));
    }
    let cipher = cipher(secret)?;
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| Error::msg("cipher: message authentication failed"))
}

fn cipher(secret: &[u8]) -> Result<Aes256Gcm, Error> {
    if secret.len() != 32 {
        return Err(Error::msg("secret is not 32 characters long"));
    }
    Aes256Gcm::new_from_slice(secret)
        .map_err(|err| Error::msg(format!("generate new AES cipher: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_plaintext() {
        let secret = [7u8; 32];
        let (ciphertext, nonce) = encrypt(&secret, b"hello world").expect("encrypt");
        assert_eq!(nonce.len(), 12);
        assert_eq!(
            decrypt(&secret, &ciphertext, &nonce).expect("decrypt"),
            b"hello world"
        );
    }

    #[test]
    fn rejects_wrong_secret_and_tampering() {
        let secret = [7u8; 32];
        let (ciphertext, nonce) = encrypt(&secret, b"hello world").expect("encrypt");
        assert_eq!(
            decrypt(&[8u8; 32], &ciphertext, &nonce)
                .expect_err("wrong key")
                .to_string(),
            "cipher: message authentication failed"
        );

        let mut tampered = ciphertext.clone();
        tampered[0] ^= 1;
        assert!(decrypt(&secret, &tampered, &nonce).is_err());
    }

    #[test]
    fn rejects_non_32_byte_secrets() {
        assert_eq!(
            encrypt(b"short", b"x").expect_err("short").to_string(),
            "secret is not 32 characters long"
        );
    }
}
