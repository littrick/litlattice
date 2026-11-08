#![forbid(unsafe_code)]

//! Differential harness: encrypts a value with a mounted key exactly like
//! `encryption.MaybeEncrypt`, so a ciphertext produced here can be decrypted by
//! the Go baseline (the reverse of the `mounted` fixtures).
//!
//! Usage: `cargo run -p sg-encryption --example encrypt_config -- \
//!         <keyname> <version> <secret-file> <plaintext>`
//!
//! Prints `{"cipher": "...", "key_id": "..."}`, the same shape as the Go
//! harness (`p9encryptcfg` in the sourcegraph checkout).

use sg_encryption::{Key, MountedKey, MountedKeyConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        eprintln!("usage: encrypt_config <keyname> <version> <secret-file> <plaintext>");
        std::process::exit(2);
    }
    let key = MountedKey::from_config(&MountedKeyConfig {
        keyname: args[0].clone(),
        version: args[1].clone(),
        env_var_name: String::new(),
        filepath: args[2].clone(),
    })
    .unwrap_or_else(|err| {
        eprintln!("creating key: {err}");
        std::process::exit(1);
    });
    let cipher = key.encrypt(args[3].as_bytes()).await.unwrap_or_else(|err| {
        eprintln!("encrypting: {err}");
        std::process::exit(1);
    });
    let version = key.version().await.unwrap_or_else(|err| {
        eprintln!("reading version: {err}");
        std::process::exit(1);
    });
    let out = serde_json::json!({
        "cipher": String::from_utf8_lossy(&cipher),
        "key_id": version.json(),
    });
    println!("{out}");
}
