//! AES-256-GCM for page files and WAL records.
//!
//! The nonce is random per write. On disk the sealed blob is
//! `nonce (12) || tag (16) || ciphertext`. Callers compress first.

use aes_gcm::aead::Aead;
use aes_gcm::aead::AeadCore;
use aes_gcm::aead::KeyInit;
use aes_gcm::aead::OsRng;
use aes_gcm::{Aes256Gcm, Key, Nonce};

pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;
pub const SEAL_LEN: usize = NONCE_LEN + TAG_LEN;

/// Encrypts `plaintext`. The result is nonce, tag, then ciphertext.
pub fn seal(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let mut sealed = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| "encrypt failed".to_string())?;
    if sealed.len() < TAG_LEN {
        return Err("ciphertext is missing its tag".into());
    }
    let tag = sealed.split_off(sealed.len() - TAG_LEN);
    let mut out = Vec::with_capacity(SEAL_LEN + sealed.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&tag);
    out.extend_from_slice(&sealed);
    Ok(out)
}

/// Decrypts a blob produced by [`seal`].
pub fn open(key: &[u8; 32], sealed: &[u8]) -> Result<Vec<u8>, String> {
    if sealed.len() < SEAL_LEN {
        return Err("sealed payload is truncated".into());
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Nonce::from_slice(&sealed[..NONCE_LEN]);
    let tag = &sealed[NONCE_LEN..SEAL_LEN];
    let ciphertext = &sealed[SEAL_LEN..];
    let mut combined = Vec::with_capacity(ciphertext.len() + TAG_LEN);
    combined.extend_from_slice(ciphertext);
    combined.extend_from_slice(tag);
    cipher
        .decrypt(nonce, combined.as_slice())
        .map_err(|_| "decrypt failed".to_string())
}
