//! Fixed-size page envelope with a checksummed, zstd-compressed payload.
//!
//! A page is the unit the buffer pool caches and the page store writes.
//! On disk the file is the 32-byte header plus the compressed payload, so
//! a repetitive page occupies fewer bytes than its logical payload. The
//! file never grows past `page_size`. The buffer pool only sees the
//! decompressed payload.

use super::crc::crc32;
use super::crypto::{self, SEAL_LEN};

/// On-disk page magic (`PAGE`).
pub const PAGE_MAGIC: u32 = 0x5041_4745;

/// Bytes occupied by the header. The compressed payload starts immediately after.
pub const PAGE_HEADER_LEN: usize = 32;

/// Byte 28. Set when the payload is an AES-256-GCM seal of the zstd frame.
const FLAG_ENCRYPTED: u8 = 1;

/// Smallest page size whose envelope can hold a zstd frame.
#[cfg_attr(not(test), allow(dead_code))]
pub const MIN_PAGE_SIZE: usize = 128;

/// One page.
///
/// `payload` is the decompressed bytes. The on-disk image stores that
/// payload as a zstd frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    /// Stable identifier. Never reused inside one partition.
    pub id: u64,
    /// Checkpoint LSN this page was written for.
    pub lsn: u64,
    /// Decompressed bytes.
    pub payload: Vec<u8>,
}

impl Page {
    /// Builds a page from its identity and decompressed payload.
    pub fn new(id: u64, lsn: u64, payload: Vec<u8>) -> Self {
        Self { id, lsn, payload }
    }

    /// How many decompressed payload bytes fit in a page of `page_size`.
    ///
    /// The limit is the largest size whose worst-case zstd frame still
    /// fits in the envelope, so a full page of incompressible bytes encodes.
    pub fn payload_capacity(page_size: usize, encrypted: bool) -> Result<usize, String> {
        let physical = physical_capacity(page_size)?;
        let seal = if encrypted { SEAL_LEN } else { 0 };
        let budget = physical.saturating_sub(seal);
        let mut lo = 0usize;
        let mut hi = budget;
        while lo < hi {
            let mid = lo + (hi - lo + 1) / 2;
            if zstd::zstd_safe::compress_bound(mid) <= budget {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        if lo == 0 {
            return Err(format!(
                "page size {page_size} is too small to hold a compressed payload"
            ));
        }
        Ok(lo)
    }

    /// Encodes the page. The result is at most `page_size` bytes.
    pub fn encode(&self, page_size: usize, key: Option<&[u8; 32]>) -> Result<Vec<u8>, String> {
        let capacity = Self::payload_capacity(page_size, key.is_some())?;
        if self.payload.len() > capacity {
            return Err(format!(
                "page {} payload is {} bytes, capacity is {}",
                self.id,
                self.payload.len(),
                capacity
            ));
        }
        let compressed = zstd::bulk::compress(&self.payload, zstd::DEFAULT_COMPRESSION_LEVEL)
            .map_err(|err| format!("page {} compress: {err}", self.id))?;
        let (flag, body) = match key {
            Some(key) => (
                FLAG_ENCRYPTED,
                crypto::seal(key, &compressed)
                    .map_err(|err| format!("page {} encrypt: {err}", self.id))?,
            ),
            None => (0, compressed),
        };
        let physical = physical_capacity(page_size)?;
        if body.len() > physical {
            return Err(format!(
                "page {} stored payload is {} bytes, capacity is {}",
                self.id,
                body.len(),
                physical
            ));
        }
        if body.len() > u32::MAX as usize {
            return Err(format!("page {} stored payload exceeds u32", self.id));
        }

        let mut bytes = vec![0u8; PAGE_HEADER_LEN + body.len()];
        bytes[0..4].copy_from_slice(&PAGE_MAGIC.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.id.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.lsn.to_le_bytes());
        bytes[20..24].copy_from_slice(&(body.len() as u32).to_le_bytes());
        bytes[28] = flag;
        bytes[PAGE_HEADER_LEN..].copy_from_slice(&body);
        let checksum = crc32(&bytes);
        bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    /// Decodes a page and rejects a bad magic, checksum, or zstd frame.
    pub fn decode(bytes: &[u8], page_size: usize, key: Option<&[u8; 32]>) -> Result<Self, String> {
        if bytes.len() < PAGE_HEADER_LEN {
            return Err(format!(
                "page is {} bytes, header needs {}",
                bytes.len(),
                PAGE_HEADER_LEN
            ));
        }
        if bytes.len() > page_size {
            return Err(format!(
                "page is {} bytes, limit is {}",
                bytes.len(),
                page_size
            ));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != PAGE_MAGIC {
            return Err(format!("page magic {:08x} is not PAGE", magic));
        }
        let id = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let lsn = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let payload_len = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
        let stored = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        if PAGE_HEADER_LEN + payload_len != bytes.len() {
            return Err(format!(
                "page {id} is {} bytes, compressed payload claims {payload_len}",
                bytes.len()
            ));
        }

        let mut check = bytes.to_vec();
        check[24..28].fill(0);
        let actual = crc32(&check);
        if actual != stored {
            return Err(format!(
                "page {id} checksum mismatch (stored {stored:08x}, computed {actual:08x})"
            ));
        }

        let flag = bytes[28];
        let stored_body = &bytes[PAGE_HEADER_LEN..];
        let compressed = match (flag, key) {
            (0, None) => stored_body.to_vec(),
            (0, Some(_)) => return Err(format!("page {id} is not encrypted")),
            (FLAG_ENCRYPTED, None) => return Err(format!("page {id} is encrypted")),
            (FLAG_ENCRYPTED, Some(key)) => {
                crypto::open(key, stored_body).map_err(|err| format!("page {id} decrypt: {err}"))?
            }
            _ => return Err(format!("page {id} encryption flag {flag} is unknown")),
        };
        let capacity = Self::payload_capacity(page_size, flag == FLAG_ENCRYPTED)?;
        let payload = zstd::bulk::decompress(&compressed, capacity)
            .map_err(|err| format!("page {id} decompress: {err}"))?;
        Ok(Self { id, lsn, payload })
    }
}

fn physical_capacity(page_size: usize) -> Result<usize, String> {
    page_size
        .checked_sub(PAGE_HEADER_LEN)
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            format!("page size {page_size} is smaller than the {PAGE_HEADER_LEN} byte header")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_round_trip() {
        let page = Page::new(7, 42, b"emp".to_vec());
        let encoded = page.encode(MIN_PAGE_SIZE, None).expect("encode");
        assert!(encoded.len() <= MIN_PAGE_SIZE);
        assert!(encoded.len() > PAGE_HEADER_LEN);
        let decoded = Page::decode(&encoded, MIN_PAGE_SIZE, None).expect("decode");
        assert_eq!(decoded, page);
    }

    #[test]
    fn test_page_rejects_flipped_payload() {
        let page = Page::new(1, 1, b"hello".to_vec());
        let mut encoded = page.encode(MIN_PAGE_SIZE, None).expect("encode");
        encoded[PAGE_HEADER_LEN] ^= 0xff;
        let err = Page::decode(&encoded, MIN_PAGE_SIZE, None).expect_err("checksum");
        assert!(err.contains("checksum"), "{}", err);
    }

    #[test]
    fn test_page_rejects_payload_larger_than_capacity() {
        let page = Page::new(1, 1, vec![1u8; MIN_PAGE_SIZE]);
        assert!(page.encode(MIN_PAGE_SIZE, None).is_err());
    }

    #[test]
    fn test_full_logical_payload_fits_in_the_envelope() {
        let page_size = 4096;
        let capacity = Page::payload_capacity(page_size, false).expect("capacity");
        let page = Page::new(1, 1, vec![0xA5; capacity]);
        let encoded = page.encode(page_size, None).expect("encode");
        assert!(encoded.len() <= page_size);
        let decoded = Page::decode(&encoded, page_size, None).expect("decode");
        assert_eq!(decoded.payload, page.payload);
    }

    #[test]
    fn test_repetitive_payload_is_smaller_than_the_raw_bytes() {
        let payload = vec![b'a'; 4000];
        let page = Page::new(3, 9, payload.clone());
        let encoded = page.encode(8192, None).expect("encode");
        assert!(
            encoded.len() < PAGE_HEADER_LEN + payload.len(),
            "compressed page is {} bytes, raw payload is {}",
            encoded.len(),
            payload.len()
        );
        let decoded = Page::decode(&encoded, 8192, None).expect("decode");
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_page_size_below_minimum_is_rejected() {
        assert!(Page::payload_capacity(64, false).is_err());
        assert!(Page::payload_capacity(MIN_PAGE_SIZE, false).is_ok());
    }

    #[test]
    fn test_encrypted_page_round_trip_and_rejects_a_missing_key() {
        let key = [9u8; 32];
        let page = Page::new(4, 8, vec![b'k'; 200]);
        let encoded = page.encode(4096, Some(&key)).expect("encode");
        assert_eq!(encoded[28], FLAG_ENCRYPTED);
        let err = Page::decode(&encoded, 4096, None).expect_err("needs the key");
        assert!(err.contains("encrypted"), "{err}");
        let decoded = Page::decode(&encoded, 4096, Some(&key)).expect("decode");
        assert_eq!(decoded.payload, page.payload);
    }
}
