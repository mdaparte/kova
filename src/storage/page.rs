//! Fixed-size pages with a checksummed header.
//!
//! A page is the unit the buffer pool caches and the page store writes.
//! Checkpoint images that do not fit in one page are split across a
//! chain of pages; each page carries the checkpoint LSN so a reader can
//! tell which snapshot it belongs to.

use super::crc::crc32;

/// On-disk page magic (`PAGE`).
pub const PAGE_MAGIC: u32 = 0x5041_4745;

/// Bytes occupied by the header. The payload starts immediately after.
pub const PAGE_HEADER_LEN: usize = 32;

/// One fixed-size page.
///
/// `payload` is the live bytes. On disk the page is padded with zeros
/// out to the configured page size so every read is the same length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    /// Stable identifier. Never reused inside one partition.
    pub id: u64,
    /// Checkpoint LSN this page was written for.
    pub lsn: u64,
    /// Live bytes. Shorter than the on-disk slot.
    pub payload: Vec<u8>,
}

impl Page {
    /// Builds a page from its identity and payload.
    pub fn new(id: u64, lsn: u64, payload: Vec<u8>) -> Self {
        Self { id, lsn, payload }
    }

    /// How many payload bytes fit in a page of `page_size`.
    pub fn payload_capacity(page_size: usize) -> Result<usize, String> {
        page_size
            .checked_sub(PAGE_HEADER_LEN)
            .filter(|n| *n > 0)
            .ok_or_else(|| {
                format!(
                    "page size {} is smaller than the {} byte header",
                    page_size, PAGE_HEADER_LEN
                )
            })
    }

    /// Encodes the page into exactly `page_size` bytes.
    pub fn encode(&self, page_size: usize) -> Result<Vec<u8>, String> {
        let capacity = Self::payload_capacity(page_size)?;
        if self.payload.len() > capacity {
            return Err(format!(
                "page {} payload is {} bytes, capacity is {}",
                self.id,
                self.payload.len(),
                capacity
            ));
        }
        if self.payload.len() > u32::MAX as usize {
            return Err(format!("page {} payload exceeds u32", self.id));
        }

        let mut bytes = vec![0u8; page_size];
        bytes[0..4].copy_from_slice(&PAGE_MAGIC.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.id.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.lsn.to_le_bytes());
        bytes[20..24].copy_from_slice(&(self.payload.len() as u32).to_le_bytes());
        // bytes[24..28] stays zero while the checksum is computed.
        bytes[32..32 + self.payload.len()].copy_from_slice(&self.payload);
        let checksum = crc32(&bytes);
        bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    /// Decodes a page and rejects a bad magic or checksum.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < PAGE_HEADER_LEN {
            return Err(format!(
                "page is {} bytes, header needs {}",
                bytes.len(),
                PAGE_HEADER_LEN
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
        if payload_len > bytes.len() - PAGE_HEADER_LEN {
            return Err(format!(
                "page {} payload length {} exceeds the page",
                id, payload_len
            ));
        }

        let mut check = bytes.to_vec();
        check[24..28].fill(0);
        let actual = crc32(&check);
        if actual != stored {
            return Err(format!(
                "page {} checksum mismatch (stored {:08x}, computed {:08x})",
                id, stored, actual
            ));
        }

        Ok(Self {
            id,
            lsn,
            payload: bytes[PAGE_HEADER_LEN..PAGE_HEADER_LEN + payload_len].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_round_trip() {
        let page = Page::new(7, 42, b"emp".to_vec());
        let encoded = page.encode(64).expect("encode");
        assert_eq!(encoded.len(), 64);
        let decoded = Page::decode(&encoded).expect("decode");
        assert_eq!(decoded, page);
    }

    #[test]
    fn test_page_rejects_flipped_payload() {
        let page = Page::new(1, 1, b"hello".to_vec());
        let mut encoded = page.encode(64).expect("encode");
        encoded[PAGE_HEADER_LEN] ^= 0xff;
        let err = Page::decode(&encoded).expect_err("checksum");
        assert!(err.contains("checksum"), "{}", err);
    }

    #[test]
    fn test_page_rejects_payload_larger_than_capacity() {
        let page = Page::new(1, 1, vec![1u8; 64]);
        assert!(page.encode(64).is_err());
    }
}
