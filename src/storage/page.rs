//! Fixed-size page envelope with a checksummed, zstd-compressed payload.
//!
//! A page is the unit the buffer pool caches and the page store writes.
//! On disk the file is the 32-byte header plus the compressed payload, so
//! a repetitive page occupies fewer bytes than its logical payload. The
//! file never grows past `page_size`. The buffer pool only sees the
//! decompressed payload.

use super::crc::crc32;

/// On-disk page magic (`PAGE`).
pub const PAGE_MAGIC: u32 = 0x5041_4745;

/// Bytes occupied by the header. The compressed payload starts immediately after.
pub const PAGE_HEADER_LEN: usize = 32;

/// Smallest page size whose envelope can hold a zstd frame.
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
    pub fn payload_capacity(page_size: usize) -> Result<usize, String> {
        let physical = physical_capacity(page_size)?;
        let mut lo = 0usize;
        let mut hi = physical;
        while lo < hi {
            let mid = lo + (hi - lo + 1) / 2;
            if zstd::zstd_safe::compress_bound(mid) <= physical {
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
        let compressed = zstd::bulk::compress(&self.payload, zstd::DEFAULT_COMPRESSION_LEVEL)
            .map_err(|err| format!("page {} compress: {err}", self.id))?;
        let physical = physical_capacity(page_size)?;
        if compressed.len() > physical {
            return Err(format!(
                "page {} compressed payload is {} bytes, capacity is {}",
                self.id,
                compressed.len(),
                physical
            ));
        }
        if compressed.len() > u32::MAX as usize {
            return Err(format!("page {} compressed payload exceeds u32", self.id));
        }

        let mut bytes = vec![0u8; PAGE_HEADER_LEN + compressed.len()];
        bytes[0..4].copy_from_slice(&PAGE_MAGIC.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.id.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.lsn.to_le_bytes());
        bytes[20..24].copy_from_slice(&(compressed.len() as u32).to_le_bytes());
        bytes[PAGE_HEADER_LEN..].copy_from_slice(&compressed);
        let checksum = crc32(&bytes);
        bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    /// Decodes a page and rejects a bad magic, checksum, or zstd frame.
    pub fn decode(bytes: &[u8], page_size: usize) -> Result<Self, String> {
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

        let capacity = Self::payload_capacity(page_size)?;
        let payload = zstd::bulk::decompress(&bytes[PAGE_HEADER_LEN..], capacity)
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
        let encoded = page.encode(MIN_PAGE_SIZE).expect("encode");
        assert!(encoded.len() <= MIN_PAGE_SIZE);
        assert!(encoded.len() > PAGE_HEADER_LEN);
        let decoded = Page::decode(&encoded, MIN_PAGE_SIZE).expect("decode");
        assert_eq!(decoded, page);
    }

    #[test]
    fn test_page_rejects_flipped_payload() {
        let page = Page::new(1, 1, b"hello".to_vec());
        let mut encoded = page.encode(MIN_PAGE_SIZE).expect("encode");
        encoded[PAGE_HEADER_LEN] ^= 0xff;
        let err = Page::decode(&encoded, MIN_PAGE_SIZE).expect_err("checksum");
        assert!(err.contains("checksum"), "{}", err);
    }

    #[test]
    fn test_page_rejects_payload_larger_than_capacity() {
        let page = Page::new(1, 1, vec![1u8; MIN_PAGE_SIZE]);
        assert!(page.encode(MIN_PAGE_SIZE).is_err());
    }

    #[test]
    fn test_full_logical_payload_fits_in_the_envelope() {
        let page_size = 4096;
        let capacity = Page::payload_capacity(page_size).expect("capacity");
        let page = Page::new(1, 1, vec![0xA5; capacity]);
        let encoded = page.encode(page_size).expect("encode");
        assert!(encoded.len() <= page_size);
        let decoded = Page::decode(&encoded, page_size).expect("decode");
        assert_eq!(decoded.payload, page.payload);
    }

    #[test]
    fn test_repetitive_payload_is_smaller_than_the_raw_bytes() {
        let payload = vec![b'a'; 4000];
        let page = Page::new(3, 9, payload.clone());
        let encoded = page.encode(8192).expect("encode");
        assert!(
            encoded.len() < PAGE_HEADER_LEN + payload.len(),
            "compressed page is {} bytes, raw payload is {}",
            encoded.len(),
            payload.len()
        );
        let decoded = Page::decode(&encoded, 8192).expect("decode");
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_page_size_below_minimum_is_rejected() {
        assert!(Page::payload_capacity(64).is_err());
        assert!(Page::payload_capacity(MIN_PAGE_SIZE).is_ok());
    }
}
