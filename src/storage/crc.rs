//! IEEE CRC-32 (ISO 3309 / PNG polynomial).
//!
//! Shared by the WAL and the page store so a torn or bit-flipped write
//! is rejected before it reaches the in-memory collections.

/// Checksum of `data` using the reflected polynomial `0xEDB88320`.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_known_vector() {
        // The check value from the PNG / ISO 3309 specification.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn test_crc32_empty_is_stable() {
        assert_eq!(crc32(b""), 0);
    }
}
