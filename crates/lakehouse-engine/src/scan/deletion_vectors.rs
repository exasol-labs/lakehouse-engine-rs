//! Decoder for the Iceberg v3 `deletion-vector-v1` Puffin blob.
//!
//! iceberg-rust `v0.10.0-rc.2` reads the Puffin container (footer parse + blob
//! decompression via `PuffinReader`) but does NOT decode the
//! `deletion-vector-v1` payload, so this module decodes it here. Given the raw
//! (uncompressed — `deletion-vector-v1` is never Puffin-compressed) blob bytes
//! and the blob's declared `cardinality`, it produces the deleted row positions
//! as a [`RoaringTreemap`] that feeds the exact `RowSelection` /
//! `ParquetAccessPlan` union point the positional-delete path uses.
//!
//! Binary layout (Apache Iceberg `format/puffin-spec.md`, "deletion-vector-v1";
//! identical to the Delta Lake deletion-vector serialization):
//!
//! ```text
//! | length (4 bytes, BE) | magic (4 bytes) | serialized vector | CRC-32 (4 bytes, BE) |
//! ```
//!
//! - `length` is the combined byte length of `magic + serialized vector`
//!   (big-endian). It excludes the 4-byte length prefix and the 4-byte CRC.
//! - `magic` is the constant `D1 D3 39 64`.
//! - `serialized vector` is a *portable* 64-bit Roaring bitmap: an 8-byte
//!   little-endian count of 32-bit Roaring bitmaps, then for each (ordered by
//!   unsigned key) a 4-byte little-endian high key followed by that bitmap in
//!   the standard 32-bit Roaring on-disk format. A 64-bit position is
//!   reconstructed as `(high_key << 32) | value`.
//! - `CRC-32` is the checksum of `magic + serialized vector` (big-endian). The
//!   variant is CRC-32/ISO-HDLC (the zlib/gzip "IEEE" polynomial), matching
//!   Iceberg's and Delta's writers and the `crc32fast` crate.

use exasol_udf_sdk::error::UdfError;
use roaring::{RoaringBitmap, RoaringTreemap};
use std::io::Read;

/// The `deletion-vector-v1` magic bytes preceding the serialized Roaring vector.
const DV_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
/// Byte width of the big-endian combined-length prefix.
const LENGTH_PREFIX_BYTES: usize = 4;
/// Byte width of the magic marker.
const MAGIC_BYTES: usize = 4;
/// Byte width of the trailing big-endian CRC-32.
const CRC_BYTES: usize = 4;
/// Byte width of the little-endian 32-bit-bitmap count in the portable vector.
const BITMAP_COUNT_BYTES: usize = 8;
/// Byte width of each little-endian high key preceding a 32-bit bitmap.
const HIGH_KEY_BYTES: usize = 4;

/// Decode a `deletion-vector-v1` blob into the set of deleted 64-bit row
/// positions, failing loud on any structural, checksum, or cardinality
/// mismatch.
///
/// `blob` is the decompressed Puffin blob payload (the full
/// `length | magic | vector | crc` structure); `cardinality` is the blob's
/// declared deleted-row count from its Puffin `BlobMetadata`. The decoded
/// position count MUST equal `cardinality`, the magic MUST match, and the
/// trailing CRC-32 MUST match the checksum of `magic + vector`; any mismatch or
/// truncation returns an `Err` (the blob carries only positions, so no message
/// echoes caller credentials).
pub(crate) fn decode_deletion_vector_v1(
    blob: &[u8],
    cardinality: u64,
) -> Result<RoaringTreemap, UdfError> {
    if blob.len() < LENGTH_PREFIX_BYTES + CRC_BYTES {
        return Err(UdfError::User(format!(
            "deletion-vector-v1 blob is truncated ({} bytes, need at least {})",
            blob.len(),
            LENGTH_PREFIX_BYTES + CRC_BYTES
        )));
    }

    let length = u32::from_be_bytes(blob[..LENGTH_PREFIX_BYTES].try_into().unwrap()) as usize;
    let expected_total = LENGTH_PREFIX_BYTES + length + CRC_BYTES;
    if blob.len() != expected_total {
        return Err(UdfError::User(format!(
            "deletion-vector-v1 length prefix claims {length} bytes (expected {expected_total} \
             total) but the blob is {} bytes",
            blob.len()
        )));
    }
    if length < MAGIC_BYTES + BITMAP_COUNT_BYTES {
        return Err(UdfError::User(format!(
            "deletion-vector-v1 length prefix ({length}) is too small to hold the magic and \
             bitmap count"
        )));
    }

    let magic_and_vector = &blob[LENGTH_PREFIX_BYTES..LENGTH_PREFIX_BYTES + length];
    if magic_and_vector[..MAGIC_BYTES] != DV_MAGIC {
        return Err(UdfError::User(
            "deletion-vector-v1 blob has a corrupt magic marker (expected D1 D3 39 64)".into(),
        ));
    }

    let stored_crc = u32::from_be_bytes(
        blob[LENGTH_PREFIX_BYTES + length..]
            .try_into()
            .expect("CRC slice is exactly 4 bytes by construction"),
    );
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(magic_and_vector);
    let actual_crc = hasher.finalize();
    if actual_crc != stored_crc {
        return Err(UdfError::User(format!(
            "deletion-vector-v1 CRC-32 mismatch (stored {stored_crc:#010x}, computed \
             {actual_crc:#010x}); the blob is corrupt"
        )));
    }

    let positions = deserialize_portable_roaring(&magic_and_vector[MAGIC_BYTES..])?;

    if positions.len() != cardinality {
        return Err(UdfError::User(format!(
            "deletion-vector-v1 cardinality mismatch: blob metadata declares {cardinality} \
             deleted positions but the vector decodes to {}",
            positions.len()
        )));
    }

    Ok(positions)
}

/// Deserialize the portable 64-bit Roaring vector (bitmap count, then per-bitmap
/// high key + standard 32-bit Roaring bitmap) into a [`RoaringTreemap`].
fn deserialize_portable_roaring(vector: &[u8]) -> Result<RoaringTreemap, UdfError> {
    let mut cursor = std::io::Cursor::new(vector);

    let mut count_buf = [0u8; BITMAP_COUNT_BYTES];
    cursor.read_exact(&mut count_buf).map_err(|e| {
        UdfError::User(format!(
            "deletion-vector-v1 is missing its bitmap count: {e}"
        ))
    })?;
    let bitmap_count = u64::from_le_bytes(count_buf);

    let mut positions = RoaringTreemap::new();
    for _ in 0..bitmap_count {
        let mut high_key_buf = [0u8; HIGH_KEY_BYTES];
        cursor.read_exact(&mut high_key_buf).map_err(|e| {
            UdfError::User(format!(
                "deletion-vector-v1 is truncated reading a bitmap high key: {e}"
            ))
        })?;
        let high_key = u32::from_le_bytes(high_key_buf);

        let bitmap = RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
            UdfError::User(format!(
                "deletion-vector-v1 has a malformed 32-bit Roaring bitmap: {e}"
            ))
        })?;
        for value in &bitmap {
            positions.insert((u64::from(high_key) << 32) | u64::from(value));
        }
    }

    if (cursor.position() as usize) != vector.len() {
        return Err(UdfError::User(
            "deletion-vector-v1 has trailing bytes after its declared bitmaps; the blob is corrupt"
                .into(),
        ));
    }

    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Encode a set of 64-bit positions into a spec-conformant
    /// `deletion-vector-v1` blob, mirroring the layout the decoder confirmed in
    /// 2.B.1 so the tests are self-consistent with the spec.
    fn encode_dv(positions: &[u64]) -> Vec<u8> {
        let mut buckets: BTreeMap<u32, RoaringBitmap> = BTreeMap::new();
        for &p in positions {
            buckets
                .entry((p >> 32) as u32)
                .or_default()
                .insert(p as u32);
        }

        let mut vector = Vec::new();
        vector.extend_from_slice(&(buckets.len() as u64).to_le_bytes());
        for (key, bitmap) in &buckets {
            vector.extend_from_slice(&key.to_le_bytes());
            bitmap.serialize_into(&mut vector).unwrap();
        }

        let mut magic_and_vector = Vec::with_capacity(MAGIC_BYTES + vector.len());
        magic_and_vector.extend_from_slice(&DV_MAGIC);
        magic_and_vector.extend_from_slice(&vector);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&magic_and_vector);
        let crc = hasher.finalize();

        let mut blob = Vec::new();
        blob.extend_from_slice(&(magic_and_vector.len() as u32).to_be_bytes());
        blob.extend_from_slice(&magic_and_vector);
        blob.extend_from_slice(&crc.to_be_bytes());
        blob
    }

    fn decoded_positions(positions: &[u64]) -> Vec<u64> {
        let blob = encode_dv(positions);
        let tree = decode_deletion_vector_v1(&blob, positions.len() as u64).unwrap();
        tree.iter().collect()
    }

    #[test]
    fn decodes_portable_roaring_positions() {
        // Single-key bitmap (all positions share high key 0).
        assert_eq!(decoded_positions(&[3, 7, 9]), vec![3, 7, 9]);
    }

    #[test]
    fn decodes_multi_key_positions_beyond_2_32() {
        let positions = [5u64, (1u64 << 32) + 10, (2u64 << 32) + 7, (2u64 << 32) + 8];
        let mut expected = positions.to_vec();
        expected.sort_unstable();
        assert_eq!(decoded_positions(&positions), expected);
    }

    #[test]
    fn decodes_empty_bitmap() {
        let blob = encode_dv(&[]);
        let tree = decode_deletion_vector_v1(&blob, 0).unwrap();
        assert!(tree.is_empty());
    }

    #[test]
    fn cardinality_mismatch_errors() {
        let blob = encode_dv(&[3, 7]);
        let err = decode_deletion_vector_v1(&blob, 5).unwrap_err().to_string();
        assert!(err.contains("cardinality"), "names the mismatch: {err}");
    }

    #[test]
    fn corrupt_magic_or_crc_errors() {
        // Corrupt magic: flip the first magic byte (blob[LENGTH_PREFIX_BYTES]).
        let mut bad_magic = encode_dv(&[3, 7, 9]);
        bad_magic[LENGTH_PREFIX_BYTES] ^= 0xFF;
        assert!(decode_deletion_vector_v1(&bad_magic, 3).is_err());

        // Corrupt CRC: flip the last byte.
        let mut bad_crc = encode_dv(&[3, 7, 9]);
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 0xFF;
        assert!(decode_deletion_vector_v1(&bad_crc, 3).is_err());
    }

    #[test]
    fn truncated_blob_errors() {
        let blob = encode_dv(&[3, 7, 9]);
        assert!(decode_deletion_vector_v1(&blob[..blob.len() - 2], 3).is_err());
        assert!(decode_deletion_vector_v1(&blob[..2], 3).is_err());
    }
}
