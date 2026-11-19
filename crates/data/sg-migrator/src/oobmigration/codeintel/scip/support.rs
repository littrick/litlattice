#![forbid(unsafe_code)]

//! Small helpers ported from the Go tree for the SCIP migration:
//!
//! * `precise.HashKey`
//! * `ranges.EncodeRanges` (`internal/codeintel/shared/ranges`)
//! * `shared.ExtractSymbolIndexes` (`internal/codeintel/uploads/shared`)
//! * `extractLanguageFromIndexerName` (`lib/codeintel/lsif/scip`)
//! * SHA-256 for `hashPayload`

use sg_scip::is_local_symbol;
use sg_scip::position::Range;
use sg_scip::scip::{Document, Language};

use crate::error::MigratorError;

/// `precise.HashKey`: stable hash of a result identifier modulo the number of
/// result chunks.
pub fn hash_key(id: &str, max_index: i32) -> i32 {
    // Go's original panics on a zero chunk count; guard so a malformed upload
    // cannot bring the migrator down.
    if max_index <= 0 {
        return 0;
    }
    let mut hash: i32 = 0;
    for character in id.chars() {
        hash = hash
            .wrapping_shl(5)
            .wrapping_sub(hash)
            .wrapping_add(character as i32);
    }
    if hash < 0 {
        hash = hash.wrapping_neg();
    }
    hash % max_index
}

/// `ranges.EncodeRanges`.
pub fn encode_ranges(values: &[i32]) -> Result<Vec<u8>, MigratorError> {
    let n = values.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    if n % 4 != 0 {
        return Err(MigratorError::new(format!(
            "unexpected range length - have {n} but expected a multiple of 4"
        )));
    }

    let quarter = n / 4;
    let mut shuffled = vec![0i32; n];

    let mut range_index = 0usize;
    let mut range_offset = 0usize;
    while range_offset < n {
        let start_line = values[range_offset];
        let start_character = values[range_offset + 1];
        let line_distance = values[range_offset + 2] - values[range_offset];
        let character_distance = values[range_offset + 3] - values[range_offset + 1];

        let (
            previous_start_line,
            previous_start_character,
            previous_line_distance,
            previous_character_distance,
        ) = if range_index != 0 {
            let previous = (range_index - 1) * 4;
            (
                values[previous],
                values[previous + 1],
                values[previous + 2] - values[previous],
                values[previous + 3] - values[previous + 1],
            )
        } else {
            (0, 0, 0, 0)
        };

        shuffled[range_index] = start_line.wrapping_sub(previous_start_line);
        shuffled[quarter + range_index] = start_character.wrapping_sub(previous_start_character);
        shuffled[2 * quarter + range_index] = line_distance.wrapping_sub(previous_line_distance);
        shuffled[3 * quarter + range_index] =
            character_distance.wrapping_sub(previous_character_distance);

        range_index += 1;
        range_offset += 4;
    }

    // Reverse Q4 so the runs of zeros are contiguous.
    let mut i = 3 * quarter;
    let mut j = n.saturating_sub(1);
    while i < j {
        shuffled.swap(i, j);
        i += 1;
        j -= 1;
    }

    Ok(write_varints(&shuffled))
}

fn write_varints(values: &[i32]) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(values.len());
    let mut index = 0usize;
    while index < values.len() {
        if values[index] == 0 {
            let run_start = index;
            while index < values.len() && values[index] == 0 {
                index += 1;
            }
            append_varint(&mut buffer, 0);
            append_varint(&mut buffer, (index - run_start) as i64);
            continue;
        }
        append_varint(&mut buffer, i64::from(values[index]));
        index += 1;
    }
    buffer
}

/// `binary.AppendVarint` (zigzag + LEB128).
fn append_varint(buffer: &mut Vec<u8>, value: i64) {
    let mut encoded = (value as u64) << 1;
    if value < 0 {
        encoded = !encoded;
    }
    while encoded >= 0x80 {
        buffer.push((encoded as u8) | 0x80);
        encoded >>= 7;
    }
    buffer.push(encoded as u8);
}

/// `InvertedRangeIndex`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InvertedRangeIndex {
    pub symbol_name: String,
    pub definition_ranges: Vec<i32>,
    pub reference_ranges: Vec<i32>,
    pub implementation_ranges: Vec<i32>,
    pub type_definition_ranges: Vec<i32>,
}

#[derive(Default)]
struct RangeSet {
    definition_ranges: Vec<Range>,
    reference_ranges: Vec<Range>,
    implementation_ranges: Vec<Range>,
    type_definition_ranges: Vec<Range>,
}

/// `shared.ExtractSymbolIndexes`.
pub fn extract_symbol_indexes(document: &Document) -> Vec<InvertedRangeIndex> {
    use std::collections::BTreeMap;

    let mut ranges_by_symbol: BTreeMap<String, RangeSet> = BTreeMap::new();
    for occurrence in &document.occurrences {
        if occurrence.symbol.is_empty() || is_local_symbol(&occurrence.symbol) {
            continue;
        }
        let range = Range::new_unchecked(&occurrence.range);
        let entry = ranges_by_symbol
            .entry(occurrence.symbol.clone())
            .or_default();
        if occurrence.symbol_roles & (sg_scip::scip::SymbolRole::Definition as i32) > 0 {
            entry.definition_ranges.push(range);
        } else {
            entry.reference_ranges.push(range);
        }
    }

    for symbol in &document.symbols {
        let definition_ranges = ranges_by_symbol
            .get(&symbol.symbol)
            .map(|entry| entry.definition_ranges.clone())
            .unwrap_or_default();
        if definition_ranges.is_empty() {
            continue;
        }
        for relationship in &symbol.relationships {
            if !(relationship.is_implementation || relationship.is_type_definition) {
                continue;
            }
            let entry = ranges_by_symbol
                .entry(relationship.symbol.clone())
                .or_default();
            if relationship.is_implementation {
                entry
                    .implementation_ranges
                    .extend_from_slice(&definition_ranges);
            }
            if relationship.is_type_definition {
                entry
                    .type_definition_ranges
                    .extend_from_slice(&definition_ranges);
            }
        }
    }

    ranges_by_symbol
        .into_iter()
        .map(|(symbol_name, range_set)| InvertedRangeIndex {
            symbol_name,
            definition_ranges: collapse_ranges(range_set.definition_ranges),
            reference_ranges: collapse_ranges(range_set.reference_ranges),
            implementation_ranges: collapse_ranges(range_set.implementation_ranges),
            type_definition_ranges: collapse_ranges(range_set.type_definition_ranges),
        })
        .collect()
}

fn collapse_ranges(mut ranges: Vec<Range>) -> Vec<i32> {
    if ranges.is_empty() {
        return Vec::new();
    }
    ranges.sort_by(|a, b| a.compare_strict(b));
    let mut components = Vec::with_capacity(ranges.len() * 4);
    for range in ranges {
        components.push(range.start.line);
        components.push(range.start.character);
        components.push(range.end.line);
        components.push(range.end.character);
    }
    components
}

/// `extractLanguageFromIndexerName`.
pub fn extract_language_from_indexer_name(indexer_name: &str) -> String {
    for prefix in ["scip-", "lsif-"] {
        let Some(remainder) = indexer_name.strip_prefix(prefix) else {
            continue;
        };
        let needle = remainder.to_lowercase();
        for value in 0..=200 {
            if let Ok(language) = Language::try_from(value) {
                let name = language.as_str_name();
                if needle == name.to_lowercase() {
                    return name.to_string();
                }
            }
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// SHA-256 (`crypto/sha256`)
// ---------------------------------------------------------------------------

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// `sha256.Sum256`.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut message = data.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in message.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (index, word) in chunk.chunks_exact(4).enumerate() {
            w[index] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }

        let mut a = state[0];
        let mut b = state[1];
        let mut c = state[2];
        let mut d = state[3];
        let mut e = state[4];
        let mut f = state[5];
        let mut g = state[6];
        let mut h = state[7];

        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[index])
                .wrapping_add(w[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }

    let mut digest = [0u8; 32];
    for (index, word) in state.iter().enumerate() {
        digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_known_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(b"The quick brown fox jumps over the lazy dog")),
            "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn hash_key_wraps_like_go() {
        assert_eq!(hash_key("311", 9), 8);
        assert_eq!(hash_key("1266", 9), 3);
        assert_eq!(hash_key("15871", 9), 1);
        // Sanity: result is always in [0, max_index).
        for id in ["", "a", "15871", "github.com/x"] {
            let value = hash_key(id, 9);
            assert!((0..9).contains(&value), "id {id} -> {value}");
        }
    }

    #[test]
    fn encodes_ranges_with_zero_runs() {
        let ranges = [0, 8, 0, 18, 3, 2, 3, 9, 4, 2, 4, 5];
        // Run lengths are themselves zigzag varints (0x01 -> 0x02, 0x04 -> 0x08).
        assert_eq!(
            encode_ranges(&ranges).unwrap(),
            vec![0x00, 0x02, 0x06, 0x02, 0x10, 0x0b, 0x00, 0x08, 0x07, 0x05, 0x14]
        );
    }

    #[test]
    fn rejects_misaligned_ranges() {
        assert!(encode_ranges(&[1, 2, 3]).is_err());
        assert!(encode_ranges(&[]).unwrap().is_empty());
    }

    #[test]
    fn extracts_language_from_indexer_name() {
        assert_eq!(extract_language_from_indexer_name("lsif-go"), "Go");
        assert_eq!(
            extract_language_from_indexer_name("scip-typescript"),
            "TypeScript"
        );
        assert_eq!(extract_language_from_indexer_name("lsif-void"), "");
        assert_eq!(extract_language_from_indexer_name("other"), "");
    }
}
