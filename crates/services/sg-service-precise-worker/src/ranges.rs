#![forbid(unsafe_code)]

//! Port of `internal/codeintel/shared/ranges/ranges.go`'s `EncodeRanges`.
//!
//! `codeintel_scip_symbols.{definition,reference,implementation,
//! type_definition}_ranges` store range quads (start line, start character,
//! end line, end character) as delta-encoded, column-oriented varints with
//! run-length encoded zero runs. The encoding is part of the storage contract:
//! readers in the Go baseline decode these bytes.

/// `EncodeRanges`: encode a flattened sequence of range quads.
///
/// The input length must be a multiple of four. An empty input encodes to an
/// empty byte string (the baseline stores it as `''::bytea`, not SQL NULL).
pub fn encode_ranges(values: &[i32]) -> Result<Vec<u8>, RangeError> {
    let n = values.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    if n % 4 != 0 {
        return Err(RangeError::new(format!(
            "unexpected range length - have {n} but expected a multiple of 4"
        )));
    }

    let quarter = n / 4;
    let (q1_offset, q2_offset, q3_offset, q4_offset) = (0, quarter, quarter * 2, quarter * 3);
    let mut shuffled = vec![0i32; n];

    for range_index in 0..quarter {
        let offset = range_index * 4;
        let start_line = values[offset];
        let start_character = values[offset + 1];
        // Go int32 arithmetic wraps on overflow; `wrapping_sub` matches.
        let line_distance = values[offset + 2].wrapping_sub(values[offset]);
        let character_distance = values[offset + 3].wrapping_sub(values[offset + 1]);

        let (
            previous_start_line,
            previous_start_character,
            previous_line_distance,
            previous_character_distance,
        ) = if range_index == 0 {
            (0, 0, 0, 0)
        } else {
            let previous = (range_index - 1) * 4;
            (
                values[previous],
                values[previous + 1],
                values[previous + 2].wrapping_sub(values[previous]),
                values[previous + 3].wrapping_sub(values[previous + 1]),
            )
        };

        shuffled[q1_offset + range_index] = start_line.wrapping_sub(previous_start_line);
        shuffled[q2_offset + range_index] = start_character.wrapping_sub(previous_start_character);
        shuffled[q3_offset + range_index] = line_distance.wrapping_sub(previous_line_distance);
        shuffled[q4_offset + range_index] =
            character_distance.wrapping_sub(previous_character_distance);
    }

    // Reverse Q4 so its long run of zeros is contiguous for run-length
    // encoding (the tail of the shuffled array).
    shuffled[q4_offset..n].reverse();

    Ok(write_varints(&shuffled))
}

/// `writeVarints`: signed varints (zigzag), with zero runs encoded as
/// `0, run_length`.
fn write_varints(values: &[i32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len());
    let mut index = 0;
    while index < values.len() {
        let value = values[index];
        if value == 0 {
            let run_start = index;
            while index < values.len() && values[index] == 0 {
                index += 1;
            }
            append_varint(&mut buf, 0);
            append_varint(&mut buf, (index - run_start) as i64);
            continue;
        }
        append_varint(&mut buf, i64::from(value));
        index += 1;
    }
    buf
}

/// `binary.AppendVarint` for an `int64` (zigzag then unsigned varint).
fn append_varint(buf: &mut Vec<u8>, value: i64) {
    let mut remaining = ((value << 1) ^ (value >> 63)) as u64;
    while remaining >= 0x80 {
        buf.push((remaining as u8) | 0x80);
        remaining >>= 7;
    }
    buf.push(remaining as u8);
}

/// A range-encoding failure (the baseline's `errors.Newf`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeError {
    message: String,
}

impl RangeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RangeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_encodes_to_empty_bytes() {
        assert!(encode_ranges(&[]).expect("encode").is_empty());
    }

    #[test]
    fn non_quad_length_is_rejected() {
        let err = encode_ranges(&[1, 2, 3]).expect_err("error");
        assert_eq!(
            err.to_string(),
            "unexpected range length - have 3 but expected a multiple of 4"
        );
    }

    #[test]
    fn single_range_encodes_start_line_and_distances() {
        // Values: start line 7, char 3, line distance 0, char distance 4.
        // Shuffled: [7, 3, 0, 4] (Q4 has one element, so reversing is a
        // no-op). Varints: 7 -> 14, 3 -> 6, zero run of 1 -> 0, 2, then 4 -> 8.
        assert_eq!(
            encode_ranges(&[7, 3, 7, 7]).expect("encode"),
            vec![14, 6, 0, 2, 8]
        );
    }

    #[test]
    fn second_range_is_delta_encoded() {
        // Two single-character ranges on the same line.
        let encoded = encode_ranges(&[2, 1, 2, 2, 2, 5, 2, 6]).expect("encode");
        // Shuffled: q1 = [2, 0], q2 = [1, 4], q3 = [0, 0], q4 = [1, 0];
        // reversing q4 gives [0, 1], so the stream is
        // [2] [0] [1,4] [0,0,0] [1] -> 2, zero-run(1), 1, 4, zero-run(3), 1.
        assert_eq!(encoded, vec![4, 0, 2, 2, 8, 0, 6, 2]);
    }

    #[test]
    fn zigzag_matches_go_binary_append_varint() {
        let mut buf = Vec::new();
        for value in [-2i64, -1, 0, 1, 2, 300] {
            append_varint(&mut buf, value);
        }
        assert_eq!(buf, vec![3, 1, 0, 2, 4, 216, 4]);
    }
}
