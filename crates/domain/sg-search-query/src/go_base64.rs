#![forbid(unsafe_code)]

//! `encoding/base64.URLEncoding.DecodeString` for the `rat=` revspec
//! (`internal/search/query/repo_revs.go:ParseRevAtTime`).
//!
//! Go's decoder ignores `\r`/`\n` and reports the offending input byte. The
//! JSON payload is not interpreted here (see the M9.9e overview §3).

/// Decode URL-safe base64 with Go's `illegal base64 data at input byte N`
/// error text.
pub(crate) fn decode_url_base64(input: &str) -> Result<Vec<u8>, String> {
    let cleaned: String = input.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    let bytes = cleaned.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut index = 0;
    while index < bytes.len() {
        let remaining = bytes.len() - index;
        if remaining < 4 {
            for (offset, byte) in bytes[index..].iter().enumerate() {
                if !is_url_safe_alphabet(*byte) && *byte != b'=' {
                    return Err(format!(
                        "illegal base64 data at input byte {}",
                        index + offset
                    ));
                }
            }
            // Go reports the start of the incomplete quantum.
            return Err(format!("illegal base64 data at input byte {index}"));
        }
        let chunk = &bytes[index..index + 4];
        let final_quantum = remaining == 4;
        let (decoded, padded) = match decode_quantum(chunk, final_quantum) {
            Ok(value) => value,
            Err(offset) => {
                return Err(format!(
                    "illegal base64 data at input byte {}",
                    index + offset
                ))
            }
        };
        out.extend(decoded);
        index += 4;
        if padded {
            return Ok(out);
        }
    }
    Ok(out)
}

fn decode_quantum(chunk: &[u8], final_quantum: bool) -> Result<(Vec<u8>, bool), usize> {
    let value = |byte: u8| -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    };
    let v0 = value(chunk[0]).ok_or(0usize)?;
    let v1 = value(chunk[1]).ok_or(1usize)?;
    match (chunk[2], chunk[3]) {
        (b'=', b'=') => {
            if !final_quantum {
                return Err(2usize);
            }
            Ok((vec![(v0 << 2) | (v1 >> 4)], true))
        }
        (b'=', _) => Err(2usize),
        (_, b'=') => {
            if !final_quantum {
                return Err(3usize);
            }
            let v2 = value(chunk[2]).ok_or(2usize)?;
            Ok((
                vec![(v0 << 2) | (v1 >> 4), (v1 << 4) | (v2 >> 2)],
                true,
            ))
        }
        _ => {
            let v2 = value(chunk[2]).ok_or(2usize)?;
            let v3 = value(chunk[3]).ok_or(3usize)?;
            Ok((
                vec![
                    (v0 << 2) | (v1 >> 4),
                    (v1 << 4) | (v2 >> 2),
                    (v2 << 6) | v3,
                ],
                false,
            ))
        }
    }
}

fn is_url_safe_alphabet(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
}
