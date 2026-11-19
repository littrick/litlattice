#![forbid(unsafe_code)]

//! Path and file-name encoding compatible with Go's `net/url`.
//!
//! The Go implementation stores every object as a flat file whose name is
//! `url.QueryEscape(key)` (`blobstore.go`), and splits `URL.Path` with
//! `strings.FieldsFunc(path, '/')` after net/http has already percent-decoded
//! it.

use percent_encoding::percent_decode_str;

/// Go `url.QueryEscape`: unreserved bytes stay, space becomes `+`, everything
/// else is `%XX` (uppercase), byte by byte.
pub fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Go `url.QueryUnescape`: `+` becomes space, `%XX` decoded; invalid escapes
/// are left as-is (matching the Go error branch that still returns the partial
/// value).
pub fn query_unescape(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                match (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                    (Some(high), Some(low)) => {
                        out.push(high << 4 | low);
                        i += 3;
                    }
                    _ => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Flat storage name for an object key (`objectFileName`).
pub fn object_file_name(key: &str) -> String {
    query_escape(key)
}

/// Inverse of [`object_file_name`] (`fnameToObjectName`).
pub fn file_name_to_object_name(file_name: &str) -> String {
    query_unescape(file_name)
}

/// `strings.FieldsFunc(URL.Path, '/')`: net/http exposes the already
/// percent-decoded path, and empty segments are dropped.
pub fn split_path(path: &str) -> Vec<String> {
    let decoded = percent_decode_str(path).decode_utf8_lossy();
    decoded
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(
            query_escape("lsif-uploads/abc.dump"),
            "lsif-uploads%2Fabc.dump"
        );
        assert_eq!(query_escape("a b+c"), "a+b%2Bc");
        assert_eq!(query_escape("ümlaut"), "%C3%BCmlaut");
        assert_eq!(query_escape("Az09-_.~"), "Az09-_.~");
    }

    #[test]
    fn query_unescape_round_trips() {
        for value in ["lsif-uploads/abc.dump", "a b+c", "ümlaut", ""] {
            assert_eq!(query_unescape(&query_escape(value)), value);
        }
    }

    #[test]
    fn split_path_decodes_and_drops_empty_segments() {
        assert_eq!(split_path("/bucket/key"), vec!["bucket", "key"]);
        assert_eq!(split_path("/bucket//key/"), vec!["bucket", "key"]);
        assert_eq!(split_path("/bucket/a%20b"), vec!["bucket", "a b"]);
        assert!(split_path("/").is_empty());
    }
}
