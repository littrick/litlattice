#![forbid(unsafe_code)]

//! XML models and serialization (`s3_types.go`).
//!
//! Go writes every response with `encoding/xml`: the documents are emitted
//! without pretty-printing, elements appear in struct-field order, and empty
//! strings/ints are written as `<Tag></Tag>` / `<Tag>0</Tag>` rather than being
//! omitted. The helpers here reproduce that byte-for-byte so S3 clients see the
//! same shapes as the Go baseline.

use std::time::SystemTime;

use time::OffsetDateTime;

use crate::backend::ObjectMeta;

/// `encoding/xml.Header`, including the trailing newline Go writes first.
pub const XML_HEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

/// Go `encoding/xml.EscapeText` for a `string`.
///
/// It escapes the five XML metacharacters plus tab/newline/carriage-return,
/// using the same numeric entity spellings as Go (`&#34;`, `&#x9;`, ...).
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            other => out.push(other),
        }
    }
    out
}

/// Go `time.Time.Format(time.RFC3339Nano)`.
///
/// The `time` crate's `Rfc3339` formatter is feature-gated (`formatting`) and
/// the blobstore crate does not enable it, so assemble the timestamp from the
/// component accessors. This mirrors Go: four-digit year, a `Z` for UTC, and a
/// fractional part that omits trailing zeros (and is absent at whole seconds).
pub fn format_time(time: SystemTime) -> String {
    let time = OffsetDateTime::from(time);
    let nanosecond = time.nanosecond();

    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second(),
    );

    if nanosecond != 0 {
        let mut fraction = format!("{nanosecond:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        out.push('.');
        out.push_str(&fraction);
    }

    let offset = time.offset().whole_seconds();
    if offset == 0 {
        out.push('Z');
    } else {
        let sign = if offset < 0 { '-' } else { '+' };
        let absolute = offset.unsigned_abs();
        out.push(sign);
        out.push_str(&format!(
            "{:02}:{:02}",
            absolute / 3600,
            (absolute % 3600) / 60
        ));
    }
    out
}

/// `writeS3Error`: three consecutive XML documents (`Error`, `Message`,
/// `BucketName`) after a single XML header.
pub fn error_documents(code: &str, message: &str, bucket: &str) -> String {
    let mut out = String::from(XML_HEADER);
    out.push_str("<Error><Code>");
    out.push_str(&escape(code));
    out.push_str("</Code></Error>");
    out.push_str("<Message>");
    out.push_str(&escape(message));
    out.push_str("</Message>");
    out.push_str("<BucketName>");
    out.push_str(&escape(bucket));
    out.push_str("</BucketName>");
    out
}

/// `<ListBucketResult>` (`s3ListBucketResult` / `s3Object`).
///
/// The Go handler only fills `Name`, `KeyCount`, and each object's `Key` and
/// `LastModified`; every other field is emitted as its zero value.
pub fn list_bucket_result(bucket: &str, objects: &[ObjectMeta]) -> String {
    let mut out = String::from(XML_HEADER);
    out.push_str("<ListBucketResult>");
    out.push_str("<IsTruncated>false</IsTruncated>");
    out.push_str("<Name>");
    out.push_str(&escape(bucket));
    out.push_str("</Name>");
    out.push_str("<Prefix></Prefix>");
    out.push_str("<Delimiter></Delimiter>");
    out.push_str("<MaxKeys>0</MaxKeys>");
    out.push_str("<KeyCount>");
    out.push_str(&objects.len().to_string());
    out.push_str("</KeyCount>");
    for object in objects {
        out.push_str("<Contents>");
        out.push_str("<Key>");
        out.push_str(&escape(&object.key));
        out.push_str("</Key>");
        out.push_str("<LastModified>");
        out.push_str(&escape(&format_time(object.last_modified)));
        out.push_str("</LastModified>");
        out.push_str("<Owner><DisplayName></DisplayName><ID></ID></Owner>");
        out.push_str("<Size>0</Size>");
        out.push_str("<StorageClass></StorageClass>");
        out.push_str("</Contents>");
    }
    out.push_str("<ContinuationToken></ContinuationToken>");
    out.push_str("<NextContinuationToken></NextContinuationToken>");
    out.push_str("<StartAfter></StartAfter>");
    out.push_str("</ListBucketResult>");
    out
}

/// `<InitiateMultipartUploadResult>` (`s3InitiateMultipartUploadResult`).
pub fn initiate_multipart_upload_result(bucket: &str, key: &str, upload_id: &str) -> String {
    let mut out = String::from(XML_HEADER);
    out.push_str("<InitiateMultipartUploadResult>");
    out.push_str("<Bucket>");
    out.push_str(&escape(bucket));
    out.push_str("</Bucket>");
    out.push_str("<Key>");
    out.push_str(&escape(key));
    out.push_str("</Key>");
    out.push_str("<UploadId>");
    out.push_str(&escape(upload_id));
    out.push_str("</UploadId>");
    out.push_str("</InitiateMultipartUploadResult>");
    out
}

/// `<CopyPartResult>` (`s3CopyPartResult`).
pub fn copy_part_result(last_modified: SystemTime) -> String {
    let mut out = String::from(XML_HEADER);
    out.push_str("<CopyPartResult>");
    out.push_str("<ETag></ETag>");
    out.push_str("<LastModified>");
    out.push_str(&escape(&format_time(last_modified)));
    out.push_str("</LastModified>");
    out.push_str("<ChecksumCRC32></ChecksumCRC32>");
    out.push_str("<ChecksumCRC32C></ChecksumCRC32C>");
    out.push_str("<ChecksumSHA1></ChecksumSHA1>");
    out.push_str("<ChecksumSHA256></ChecksumSHA256>");
    out.push_str("</CopyPartResult>");
    out
}

/// `<CompleteMultipartUploadResult>` (`s3CompleteMultipartUploadResult`).
pub fn complete_multipart_upload_result(bucket: &str, key: &str) -> String {
    let mut out = String::from(XML_HEADER);
    out.push_str("<CompleteMultipartUploadResult>");
    out.push_str("<Bucket>");
    out.push_str(&escape(bucket));
    out.push_str("</Bucket>");
    out.push_str("<Key>");
    out.push_str(&escape(key));
    out.push_str("</Key>");
    out.push_str("<ETag></ETag>");
    out.push_str("<ChecksumCRC32></ChecksumCRC32>");
    out.push_str("<ChecksumCRC32C></ChecksumCRC32C>");
    out.push_str("<ChecksumSHA1></ChecksumSHA1>");
    out.push_str("<ChecksumSHA256></ChecksumSHA256>");
    out.push_str("</CompleteMultipartUploadResult>");
    out
}

/// Parse a `POST /<bucket>?delete` request body (`s3DeleteObjectsRequest`).
///
/// Go decodes `<Delete><Object><Key>..</Key></Object>...</Delete>` and ignores
/// both `VersionId` and `Quiet`; only the keys are returned.
pub fn parse_delete_objects(body: &[u8]) -> Result<Vec<String>, String> {
    let text = std::str::from_utf8(body).map_err(|error| error.to_string())?;
    let mut reader = quick_xml::Reader::from_str(text);
    let mut keys = Vec::new();
    let mut in_key = false;
    let mut current = String::new();
    // Go's `xml.Decoder.Decode` rejects a root other than `<Delete>` and
    // reports unexpected EOF for unclosed elements; quick-xml's reader is
    // lower-level, so track the nesting ourselves.
    let mut depth = 0usize;

    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Start(start)) => {
                let name = start.local_name();
                if depth == 0 && name.as_ref() != b"Delete" {
                    return Err(format!(
                        "expected element type <Delete> but have <{}>",
                        String::from_utf8_lossy(name.as_ref())
                    ));
                }
                depth += 1;
                if name.as_ref() == b"Key" {
                    in_key = true;
                    current.clear();
                }
            }
            Ok(quick_xml::events::Event::Empty(empty)) => {
                if depth == 0 && empty.local_name().as_ref() != b"Delete" {
                    return Err("expected element type <Delete>".to_owned());
                }
                if empty.local_name().as_ref() == b"Key" && in_key {
                    keys.push(String::new());
                }
            }
            Ok(quick_xml::events::Event::End(end)) => {
                if depth == 0 {
                    return Err("unexpected end tag".to_owned());
                }
                if end.local_name().as_ref() == b"Key" && in_key {
                    in_key = false;
                    keys.push(std::mem::take(&mut current));
                }
                depth -= 1;
            }
            Ok(quick_xml::events::Event::Text(text)) if in_key => {
                let unescaped = text.unescape().map_err(|error| error.to_string())?;
                current.push_str(&unescaped);
            }
            Ok(quick_xml::events::Event::CData(data)) if in_key => {
                current.push_str(&String::from_utf8_lossy(&data));
            }
            Ok(quick_xml::events::Event::Eof) => {
                if depth != 0 {
                    return Err("unexpected EOF".to_owned());
                }
                break;
            }
            Ok(_) => {}
            Err(error) => return Err(error.to_string()),
        }
    }

    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn object(key: &str) -> ObjectMeta {
        ObjectMeta {
            key: key.to_owned(),
            size: 0,
            last_modified: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        }
    }

    #[test]
    fn escapes_like_encoding_xml() {
        assert_eq!(
            escape("<a>&\"'\t\n\r"),
            "&lt;a&gt;&amp;&#34;&#39;&#x9;&#xA;&#xD;"
        );
    }

    #[test]
    fn formats_rfc3339_nano_like_go() {
        assert_eq!(format_time(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_time(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            format_time(
                UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_millis(120)
            ),
            "2023-11-14T22:13:20.12Z"
        );
        assert_eq!(
            format_time(UNIX_EPOCH + Duration::from_nanos(1)),
            "1970-01-01T00:00:00.000000001Z"
        );
    }

    #[test]
    fn error_is_three_documents() {
        let body = error_documents("NoSuchBucket", "no such bucket", "my bucket");
        assert_eq!(
            body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>NoSuchBucket</Code></Error>\
             <Message>no such bucket</Message>\
             <BucketName>my bucket</BucketName>"
        );
    }

    #[test]
    fn list_result_matches_go_field_order_and_zero_values() {
        let body = list_bucket_result("bucket", &[object("a<b")]);
        assert_eq!(
            body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ListBucketResult>\
             <IsTruncated>false</IsTruncated>\
             <Name>bucket</Name>\
             <Prefix></Prefix>\
             <Delimiter></Delimiter>\
             <MaxKeys>0</MaxKeys>\
             <KeyCount>1</KeyCount>\
             <Contents>\
             <Key>a&lt;b</Key>\
             <LastModified>2023-11-14T22:13:20Z</LastModified>\
             <Owner><DisplayName></DisplayName><ID></ID></Owner>\
             <Size>0</Size>\
             <StorageClass></StorageClass>\
             </Contents>\
             <ContinuationToken></ContinuationToken>\
             <NextContinuationToken></NextContinuationToken>\
             <StartAfter></StartAfter>\
             </ListBucketResult>"
        );
    }

    #[test]
    fn list_result_empty_bucket() {
        let body = list_bucket_result("bucket", &[]);
        assert!(body.contains("<KeyCount>0</KeyCount>"));
        assert!(!body.contains("<Contents>"));
    }

    #[test]
    fn copy_part_result_order() {
        let body = copy_part_result(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        assert_eq!(
            body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <CopyPartResult>\
             <ETag></ETag>\
             <LastModified>2023-11-14T22:13:20Z</LastModified>\
             <ChecksumCRC32></ChecksumCRC32>\
             <ChecksumCRC32C></ChecksumCRC32C>\
             <ChecksumSHA1></ChecksumSHA1>\
             <ChecksumSHA256></ChecksumSHA256>\
             </CopyPartResult>"
        );
    }

    #[test]
    fn complete_result_order() {
        let body = complete_multipart_upload_result("bucket", "key");
        assert_eq!(
            body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <CompleteMultipartUploadResult>\
             <Bucket>bucket</Bucket>\
             <Key>key</Key>\
             <ETag></ETag>\
             <ChecksumCRC32></ChecksumCRC32>\
             <ChecksumCRC32C></ChecksumCRC32C>\
             <ChecksumSHA1></ChecksumSHA1>\
             <ChecksumSHA256></ChecksumSHA256>\
             </CompleteMultipartUploadResult>"
        );
    }

    #[test]
    fn initiate_result_order() {
        let body = initiate_multipart_upload_result("bucket", "key", "upload-id");
        assert_eq!(
            body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <InitiateMultipartUploadResult>\
             <Bucket>bucket</Bucket>\
             <Key>key</Key>\
             <UploadId>upload-id</UploadId>\
             </InitiateMultipartUploadResult>"
        );
    }

    #[test]
    fn parses_delete_objects() {
        let body = br#"<?xml version="1.0"?>
            <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Object><Key>one</Key></Object>
              <Object><Key>a&amp;b</Key><VersionId>ignored</VersionId></Object>
              <Quiet>true</Quiet>
            </Delete>"#;
        assert_eq!(
            parse_delete_objects(body).expect("valid XML"),
            vec!["one".to_owned(), "a&b".to_owned()]
        );
    }

    #[test]
    fn rejects_invalid_delete_xml() {
        assert!(parse_delete_objects(b"<Delete><Object>").is_err());
    }
}
