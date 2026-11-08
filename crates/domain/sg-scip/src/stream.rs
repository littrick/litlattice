#![forbid(unsafe_code)]

//! Port of `parse.go`: `IndexVisitor.ParseStreaming`.
//!
//! The baseline parses the `Index` message field-by-field so a large upload
//! never has to be fully materialized. `Index.metadata` must appear first, and
//! only `metadata`, `documents` and `external_symbols` are recognized; any
//! other top-level field is a hard error (the upstream parser insists the
//! visitor be updated when the schema grows).

use std::fmt;
use std::io::{self, Read};

use prost::Message;

use crate::scip::{Document, Metadata, SymbolInformation};

/// `scip.IndexVisitor`: callbacks for the streamed top-level fields.
pub trait IndexVisitor {
    fn visit_metadata(&mut self, _metadata: Metadata) {}
    fn visit_document(&mut self, _document: Document) {}
    fn visit_external_symbol(&mut self, _symbol: SymbolInformation) {}
}

/// A streaming parse failure. The message mirrors the baseline's `errors.Wrap`
/// output where practical; only success/failure is compared in the
/// differential harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseError {}

const METADATA_FIELD_NUMBER: u32 = 1;
const DOCUMENTS_FIELD_NUMBER: u32 = 2;
const EXTERNAL_SYMBOLS_FIELD_NUMBER: u32 = 3;

const MAX_VARINT_BYTES: usize = 10;

/// One top-level `Index` field, already decoded.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexEvent {
    Metadata(Metadata),
    Document(Document),
    ExternalSymbol(SymbolInformation),
}

/// Pull-based counterpart of [`IndexVisitor`]: yields one decoded top-level
/// field per call. The code-graph writer needs this because its per-document
/// callback awaits database writes, which the visitor pattern cannot express.
pub struct StreamingReader<R> {
    reader: R,
    data: Vec<u8>,
}

impl<R: Read> StreamingReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            data: Vec::new(),
        }
    }

    /// Read the next top-level field, or `None` at a clean EOF.
    pub fn next_event(&mut self) -> Result<Option<IndexEvent>, ParseError> {
        let Some(tag) = read_one(&mut self.reader)
            .map_err(|err| ParseError::new(format!("failed to read from index reader: {err}")))?
        else {
            return Ok(None);
        };

        // `protowire.ConsumeTag` over a single byte rejects continuation bits
        // (the Index message has fewer than 15 fields, so every tag fits in
        // one byte).
        if tag & 0x80 != 0 {
            return Err(ParseError::new("failed to consume tag: invalid tag"));
        }
        let field_number = u32::from(tag >> 3);
        let wire_type = tag & 0x7;

        match field_number {
            METADATA_FIELD_NUMBER | DOCUMENTS_FIELD_NUMBER | EXTERNAL_SYMBOLS_FIELD_NUMBER => {
                if wire_type != 2 {
                    return Err(ParseError::new(format!(
                        "expected LEN type tag for {}",
                        field_name(field_number)
                    )));
                }

                let data_len = read_varint(&mut self.reader).map_err(|err| {
                    ParseError::new(format!(
                        "failed to read length for {}: {err}",
                        field_name(field_number)
                    ))
                })? as usize;

                self.data.resize(data_len, 0);
                if data_len > 0 {
                    self.reader.read_exact(&mut self.data).map_err(|err| {
                        ParseError::new(format!(
                            "expected to read {data_len} bytes based on LEN: {err}"
                        ))
                    })?;
                }

                let event = match field_number {
                    METADATA_FIELD_NUMBER => Metadata::decode(self.data.as_slice())
                        .map(IndexEvent::Metadata)
                        .map_err(|err| {
                            ParseError::new(format!("failed to read metadata: {err}"))
                        })?,
                    DOCUMENTS_FIELD_NUMBER => Document::decode(self.data.as_slice())
                        .map(IndexEvent::Document)
                        .map_err(|err| {
                            ParseError::new(format!("failed to read documents: {err}"))
                        })?,
                    _ => SymbolInformation::decode(self.data.as_slice())
                        .map(IndexEvent::ExternalSymbol)
                        .map_err(|err| {
                            ParseError::new(format!("failed to read external_symbols: {err}"))
                        })?,
                };
                Ok(Some(event))
            }
            other => Err(ParseError::new(format!(
                "added new field with number: {other} in scip.Index but forgot to update streaming parser"
            ))),
        }
    }
}

impl<R> StreamingReader<R> {
    /// Consume the reader, returning the underlying stream.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

/// `IndexVisitor.ParseStreaming`.
pub fn parse_streaming<R: Read>(
    reader: &mut R,
    visitor: &mut impl IndexVisitor,
) -> Result<(), ParseError> {
    let mut stream = StreamingReader::new(reader);
    while let Some(event) = stream.next_event()? {
        match event {
            IndexEvent::Metadata(metadata) => visitor.visit_metadata(metadata),
            IndexEvent::Document(document) => visitor.visit_document(document),
            IndexEvent::ExternalSymbol(symbol) => visitor.visit_external_symbol(symbol),
        }
    }
    Ok(())
}

fn field_name(field_number: u32) -> &'static str {
    match field_number {
        METADATA_FIELD_NUMBER => "metadata",
        DOCUMENTS_FIELD_NUMBER => "documents",
        EXTERNAL_SYMBOLS_FIELD_NUMBER => "external_symbols",
        _ => "<unknown>",
    }
}

/// Read exactly one byte, or `None` at a clean EOF.
fn read_one<R: Read>(reader: &mut R) -> io::Result<Option<u8>> {
    let mut buffer = [0u8; 1];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(Some(buffer[0])),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
}

/// `readVarint`: at most ten bytes, little-endian groups of seven bits.
fn read_varint<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;

    for index in 0..MAX_VARINT_BYTES {
        let byte = read_one(reader)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("failed to read {index}-th byte of Varint"),
            )
        })?;

        value |= u64::from(byte & 0x7f) << shift;
        if byte <= 127 {
            return Ok(value);
        }
        shift += 7;
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "varint overflow",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scip::{Index, ToolInfo};

    #[derive(Default)]
    struct Collector {
        metadata: Option<Metadata>,
        documents: Vec<Document>,
        external_symbols: Vec<SymbolInformation>,
    }

    impl IndexVisitor for Collector {
        fn visit_metadata(&mut self, metadata: Metadata) {
            self.metadata = Some(metadata);
        }

        fn visit_document(&mut self, document: Document) {
            self.documents.push(document);
        }

        fn visit_external_symbol(&mut self, symbol: SymbolInformation) {
            self.external_symbols.push(symbol);
        }
    }

    fn index_bytes(index: &Index) -> Vec<u8> {
        index.encode_to_vec()
    }

    #[test]
    fn streams_top_level_fields_in_order() {
        let index = Index {
            metadata: Some(Metadata {
                version: 0,
                tool_info: Some(ToolInfo {
                    name: "tool".to_owned(),
                    version: "1".to_owned(),
                    arguments: vec!["-a".to_owned()],
                }),
                project_root: String::new(),
                text_document_encoding: 1,
            }),
            documents: vec![
                Document {
                    relative_path: "a.go".to_owned(),
                    ..Document::default()
                },
                Document {
                    relative_path: "b.go".to_owned(),
                    ..Document::default()
                },
            ],
            external_symbols: vec![SymbolInformation {
                symbol: "s m n 1 A#".to_owned(),
                ..SymbolInformation::default()
            }],
        };

        let mut visitor = Collector::default();
        parse_streaming(&mut index_bytes(&index).as_slice(), &mut visitor).expect("parse");

        assert_eq!(
            visitor
                .metadata
                .expect("metadata")
                .tool_info
                .expect("tool")
                .name,
            "tool"
        );
        assert_eq!(visitor.documents.len(), 2);
        assert_eq!(visitor.documents[1].relative_path, "b.go");
        assert_eq!(visitor.external_symbols.len(), 1);
    }

    #[test]
    fn unknown_top_level_field_is_an_error() {
        // field 5, LEN type: tag = (5 << 3) | 2 = 0x2a.
        let mut visitor = Collector::default();
        let err = parse_streaming(&mut [0x2au8, 0x00].as_slice(), &mut visitor).expect_err("error");
        assert!(err.message.contains("added new field with number: 5"));
    }
}
