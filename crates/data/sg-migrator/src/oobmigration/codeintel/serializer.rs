#![forbid(unsafe_code)]

//! Port of `internal/oobmigration/migrations/codeintel/lsif/serializer.go`.
//!
//! Every payload is `gzip(gob(value))`. Decoding accepts an empty byte slice
//! and leaves the destination at its zero value, matching Go's `decode`.

use super::gob::{decode, GobEncoder, GobType, GobValue, GobValueOut};
use super::gzip;
use super::types::{
    DiagnosticData, DocumentData, LocationData, MarshalledDocumentData, MonikerData,
    PackageInformationData, RangeData, ResultChunkData,
};

/// A serializer/codec error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CodecError {}

type Result<T> = std::result::Result<T, CodecError>;

fn codec(message: impl Into<String>) -> CodecError {
    CodecError(message.into())
}

fn decode_payload(data: &[u8]) -> Result<Option<GobValue>> {
    if data.is_empty() {
        return Ok(None);
    }
    let raw = gzip::decompress(data).map_err(|error| codec(error.to_string()))?;
    decode(&raw)
        .map(Some)
        .map_err(|error| codec(error.to_string()))
}

fn encode_payload(ty: &GobType, value: &GobValueOut) -> Result<Vec<u8>> {
    let raw = GobEncoder::new()
        .encode(ty, value)
        .map_err(|error| codec(error.to_string()))?;
    Ok(gzip::compress(&raw))
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// `UnmarshalLegacyDocumentData`.
pub fn unmarshal_legacy_document_data(data: &[u8]) -> Result<DocumentData> {
    match decode_payload(data)? {
        Some(value) => DocumentData::from_gob(&value).map_err(|error| codec(error.to_string())),
        None => Ok(DocumentData::default()),
    }
}

/// `UnmarshalDocumentData`.
pub fn unmarshal_document_data(data: &MarshalledDocumentData) -> Result<DocumentData> {
    let ranges = decode_payload(&data.ranges)?;
    let hover_results = decode_payload(&data.hover_results)?;
    let monikers = decode_payload(&data.monikers)?;
    let package_information = decode_payload(&data.package_information)?;
    let diagnostics = decode_payload(&data.diagnostics)?;

    super::types::document_from_columns(
        ranges.as_ref(),
        hover_results.as_ref(),
        monikers.as_ref(),
        package_information.as_ref(),
        diagnostics.as_ref(),
    )
    .map_err(|error| codec(error.to_string()))
}

/// `UnmarshalLocations`.
pub fn unmarshal_locations(data: &[u8]) -> Result<Vec<LocationData>> {
    match decode_payload(data)? {
        Some(value) => {
            super::types::locations_from_gob(&value).map_err(|error| codec(error.to_string()))
        }
        None => Ok(Vec::new()),
    }
}

/// `UnmarshalResultChunkData`.
pub fn unmarshal_result_chunk_data(data: &[u8]) -> Result<ResultChunkData> {
    match decode_payload(data)? {
        Some(value) => ResultChunkData::from_gob(&value).map_err(|error| codec(error.to_string())),
        None => Ok(ResultChunkData::default()),
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// `MarshalLegacyDocumentData`.
pub fn marshal_legacy_document_data(document: &DocumentData) -> Result<Vec<u8>> {
    encode_payload(&document_type(), &document_value(document))
}

/// `MarshalDocumentData`.
pub fn marshal_document_data(document: &DocumentData) -> Result<MarshalledDocumentData> {
    Ok(MarshalledDocumentData {
        ranges: encode_payload(&ranges_type(), &ranges_value(document))?,
        hover_results: encode_payload(
            &string_map_type(),
            &map_value(&document.hover_results, |value| {
                GobValueOut::String(value.clone())
            }),
        )?,
        monikers: encode_payload(
            &monikers_type(),
            &map_value(&document.monikers, moniker_value),
        )?,
        package_information: encode_payload(
            &package_information_type_map(),
            &map_value(&document.package_information, package_information_value),
        )?,
        diagnostics: encode_payload(
            &diagnostics_type(),
            &GobValueOut::Slice(document.diagnostics.iter().map(diagnostic_value).collect()),
        )?,
    })
}

fn map_value<T>(
    map: &std::collections::BTreeMap<String, T>,
    convert: impl Fn(&T) -> GobValueOut,
) -> GobValueOut {
    GobValueOut::Map(
        map.iter()
            .map(|(key, value)| (GobValueOut::String(key.clone()), convert(value)))
            .collect(),
    )
}

fn range_type() -> GobType {
    GobType::Struct {
        name: "RangeData".to_string(),
        fields: vec![
            ("StartLine".to_string(), GobType::Int),
            ("StartCharacter".to_string(), GobType::Int),
            ("EndLine".to_string(), GobType::Int),
            ("EndCharacter".to_string(), GobType::Int),
            ("DefinitionResultID".to_string(), GobType::String),
            ("ReferenceResultID".to_string(), GobType::String),
            ("ImplementationResultID".to_string(), GobType::String),
            ("HoverResultID".to_string(), GobType::String),
            (
                "MonikerIDs".to_string(),
                GobType::Slice(Box::new(GobType::String)),
            ),
        ],
    }
}

fn moniker_type() -> GobType {
    GobType::Struct {
        name: "MonikerData".to_string(),
        fields: vec![
            ("Kind".to_string(), GobType::String),
            ("Scheme".to_string(), GobType::String),
            ("Identifier".to_string(), GobType::String),
            ("PackageInformationID".to_string(), GobType::String),
        ],
    }
}

fn package_information_type() -> GobType {
    GobType::Struct {
        name: "PackageInformationData".to_string(),
        fields: vec![
            ("Manager".to_string(), GobType::String),
            ("Name".to_string(), GobType::String),
            ("Version".to_string(), GobType::String),
        ],
    }
}

fn diagnostic_type() -> GobType {
    GobType::Struct {
        name: "DiagnosticData".to_string(),
        fields: vec![
            ("Severity".to_string(), GobType::Int),
            ("Code".to_string(), GobType::String),
            ("Message".to_string(), GobType::String),
            ("Source".to_string(), GobType::String),
            ("StartLine".to_string(), GobType::Int),
            ("StartCharacter".to_string(), GobType::Int),
            ("EndLine".to_string(), GobType::Int),
            ("EndCharacter".to_string(), GobType::Int),
        ],
    }
}

fn ranges_type() -> GobType {
    GobType::Map(Box::new(GobType::String), Box::new(range_type()))
}

fn string_map_type() -> GobType {
    GobType::Map(Box::new(GobType::String), Box::new(GobType::String))
}

fn monikers_type() -> GobType {
    GobType::Map(Box::new(GobType::String), Box::new(moniker_type()))
}

fn package_information_type_map() -> GobType {
    GobType::Map(
        Box::new(GobType::String),
        Box::new(package_information_type()),
    )
}

fn diagnostics_type() -> GobType {
    GobType::Slice(Box::new(diagnostic_type()))
}

fn document_type() -> GobType {
    GobType::Struct {
        name: "DocumentData".to_string(),
        fields: vec![
            ("Ranges".to_string(), ranges_type()),
            ("HoverResults".to_string(), string_map_type()),
            ("Monikers".to_string(), monikers_type()),
            (
                "PackageInformation".to_string(),
                package_information_type_map(),
            ),
            ("Diagnostics".to_string(), diagnostics_type()),
        ],
    }
}

fn range_value(range: &RangeData) -> GobValueOut {
    GobValueOut::Struct(vec![
        (
            "StartLine".to_string(),
            GobValueOut::Int(i64::from(range.start_line)),
        ),
        (
            "StartCharacter".to_string(),
            GobValueOut::Int(i64::from(range.start_character)),
        ),
        (
            "EndLine".to_string(),
            GobValueOut::Int(i64::from(range.end_line)),
        ),
        (
            "EndCharacter".to_string(),
            GobValueOut::Int(i64::from(range.end_character)),
        ),
        (
            "DefinitionResultID".to_string(),
            GobValueOut::String(range.definition_result_id.clone()),
        ),
        (
            "ReferenceResultID".to_string(),
            GobValueOut::String(range.reference_result_id.clone()),
        ),
        (
            "ImplementationResultID".to_string(),
            GobValueOut::String(range.implementation_result_id.clone()),
        ),
        (
            "HoverResultID".to_string(),
            GobValueOut::String(range.hover_result_id.clone()),
        ),
        (
            "MonikerIDs".to_string(),
            GobValueOut::Slice(
                range
                    .moniker_ids
                    .iter()
                    .map(|id| GobValueOut::String(id.clone()))
                    .collect(),
            ),
        ),
    ])
}

fn moniker_value(moniker: &MonikerData) -> GobValueOut {
    GobValueOut::Struct(vec![
        (
            "Kind".to_string(),
            GobValueOut::String(moniker.kind.clone()),
        ),
        (
            "Scheme".to_string(),
            GobValueOut::String(moniker.scheme.clone()),
        ),
        (
            "Identifier".to_string(),
            GobValueOut::String(moniker.identifier.clone()),
        ),
        (
            "PackageInformationID".to_string(),
            GobValueOut::String(moniker.package_information_id.clone()),
        ),
    ])
}

fn package_information_value(info: &PackageInformationData) -> GobValueOut {
    GobValueOut::Struct(vec![
        (
            "Manager".to_string(),
            GobValueOut::String(info.manager.clone()),
        ),
        ("Name".to_string(), GobValueOut::String(info.name.clone())),
        (
            "Version".to_string(),
            GobValueOut::String(info.version.clone()),
        ),
    ])
}

fn diagnostic_value(diagnostic: &DiagnosticData) -> GobValueOut {
    GobValueOut::Struct(vec![
        (
            "Severity".to_string(),
            GobValueOut::Int(i64::from(diagnostic.severity)),
        ),
        (
            "Code".to_string(),
            GobValueOut::String(diagnostic.code.clone()),
        ),
        (
            "Message".to_string(),
            GobValueOut::String(diagnostic.message.clone()),
        ),
        (
            "Source".to_string(),
            GobValueOut::String(diagnostic.source.clone()),
        ),
        (
            "StartLine".to_string(),
            GobValueOut::Int(i64::from(diagnostic.start_line)),
        ),
        (
            "StartCharacter".to_string(),
            GobValueOut::Int(i64::from(diagnostic.start_character)),
        ),
        (
            "EndLine".to_string(),
            GobValueOut::Int(i64::from(diagnostic.end_line)),
        ),
        (
            "EndCharacter".to_string(),
            GobValueOut::Int(i64::from(diagnostic.end_character)),
        ),
    ])
}

fn ranges_value(document: &DocumentData) -> GobValueOut {
    map_value(&document.ranges, range_value)
}

fn document_value(document: &DocumentData) -> GobValueOut {
    GobValueOut::Struct(vec![
        ("Ranges".to_string(), ranges_value(document)),
        (
            "HoverResults".to_string(),
            map_value(&document.hover_results, |value| {
                GobValueOut::String(value.clone())
            }),
        ),
        (
            "Monikers".to_string(),
            map_value(&document.monikers, moniker_value),
        ),
        (
            "PackageInformation".to_string(),
            map_value(&document.package_information, package_information_value),
        ),
        (
            "Diagnostics".to_string(),
            GobValueOut::Slice(document.diagnostics.iter().map(diagnostic_value).collect()),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn hex(value: &str) -> Vec<u8> {
        let value: String = value.chars().filter(|c| !c.is_whitespace()).collect();
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
            .collect()
    }

    fn sample_document() -> DocumentData {
        let mut document = DocumentData::default();
        document.ranges.insert(
            "7864".to_string(),
            RangeData {
                start_line: 541,
                start_character: 10,
                end_line: 541,
                end_character: 12,
                definition_result_id: "1266".to_string(),
                reference_result_id: "15871".to_string(),
                implementation_result_id: String::new(),
                hover_result_id: "1269".to_string(),
                moniker_ids: vec!["314".to_string()],
            },
        );
        document
            .hover_results
            .insert("1269".to_string(), "`hover`".to_string());
        document.monikers.insert(
            "314".to_string(),
            MonikerData {
                kind: "export".to_string(),
                scheme: "gomod".to_string(),
                identifier: "pkg:Vertex".to_string(),
                package_information_id: "213".to_string(),
            },
        );
        document.package_information.insert(
            "213".to_string(),
            PackageInformationData {
                manager: String::new(),
                name: "github.com/sourcegraph/lsif-go".to_string(),
                version: "v0.0.0".to_string(),
            },
        );
        document.diagnostics.push(DiagnosticData {
            severity: 1,
            code: "c1".to_string(),
            message: "m1".to_string(),
            source: "s".to_string(),
            start_line: 1,
            start_character: 2,
            end_line: 1,
            end_character: 5,
        });
        document
    }

    #[test]
    fn legacy_document_round_trips() {
        let document = sample_document();
        let encoded = marshal_legacy_document_data(&document).unwrap();
        let decoded = unmarshal_legacy_document_data(&encoded).unwrap();
        assert_eq!(decoded, document);
    }

    #[test]
    fn split_columns_round_trip() {
        let document = sample_document();
        let columns = marshal_document_data(&document).unwrap();
        let decoded = unmarshal_document_data(&columns).unwrap();
        assert_eq!(decoded, document);
    }

    #[test]
    fn decodes_go_legacy_document_fixture() {
        // Go 1.22 encoding of the same DocumentData shape used by the baseline
        // serializer tests (gzip -> gob).
        let compressed = hex(include_str!("testdata/legacy_document.hex"));

        let document = unmarshal_legacy_document_data(&compressed).unwrap();
        assert_eq!(document.ranges.len(), 2);
        assert_eq!(document.ranges.get("7864").unwrap().start_line, 541);
        assert_eq!(document.hover_results.len(), 2);
        assert_eq!(document.monikers.len(), 2);
        assert_eq!(document.package_information.len(), 1);
        assert_eq!(document.diagnostics.len(), 1);
    }

    #[test]
    fn decodes_go_locations_fixture() {
        // Go 1.22 encoding of a two-element `[]LocationData`.
        let compressed = hex(include_str!("testdata/locations.hex"));

        let locations = unmarshal_locations(&compressed).unwrap();
        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].uri, "internal/index/indexer.go");
        assert_eq!(locations[1].end_character, 15);
    }

    #[test]
    fn decodes_go_result_chunk_fixture() {
        // Go 1.22 encoding of a `ResultChunkData` with two documents.
        let compressed = hex(include_str!("testdata/result_chunk.hex"));

        let chunk = unmarshal_result_chunk_data(&compressed).unwrap();
        assert_eq!(chunk.document_paths.len(), 2);
        assert_eq!(chunk.document_id_range_ids.len(), 2);
        assert_eq!(chunk.document_id_range_ids.get("1266").unwrap().len(), 2);
    }

    #[test]
    fn empty_payloads_decode_to_defaults() {
        assert_eq!(
            unmarshal_legacy_document_data(&[]).unwrap(),
            DocumentData::default()
        );
        assert!(unmarshal_locations(&[]).unwrap().is_empty());
        assert_eq!(
            unmarshal_result_chunk_data(&[]).unwrap(),
            ResultChunkData::default()
        );
    }

    #[test]
    fn split_columns_from_go_fixture() {
        // The five independently-encoded columns of the same document.
        let columns = MarshalledDocumentData {
            ranges: hex(include_str!("testdata/ranges.hex")),
            hover_results: hex(include_str!("testdata/hovers.hex")),
            monikers: hex(include_str!("testdata/monikers.hex")),
            package_information: hex(include_str!("testdata/packages.hex")),
            diagnostics: hex(include_str!("testdata/diagnostics.hex")),
        };

        let document = unmarshal_document_data(&columns).unwrap();
        assert_eq!(document.ranges.len(), 2);
        assert_eq!(document.hover_results.len(), 2);
        assert_eq!(document.monikers.len(), 2);
        assert_eq!(document.package_information.len(), 1);
        assert_eq!(document.diagnostics.len(), 1);
        // Confirm a nested field survives the column split.
        assert_eq!(
            document.package_information.get("213").unwrap().version,
            "v0.0.0-ad3507cbeb18"
        );
    }

    #[test]
    fn encoders_are_deterministic_for_same_map() {
        let document = sample_document();
        let first = marshal_document_data(&document).unwrap();
        let second = marshal_document_data(&document).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn map_value_preserves_all_entries() {
        let mut map = BTreeMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        let value = map_value(&map, |value| GobValueOut::Int(i64::from(*value)));
        match value {
            GobValueOut::Map(entries) => assert_eq!(entries.len(), 2),
            _ => panic!("expected map"),
        }
    }
}
