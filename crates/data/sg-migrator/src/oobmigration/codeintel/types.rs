#![forbid(unsafe_code)]

//! The LSIF payload types from `internal/oobmigration/migrations/codeintel/lsif/types.go`
//! and their conversion from decoded gob values.
//!
//! Go encodes these as gob structs with exported field names; missing fields
//! (zero values) are omitted from the stream, so the readers default.

use std::collections::BTreeMap;

use super::gob::{GobError, GobValue};

pub type Id = String;

/// `DocumentData`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DocumentData {
    pub ranges: BTreeMap<Id, RangeData>,
    pub hover_results: BTreeMap<Id, String>,
    pub monikers: BTreeMap<Id, MonikerData>,
    pub package_information: BTreeMap<Id, PackageInformationData>,
    pub diagnostics: Vec<DiagnosticData>,
}

/// `RangeData`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeData {
    pub start_line: i32,
    pub start_character: i32,
    pub end_line: i32,
    pub end_character: i32,
    pub definition_result_id: Id,
    pub reference_result_id: Id,
    pub implementation_result_id: Id,
    pub hover_result_id: Id,
    pub moniker_ids: Vec<Id>,
}

/// `MonikerData`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MonikerData {
    pub kind: String,
    pub scheme: String,
    pub identifier: String,
    pub package_information_id: Id,
}

/// `PackageInformationData`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageInformationData {
    pub manager: String,
    pub name: String,
    pub version: String,
}

/// `DiagnosticData`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticData {
    pub severity: i32,
    pub code: String,
    pub message: String,
    pub source: String,
    pub start_line: i32,
    pub start_character: i32,
    pub end_line: i32,
    pub end_character: i32,
}

/// `LocationData`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocationData {
    pub uri: String,
    pub start_line: i32,
    pub start_character: i32,
    pub end_line: i32,
    pub end_character: i32,
}

/// `DocumentIDRangeID`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentIdRangeId {
    pub document_id: Id,
    pub range_id: Id,
}

/// `ResultChunkData`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultChunkData {
    pub document_paths: BTreeMap<Id, String>,
    pub document_id_range_ids: BTreeMap<Id, Vec<DocumentIdRangeId>>,
}

/// `MarshalledDocumentData`: one gzip(gob(...)) payload per column.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MarshalledDocumentData {
    pub ranges: Vec<u8>,
    pub hover_results: Vec<u8>,
    pub monikers: Vec<u8>,
    pub package_information: Vec<u8>,
    pub diagnostics: Vec<u8>,
}

fn bad(message: impl Into<String>) -> GobError {
    GobError::BadValue(message.into())
}

fn int(value: &GobValue) -> Result<i32, GobError> {
    value.as_int().map(|value| value as i32)
}

fn string(value: &GobValue) -> Result<String, GobError> {
    value.as_string().map(str::to_owned)
}

fn string_slice(value: &GobValue) -> Result<Vec<String>, GobError> {
    value
        .as_slice()?
        .iter()
        .map(string)
        .collect::<Result<Vec<_>, _>>()
}

fn field_string(value: &GobValue, name: &str) -> Result<String, GobError> {
    match value.field(name) {
        Some(field) => string(field),
        None => Ok(String::new()),
    }
}

fn field_int(value: &GobValue, name: &str) -> Result<i32, GobError> {
    match value.field(name) {
        Some(field) => int(field),
        None => Ok(0),
    }
}

fn struct_fields(value: &GobValue) -> Result<&[(String, GobValue)], GobError> {
    match value {
        GobValue::Struct(fields) => Ok(fields),
        _ => Err(bad("expected gob struct")),
    }
}

impl RangeData {
    fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        let moniker_ids = match value.field("MonikerIDs") {
            Some(field) => string_slice(field)?,
            None => Vec::new(),
        };
        Ok(Self {
            start_line: field_int(value, "StartLine")?,
            start_character: field_int(value, "StartCharacter")?,
            end_line: field_int(value, "EndLine")?,
            end_character: field_int(value, "EndCharacter")?,
            definition_result_id: field_string(value, "DefinitionResultID")?,
            reference_result_id: field_string(value, "ReferenceResultID")?,
            implementation_result_id: field_string(value, "ImplementationResultID")?,
            hover_result_id: field_string(value, "HoverResultID")?,
            moniker_ids,
        })
    }
}

impl MonikerData {
    fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        Ok(Self {
            kind: field_string(value, "Kind")?,
            scheme: field_string(value, "Scheme")?,
            identifier: field_string(value, "Identifier")?,
            package_information_id: field_string(value, "PackageInformationID")?,
        })
    }
}

impl PackageInformationData {
    fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        Ok(Self {
            manager: field_string(value, "Manager")?,
            name: field_string(value, "Name")?,
            version: field_string(value, "Version")?,
        })
    }
}

impl DiagnosticData {
    fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        Ok(Self {
            severity: field_int(value, "Severity")?,
            code: field_string(value, "Code")?,
            message: field_string(value, "Message")?,
            source: field_string(value, "Source")?,
            start_line: field_int(value, "StartLine")?,
            start_character: field_int(value, "StartCharacter")?,
            end_line: field_int(value, "EndLine")?,
            end_character: field_int(value, "EndCharacter")?,
        })
    }
}

impl LocationData {
    fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        Ok(Self {
            uri: field_string(value, "URI")?,
            start_line: field_int(value, "StartLine")?,
            start_character: field_int(value, "StartCharacter")?,
            end_line: field_int(value, "EndLine")?,
            end_character: field_int(value, "EndCharacter")?,
        })
    }
}

impl DocumentIdRangeId {
    fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        Ok(Self {
            document_id: field_string(value, "DocumentID")?,
            range_id: field_string(value, "RangeID")?,
        })
    }
}

fn map_of<T>(
    value: &GobValue,
    convert: impl Fn(&GobValue) -> Result<T, GobError>,
) -> Result<BTreeMap<Id, T>, GobError> {
    let entries = value.as_map()?;
    let mut out = BTreeMap::new();
    for (key, entry) in entries {
        let key = string(key)?;
        out.insert(key, convert(entry)?);
    }
    Ok(out)
}

impl DocumentData {
    /// `UnmarshalLegacyDocumentData` / `UnmarshalDocumentData`.
    pub fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        let ranges = match value.field("Ranges") {
            Some(field) => map_of(field, RangeData::from_gob)?,
            None => BTreeMap::new(),
        };
        let hover_results = match value.field("HoverResults") {
            Some(field) => map_of(field, string)?,
            None => BTreeMap::new(),
        };
        let monikers = match value.field("Monikers") {
            Some(field) => map_of(field, MonikerData::from_gob)?,
            None => BTreeMap::new(),
        };
        let package_information = match value.field("PackageInformation") {
            Some(field) => map_of(field, PackageInformationData::from_gob)?,
            None => BTreeMap::new(),
        };
        let diagnostics = match value.field("Diagnostics") {
            Some(field) => field
                .as_slice()?
                .iter()
                .map(DiagnosticData::from_gob)
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };

        Ok(Self {
            ranges,
            hover_results,
            monikers,
            package_information,
            diagnostics,
        })
    }
}

/// Decode the map-only pieces of `MarshalledDocumentData`.
pub fn document_from_columns(
    ranges: Option<&GobValue>,
    hover_results: Option<&GobValue>,
    monikers: Option<&GobValue>,
    package_information: Option<&GobValue>,
    diagnostics: Option<&GobValue>,
) -> Result<DocumentData, GobError> {
    let ranges = match ranges {
        Some(value) => map_of(value, RangeData::from_gob)?,
        None => BTreeMap::new(),
    };
    let hover_results = match hover_results {
        Some(value) => map_of(value, string)?,
        None => BTreeMap::new(),
    };
    let monikers = match monikers {
        Some(value) => map_of(value, MonikerData::from_gob)?,
        None => BTreeMap::new(),
    };
    let package_information = match package_information {
        Some(value) => map_of(value, PackageInformationData::from_gob)?,
        None => BTreeMap::new(),
    };
    let diagnostics = match diagnostics {
        Some(value) => value
            .as_slice()?
            .iter()
            .map(DiagnosticData::from_gob)
            .collect::<Result<Vec<_>, _>>()?,
        None => Vec::new(),
    };

    Ok(DocumentData {
        ranges,
        hover_results,
        monikers,
        package_information,
        diagnostics,
    })
}

impl ResultChunkData {
    pub fn from_gob(value: &GobValue) -> Result<Self, GobError> {
        struct_fields(value)?;
        let document_paths = match value.field("DocumentPaths") {
            Some(field) => map_of(field, string)?,
            None => BTreeMap::new(),
        };
        let document_id_range_ids = match value.field("DocumentIDRangeIDs") {
            Some(field) => map_of(field, |entry| {
                entry
                    .as_slice()?
                    .iter()
                    .map(DocumentIdRangeId::from_gob)
                    .collect::<Result<Vec<_>, _>>()
            })?,
            None => BTreeMap::new(),
        };
        Ok(Self {
            document_paths,
            document_id_range_ids,
        })
    }
}

/// Decode a `[]LocationData` payload.
pub fn locations_from_gob(value: &GobValue) -> Result<Vec<LocationData>, GobError> {
    value
        .as_slice()?
        .iter()
        .map(LocationData::from_gob)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_document_from_decoded_struct() {
        let value = GobValue::Struct(vec![(
            "Diagnostics".to_string(),
            GobValue::Slice(vec![GobValue::Struct(vec![
                ("Severity".to_string(), GobValue::Int(2)),
                ("Code".to_string(), GobValue::String("c1".to_string())),
            ])]),
        )]);

        let document = DocumentData::from_gob(&value).unwrap();
        assert_eq!(document.diagnostics.len(), 1);
        assert_eq!(document.diagnostics[0].severity, 2);
        assert_eq!(document.diagnostics[0].code, "c1");
        assert!(document.ranges.is_empty());
    }
}
