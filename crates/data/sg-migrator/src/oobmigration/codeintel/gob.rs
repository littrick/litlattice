#![forbid(unsafe_code)]

//! Minimal `encoding/gob` codec for the LSIF payloads read by the
//! code-intelligence migrations.
//!
//! The decoder is adapted from `crates/domain/sg-session/src/gob.rs` (same
//! wire-format coverage: bootstrap types, type definitions, structs, maps,
//! slices and interfaces). The encoder is new and only supports the shapes
//! these migrations write: the LSIF document structs and their field maps and
//! slices. It produces a standard gob stream that Go's `encoding/gob` decoder
//! accepts; it is not byte-identical to Go's encoder (field order within maps
//! and omission of zero fields differ), which does not matter for the
//! migrations.
//!
//! Wire format notes (integers are gob uints: a byte `<= 0x7f` is the value,
//! otherwise the first byte is the negated big-endian byte count):
//!
//! * stream = delimited messages, each `uint(len) body`;
//! * a body is `int(-typeId) wireType` (type definition) or `int(typeId) value`;
//! * a top-level non-struct value is preceded by a zero field delta;
//! * a struct is a sequence of `uint(fieldDelta) value` terminated by a zero
//!   delta; structs nested inside other values have no leading zero delta;
//! * a map is `uint(count)` then key/value pairs, a slice `uint(count)` then
//!   elements.

use std::fmt;

// Builtin type ids (encoding/gob bootstrap types).
pub const T_BOOL: i64 = 1;
pub const T_INT: i64 = 2;
pub const T_UINT: i64 = 3;
pub const T_FLOAT: i64 = 4;
pub const T_BYTES: i64 = 5;
pub const T_STRING: i64 = 6;
pub const T_COMPLEX: i64 = 7;
pub const T_INTERFACE: i64 = 8;

/// First type id handed out to user-defined types by a fresh encoder.
const FIRST_USER_ID: i64 = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GobError {
    Truncated,
    BadUint,
    BadValue(String),
    UnknownType(i64),
    TrailingBytes,
    LegacyDelimitedType,
}

impl fmt::Display for GobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GobError::Truncated => write!(f, "gob: unexpected end of input"),
            GobError::BadUint => write!(f, "gob: malformed uint"),
            GobError::BadValue(message) => write!(f, "gob: {message}"),
            GobError::UnknownType(id) => write!(f, "gob: undefined type id {id}"),
            GobError::TrailingBytes => write!(f, "gob: trailing bytes in message"),
            GobError::LegacyDelimitedType => {
                write!(
                    f,
                    "gob: legacy delimited type definition in interface value"
                )
            }
        }
    }
}

impl std::error::Error for GobError {}

pub type Result<T> = std::result::Result<T, GobError>;

/// A decoded gob value. Map entries keep wire order.
#[derive(Debug, Clone, PartialEq)]
pub enum GobValue {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Complex(f64, f64),
    Bytes(Vec<u8>),
    String(String),
    /// `None` is a nil interface.
    Interface(Option<Box<GobValue>>),
    Array(Vec<GobValue>),
    Slice(Vec<GobValue>),
    Map(Vec<(GobValue, GobValue)>),
    /// Field name -> value, in wire (declaration) order.
    Struct(Vec<(String, GobValue)>),
    GobEncoded(Vec<u8>),
}

impl GobValue {
    /// Look up a decoded struct field by its Go field name.
    pub fn field(&self, name: &str) -> Option<&GobValue> {
        match self {
            GobValue::Struct(fields) => fields
                .iter()
                .find_map(|(field, value)| (field == name).then_some(value)),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Result<i64> {
        match self {
            GobValue::Int(value) => Ok(*value),
            _ => Err(GobError::BadValue("expected int".to_string())),
        }
    }

    pub fn as_string(&self) -> Result<&str> {
        match self {
            GobValue::String(value) => Ok(value),
            _ => Err(GobError::BadValue("expected string".to_string())),
        }
    }

    pub fn as_slice(&self) -> Result<&[GobValue]> {
        match self {
            GobValue::Slice(items) | GobValue::Array(items) => Ok(items),
            _ => Err(GobError::BadValue("expected slice".to_string())),
        }
    }

    pub fn as_map(&self) -> Result<&[(GobValue, GobValue)]> {
        match self {
            GobValue::Map(entries) => Ok(entries),
            _ => Err(GobError::BadValue("expected map".to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WireType {
    Array { elem: i64, len: i64 },
    Slice { elem: i64 },
    Struct { fields: Vec<(String, i64)> },
    Map { key: i64, elem: i64 },
    GobEncoder,
}

struct Body<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Body<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Body { buf, pos: 0 }
    }

    fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }

    fn byte(&mut self) -> Result<u8> {
        let byte = *self.buf.get(self.pos).ok_or(GobError::Truncated)?;
        self.pos += 1;
        Ok(byte)
    }

    fn uint(&mut self) -> Result<u64> {
        let first = self.byte()?;
        if first <= 0x7f {
            return Ok(u64::from(first));
        }
        let n = (first as i8).unsigned_abs() as usize;
        if n == 0 || n > 8 {
            return Err(GobError::BadUint);
        }
        let bytes = self.take(n)?;
        let mut value = 0u64;
        for byte in bytes {
            value = (value << 8) | u64::from(*byte);
        }
        Ok(value)
    }

    fn int(&mut self) -> Result<i64> {
        Ok(to_int(self.uint()?))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(GobError::Truncated)?;
        if end > self.buf.len() {
            return Err(GobError::Truncated);
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn byte_block(&mut self) -> Result<Vec<u8>> {
        let n = self.uint()? as usize;
        Ok(self.take(n)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        let n = self.uint()? as usize;
        let bytes = self.take(n)?;
        String::from_utf8(bytes.to_vec()).map_err(|error| GobError::BadValue(error.to_string()))
    }
}

/// gob's `toInt`: low bit is the sign, remaining bits the magnitude.
fn to_int(value: u64) -> i64 {
    let magnitude = (value >> 1) as i64;
    if value & 1 != 0 {
        !magnitude
    } else {
        magnitude
    }
}

pub struct GobDecoder<'a> {
    buf: &'a [u8],
    pos: usize,
    wire: std::collections::HashMap<i64, WireType>,
}

impl<'a> GobDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        GobDecoder {
            buf,
            pos: 0,
            wire: std::collections::HashMap::new(),
        }
    }

    /// Decode every top-level value in the stream (type definitions are
    /// consumed but not returned).
    pub fn decode_all(&mut self) -> Result<Vec<GobValue>> {
        let mut values = Vec::new();
        while self.pos < self.buf.len() {
            let n = {
                let mut body = Body::new(&self.buf[self.pos..]);
                let n = body.uint()? as usize;
                self.pos += body.pos;
                n
            };
            let end = self.pos.checked_add(n).ok_or(GobError::Truncated)?;
            if end > self.buf.len() {
                return Err(GobError::Truncated);
            }
            let body = &self.buf[self.pos..end];
            self.pos = end;
            if let Some(value) = self.parse_message(body)? {
                values.push(value);
            }
        }
        Ok(values)
    }

    fn parse_message(&mut self, body: &'a [u8]) -> Result<Option<GobValue>> {
        let mut body = Body::new(body);
        let id = body.int()?;
        if id < 0 {
            self.parse_type_def(-id, &mut body)?;
            if !body.at_end() {
                return Err(GobError::TrailingBytes);
            }
            Ok(None)
        } else {
            let value = self.value(&mut body, id)?;
            if !body.at_end() {
                return Err(GobError::TrailingBytes);
            }
            Ok(Some(value))
        }
    }

    fn parse_type_def(&mut self, id: i64, body: &mut Body<'a>) -> Result<()> {
        let field = body.uint()? as i64 - 1;
        let wire = match field {
            0 => {
                expect_delta(body, 1)?;
                self.common(body)?;
                expect_delta(body, 1)?;
                let elem = body.int()?;
                expect_delta(body, 1)?;
                let len = body.int()?;
                WireType::Array { elem, len }
            }
            1 => {
                expect_delta(body, 1)?;
                self.common(body)?;
                expect_delta(body, 1)?;
                let elem = body.int()?;
                WireType::Slice { elem }
            }
            2 => {
                expect_delta(body, 1)?;
                self.common(body)?;
                expect_delta(body, 1)?;
                let n = body.uint()? as usize;
                let mut fields = Vec::with_capacity(n);
                for _ in 0..n {
                    expect_delta(body, 1)?;
                    let name = body.string()?;
                    expect_delta(body, 1)?;
                    let field_id = body.int()?;
                    expect_delta(body, 0)?;
                    fields.push((name, field_id));
                }
                WireType::Struct { fields }
            }
            3 => {
                expect_delta(body, 1)?;
                self.common(body)?;
                expect_delta(body, 1)?;
                let key = body.int()?;
                expect_delta(body, 1)?;
                let elem = body.int()?;
                WireType::Map { key, elem }
            }
            4..=6 => {
                expect_delta(body, 1)?;
                self.common(body)?;
                WireType::GobEncoder
            }
            other => {
                return Err(GobError::BadValue(format!(
                    "unknown wireType field {other}"
                )))
            }
        };
        expect_delta(body, 0)?;
        expect_delta(body, 0)?;
        self.wire.insert(id, wire);
        Ok(())
    }

    fn common(&mut self, body: &mut Body<'a>) -> Result<()> {
        let mut field = -1i64;
        loop {
            let delta = body.uint()? as i64;
            if delta == 0 {
                break;
            }
            field += delta;
            match field {
                0 => {
                    body.string()?;
                }
                1 => {
                    body.int()?;
                }
                other => {
                    return Err(GobError::BadValue(format!(
                        "corrupted CommonType field {other}"
                    )))
                }
            }
        }
        Ok(())
    }

    fn value(&mut self, body: &mut Body<'a>, id: i64) -> Result<GobValue> {
        let is_struct = !is_builtin(id)
            && id != T_INTERFACE
            && matches!(self.wire_type(id)?, WireType::Struct { .. });
        if is_struct {
            return self.struct_value(body, id);
        }
        let zero = body.uint()?;
        if zero != 0 {
            return Err(GobError::BadValue(format!(
                "singleton {id} has non-zero field delta {zero}"
            )));
        }
        self.field_value(body, id)
    }

    fn field_value(&mut self, body: &mut Body<'a>, id: i64) -> Result<GobValue> {
        if id == T_INTERFACE {
            return self.interface_value(body);
        }
        if is_builtin(id) {
            return self.builtin(body, id);
        }
        match self.wire_type(id)?.clone() {
            WireType::Array { elem, len } => {
                let n = body.uint()? as usize;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(self.field_value(body, elem)?);
                }
                if n as i64 != len {
                    return Err(GobError::BadValue(format!(
                        "array length {n} != declared {len}"
                    )));
                }
                Ok(GobValue::Array(items))
            }
            WireType::Slice { elem } => {
                let n = body.uint()? as usize;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(self.field_value(body, elem)?);
                }
                Ok(GobValue::Slice(items))
            }
            WireType::Map { key, elem } => {
                let n = body.uint()? as usize;
                let mut pairs = Vec::with_capacity(n);
                for _ in 0..n {
                    let key = self.field_value(body, key)?;
                    let value = self.field_value(body, elem)?;
                    pairs.push((key, value));
                }
                Ok(GobValue::Map(pairs))
            }
            WireType::Struct { .. } => self.struct_value(body, id),
            WireType::GobEncoder => Ok(GobValue::GobEncoded(body.byte_block()?)),
        }
    }

    fn struct_value(&mut self, body: &mut Body<'a>, id: i64) -> Result<GobValue> {
        let fields = match self.wire_type(id)?.clone() {
            WireType::Struct { fields } => fields,
            _ => return Err(GobError::BadValue(format!("type {id} is not a struct"))),
        };
        let mut out = Vec::new();
        let mut fieldnum = -1i64;
        loop {
            let delta = body.uint()? as i64;
            if delta == 0 {
                break;
            }
            fieldnum += delta;
            let (name, field_id) = fields.get(fieldnum as usize).ok_or_else(|| {
                GobError::BadValue(format!("struct field {fieldnum} out of range"))
            })?;
            let value = self.field_value(body, *field_id)?;
            out.push((name.clone(), value));
        }
        Ok(GobValue::Struct(out))
    }

    fn interface_value(&mut self, body: &mut Body<'a>) -> Result<GobValue> {
        let name_len = body.uint()? as usize;
        if name_len == 0 {
            return Ok(GobValue::Interface(None));
        }
        body.take(name_len)?;
        let id = body.int()?;
        if id < 0 {
            return Err(GobError::LegacyDelimitedType);
        }
        body.uint()?;
        let inner = self.value(body, id)?;
        Ok(GobValue::Interface(Some(Box::new(inner))))
    }

    fn builtin(&mut self, body: &mut Body<'a>, id: i64) -> Result<GobValue> {
        Ok(match id {
            T_BOOL => GobValue::Bool(body.uint()? != 0),
            T_INT => GobValue::Int(body.int()?),
            T_UINT => GobValue::Uint(body.uint()?),
            T_FLOAT => GobValue::Float(f64::from_bits(body.uint()?)),
            T_COMPLEX => {
                let real = f64::from_bits(body.uint()?);
                let imaginary = f64::from_bits(body.uint()?);
                GobValue::Complex(real, imaginary)
            }
            T_BYTES => GobValue::Bytes(body.byte_block()?),
            T_STRING => GobValue::String(body.string()?),
            other => return Err(GobError::UnknownType(other)),
        })
    }

    fn wire_type(&self, id: i64) -> Result<&WireType> {
        self.wire.get(&id).ok_or(GobError::UnknownType(id))
    }
}

fn is_builtin(id: i64) -> bool {
    (T_BOOL..=T_COMPLEX).contains(&id) || id == T_BYTES || id == T_STRING
}

fn expect_delta(body: &mut Body<'_>, want: u64) -> Result<()> {
    let got = body.uint()?;
    if got != want {
        return Err(GobError::BadValue(format!(
            "type descriptor delta {got}, want {want}"
        )));
    }
    Ok(())
}

/// Decode exactly one gob value from `buf`.
pub fn decode(buf: &[u8]) -> Result<GobValue> {
    let mut values = GobDecoder::new(buf).decode_all()?;
    match values.len() {
        1 => Ok(values.remove(0)),
        n => Err(GobError::BadValue(format!(
            "expected exactly one gob value, found {n}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// A gob type descriptor for the values these migrations write.
#[allow(dead_code)] // `Bool`/`Bytes` keep the codec complete for future fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GobType {
    Bool,
    Int,
    Bytes,
    String,
    Slice(Box<GobType>),
    Map(Box<GobType>, Box<GobType>),
    Struct {
        name: String,
        fields: Vec<(String, GobType)>,
    },
}

impl GobType {
    fn builtin_id(&self) -> Option<i64> {
        match self {
            GobType::Bool => Some(T_BOOL),
            GobType::Int => Some(T_INT),
            GobType::Bytes => Some(T_BYTES),
            GobType::String => Some(T_STRING),
            _ => None,
        }
    }
}

/// A gob value paired with no type information; the encoder is driven by the
/// [`GobType`] it is given (map/slice element types are not inferable from an
/// empty collection).
#[allow(dead_code)] // `Bool`/`Bytes` keep the codec complete for future fields.
#[derive(Debug, Clone, PartialEq)]
pub enum GobValueOut {
    Bool(bool),
    Int(i64),
    Bytes(Vec<u8>),
    String(String),
    Slice(Vec<GobValueOut>),
    Map(Vec<(GobValueOut, GobValueOut)>),
    Struct(Vec<(String, GobValueOut)>),
}

impl GobType {
    fn struct_field(&self, name: &str) -> Option<&GobType> {
        match self {
            GobType::Struct { fields, .. } => fields
                .iter()
                .find_map(|(field, ty)| (field == name).then_some(ty)),
            _ => None,
        }
    }
}

/// Builds the type-definition messages and value message for one gob stream.
/// A fresh encoder is used per column, matching `serializer.encode` in Go.
pub struct GobEncoder {
    definitions: Vec<u8>,
    wire_types: Vec<(i64, WireType)>,
    next_id: i64,
}

impl Default for GobEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl GobEncoder {
    pub fn new() -> Self {
        Self {
            definitions: Vec::new(),
            wire_types: Vec::new(),
            next_id: FIRST_USER_ID,
        }
    }

    /// Encode a single top-level value of the given type.
    pub fn encode(&mut self, ty: &GobType, value: &GobValueOut) -> Result<Vec<u8>> {
        let type_id = self.type_id(ty)?;

        let mut body = Vec::new();
        put_int(&mut body, type_id);
        if matches!(ty, GobType::Struct { .. }) {
            encode_struct_body(&mut body, ty, value)?;
        } else {
            body.push(0); // singleton field delta
            encode_field_payload(&mut body, ty, value)?;
        }

        let mut out = std::mem::take(&mut self.definitions);
        put_delimited(&mut out, &body);
        Ok(out)
    }

    fn type_id(&mut self, ty: &GobType) -> Result<i64> {
        if let Some(id) = ty.builtin_id() {
            return Ok(id);
        }

        let wire = match ty {
            GobType::Bool | GobType::Int | GobType::Bytes | GobType::String => unreachable!(),
            GobType::Slice(elem) => WireType::Slice {
                elem: self.type_id(elem)?,
            },
            GobType::Map(key, elem) => WireType::Map {
                key: self.type_id(key)?,
                elem: self.type_id(elem)?,
            },
            GobType::Struct { fields, .. } => {
                let mut encoded = Vec::with_capacity(fields.len());
                for (name, field_ty) in fields {
                    encoded.push((name.clone(), self.type_id(field_ty)?));
                }
                WireType::Struct { fields: encoded }
            }
        };

        if let Some((id, _)) = self
            .wire_types
            .iter()
            .find(|(_, existing)| *existing == wire)
        {
            return Ok(*id);
        }

        let id = self.next_id;
        self.next_id += 1;
        self.wire_types.push((id, wire.clone()));
        emit_type_definition(&mut self.definitions, id, &wire);
        Ok(id)
    }
}

fn encode_struct_body(out: &mut Vec<u8>, ty: &GobType, value: &GobValueOut) -> Result<()> {
    let (declared_fields, values) = match (ty, value) {
        (GobType::Struct { fields, .. }, GobValueOut::Struct(values)) => (fields, values),
        _ => return Err(GobError::BadValue("expected struct value".to_string())),
    };

    let mut previous = -1i64;
    for (index, (name, _field_ty)) in declared_fields.iter().enumerate() {
        let field_ty = ty
            .struct_field(name)
            .ok_or_else(|| GobError::BadValue(format!("missing field type for {name}")))?;
        let field_value = values
            .iter()
            .find_map(|(field, value)| (field == name).then_some(value))
            .ok_or_else(|| GobError::BadValue(format!("missing value for field {name}")))?;

        put_uint(out, (index as i64 - previous) as u64);
        previous = index as i64;
        encode_field_payload(out, field_ty, field_value)?;
    }
    put_uint(out, 0);
    Ok(())
}

fn encode_field_payload(out: &mut Vec<u8>, ty: &GobType, value: &GobValueOut) -> Result<()> {
    match (ty, value) {
        (GobType::Bool, GobValueOut::Bool(value)) => {
            put_uint(out, u64::from(*value));
            Ok(())
        }
        (GobType::Int, GobValueOut::Int(value)) => {
            put_int(out, *value);
            Ok(())
        }
        (GobType::Bytes, GobValueOut::Bytes(value)) => {
            put_uint(out, value.len() as u64);
            out.extend_from_slice(value);
            Ok(())
        }
        (GobType::String, GobValueOut::String(value)) => {
            put_uint(out, value.len() as u64);
            out.extend_from_slice(value.as_bytes());
            Ok(())
        }
        (GobType::Slice(elem), GobValueOut::Slice(items)) => {
            put_uint(out, items.len() as u64);
            for item in items {
                encode_field_payload(out, elem, item)?;
            }
            Ok(())
        }
        (GobType::Map(key_ty, elem_ty), GobValueOut::Map(entries)) => {
            put_uint(out, entries.len() as u64);
            for (key, value) in entries {
                encode_field_payload(out, key_ty, key)?;
                encode_field_payload(out, elem_ty, value)?;
            }
            Ok(())
        }
        (GobType::Struct { .. }, GobValueOut::Struct(_)) => encode_struct_body(out, ty, value),
        _ => Err(GobError::BadValue(
            "value does not match declared gob type".to_string(),
        )),
    }
}

fn emit_type_definition(out: &mut Vec<u8>, id: i64, wire: &WireType) {
    let mut body = Vec::new();
    put_int(&mut body, -id);

    match wire {
        WireType::Slice { elem } => {
            // wireType field 1 (SliceT); SliceT.CommonType; SliceT.Elem.
            put_uint(&mut body, 2);
            put_uint(&mut body, 1); // SliceT field 0: CommonType
            put_common_type(&mut body, "", id);
            put_uint(&mut body, 1); // SliceT field 1: Elem
            put_int(&mut body, *elem);
        }
        WireType::Map { key, elem } => {
            // wireType field 3 (MapT); MapT.CommonType/Key/Elem.
            put_uint(&mut body, 4);
            put_uint(&mut body, 1); // MapT field 0: CommonType
            put_common_type(&mut body, "", id);
            put_uint(&mut body, 1); // MapT field 1: Key
            put_int(&mut body, *key);
            put_uint(&mut body, 1); // MapT field 2: Elem
            put_int(&mut body, *elem);
        }
        WireType::Struct { fields } => {
            // wireType field 2 (StructT); StructT.CommonType/Field.
            put_uint(&mut body, 3);
            put_uint(&mut body, 1); // StructT field 0: CommonType
            put_common_type(&mut body, "", id);
            put_uint(&mut body, 1); // StructT field 1: Field
            put_uint(&mut body, fields.len() as u64);
            for (name, field_id) in fields {
                put_uint(&mut body, 1);
                put_uint(&mut body, name.len() as u64);
                body.extend_from_slice(name.as_bytes());
                put_uint(&mut body, 1);
                put_int(&mut body, *field_id);
                put_uint(&mut body, 0); // end of fieldType
            }
        }
        WireType::Array { .. } | WireType::GobEncoder => {
            unreachable!("encoder never emits array/gob-encoder types")
        }
    }

    put_uint(&mut body, 0); // end of concrete wire type
    put_uint(&mut body, 0); // end of wireType
    put_delimited(out, &body);
}

fn put_common_type(out: &mut Vec<u8>, name: &str, id: i64) {
    // CommonType { Name string; Id typeId }. Emit Name (field 0) when present,
    // then Id (field 1). Go's decoder does not consult Id, but writing it keeps
    // the stream faithful to Go's encoder.
    if !name.is_empty() {
        put_uint(out, 1);
        put_uint(out, name.len() as u64);
        out.extend_from_slice(name.as_bytes());
        put_uint(out, 1);
        put_int(out, id);
    } else {
        put_uint(out, 2);
        put_int(out, id);
    }
    put_uint(out, 0);
}

/// Encode a gob uint (see `Body::uint`).
fn put_uint(out: &mut Vec<u8>, value: u64) {
    if value <= 0x7f {
        out.push(value as u8);
        return;
    }
    let be = value.to_be_bytes();
    let first = be.iter().position(|byte| *byte != 0).unwrap();
    let n = 8 - first;
    out.push((-(n as i8)) as u8);
    out.extend_from_slice(&be[first..]);
}

/// Encode a gob int (zigzag with the inverted negative branch, as in Go).
fn put_int(out: &mut Vec<u8>, value: i64) {
    let encoded = if value < 0 {
        !((value as u64) << 1)
    } else {
        (value as u64) << 1
    };
    put_uint(out, encoded);
}

fn put_delimited(out: &mut Vec<u8>, body: &[u8]) {
    put_uint(out, body.len() as u64);
    out.extend_from_slice(body);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(value: &str) -> Vec<u8> {
        let value: String = value.chars().filter(|c| !c.is_whitespace()).collect();
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn round_trips_slice_of_locations() {
        let ty = GobType::Slice(Box::new(GobType::Struct {
            name: "LocationData".to_string(),
            fields: vec![
                ("URI".to_string(), GobType::String),
                ("StartLine".to_string(), GobType::Int),
                ("StartCharacter".to_string(), GobType::Int),
                ("EndLine".to_string(), GobType::Int),
                ("EndCharacter".to_string(), GobType::Int),
            ],
        }));

        let value = GobValueOut::Slice(vec![GobValueOut::Struct(vec![
            ("URI".to_string(), GobValueOut::String("a.go".to_string())),
            ("StartLine".to_string(), GobValueOut::Int(36)),
            ("StartCharacter".to_string(), GobValueOut::Int(26)),
            ("EndLine".to_string(), GobValueOut::Int(36)),
            ("EndCharacter".to_string(), GobValueOut::Int(32)),
        ])]);

        let bytes = GobEncoder::new().encode(&ty, &value).unwrap();
        let decoded = decode(&bytes).unwrap();

        let items = decoded.as_slice().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].field("URI").unwrap().as_string().unwrap(), "a.go");
        assert_eq!(
            items[0].field("StartCharacter").unwrap().as_int().unwrap(),
            26
        );
    }

    #[test]
    fn round_trips_map_of_structs() {
        let range_ty = GobType::Struct {
            name: "RangeData".to_string(),
            fields: vec![
                ("StartLine".to_string(), GobType::Int),
                (
                    "MonikerIDs".to_string(),
                    GobType::Slice(Box::new(GobType::String)),
                ),
            ],
        };
        let ty = GobType::Map(Box::new(GobType::String), Box::new(range_ty));
        let value = GobValueOut::Map(vec![(
            GobValueOut::String("7864".to_string()),
            GobValueOut::Struct(vec![
                ("StartLine".to_string(), GobValueOut::Int(541)),
                (
                    "MonikerIDs".to_string(),
                    GobValueOut::Slice(vec![GobValueOut::String("314".to_string())]),
                ),
            ]),
        )]);

        let bytes = GobEncoder::new().encode(&ty, &value).unwrap();
        let decoded = decode(&bytes).unwrap();
        let entries = decoded.as_map().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.as_string().unwrap(), "7864");
        assert_eq!(
            entries[0].1.field("StartLine").unwrap().as_int().unwrap(),
            541
        );
    }

    #[test]
    fn uint_round_trips() {
        for value in [0u64, 1, 0x7f, 0x80, 0x1234, u32::MAX as u64] {
            let mut buffer = Vec::new();
            put_uint(&mut buffer, value);
            let mut body = Body::new(&buffer);
            assert_eq!(body.uint().unwrap(), value);
        }
    }
}
