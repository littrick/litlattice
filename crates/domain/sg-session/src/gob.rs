//! Restricted `encoding/gob` codec.
//!
//! Sourcegraph stores two gob-encoded values that Rust must interoperate with:
//!
//! * the session cookie value: a bare Go `string` (the session ID);
//! * the Redis session payload: `map[interface{}]interface{}` whose keys are
//!   `string` and whose values are `[]byte` (JSON documents written by
//!   `session.SetData`).
//!
//! The decoder below implements enough of the gob wire format to read *any*
//! value Go produces for these stores (all builtin types, maps, slices,
//! structs, interfaces), mirroring `$GOROOT/src/encoding/gob/debug.go`. The
//! encoder only emits the two shapes above, byte-for-byte identical to Go's
//! encoder in a fresh `gob.Encoder` (which is what `redistore.GobSerializer`
//! and `securecookie.GobEncoder` use: one encoder per `Save`/`Encode` call).
//!
//! Wire format notes (all integers are gob uints: a byte <= 0x7f is the value,
//! otherwise the first byte is the negated big-endian byte count):
//!
//! * stream = delimited messages, each `uint(len) body`;
//! * a message body is either `int(-typeId) wireType` (type definition, only
//!   ever sent for the first occurrence of a type in this encoder) or
//!   `int(typeId) value`;
//! * a top-level non-struct value is preceded by a zero field delta (`0x00`);
//! * an interface value is `uint(nameLen) name int(concreteTypeId)
//!   uint(delimitedLen) value` (nameLen 0 means nil), where `value` for a
//!   non-struct concrete type includes its own leading zero delta;
//! * a map is `uint(count)` then `key value` pairs; iteration order is Go map
//!   order, i.e. randomized for more than one entry.

use std::collections::HashMap;
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

#[derive(Debug, Clone, PartialEq)]
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
            GobError::BadValue(m) => write!(f, "gob: {m}"),
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

/// A decoded gob value. Map entries keep wire order (Go's map order).
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
    /// Interpret the value as the Redis session payload:
    /// `map[interface{}]interface{}` with string keys and []byte values.
    pub fn into_session_map(self) -> Result<Vec<(String, Vec<u8>)>> {
        let GobValue::Map(pairs) = self else {
            return Err(GobError::BadValue("top-level value is not a map".into()));
        };
        pairs
            .into_iter()
            .map(|(k, v)| {
                let key = match k {
                    GobValue::Interface(Some(inner)) => match *inner {
                        GobValue::String(s) => s,
                        other => {
                            return Err(GobError::BadValue(format!(
                                "session key has concrete type {other:?}, want string"
                            )))
                        }
                    },
                    other => {
                        return Err(GobError::BadValue(format!(
                            "session key is not an interface: {other:?}"
                        )))
                    }
                };
                let value = match v {
                    GobValue::Interface(Some(inner)) => match *inner {
                        GobValue::Bytes(b) => b,
                        GobValue::String(s) => s.into_bytes(),
                        other => {
                            return Err(GobError::BadValue(format!(
                            "session value for {key:?} has concrete type {other:?}, want []uint8"
                        )))
                        }
                    },
                    other => {
                        return Err(GobError::BadValue(format!(
                            "session value for {key:?} is not an interface: {other:?}"
                        )))
                    }
                };
                Ok((key, value))
            })
            .collect()
    }

    pub fn into_string(self) -> Result<String> {
        match self {
            GobValue::String(s) => Ok(s),
            other => Err(GobError::BadValue(format!(
                "top-level value is not a string: {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // names are kept for diagnostics
enum WireType {
    Array {
        elem: i64,
        len: i64,
    },
    Slice {
        elem: i64,
    },
    Struct {
        name: String,
        fields: Vec<(String, i64)>,
    },
    Map {
        key: i64,
        elem: i64,
    },
    GobEncoder {
        name: String,
    },
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
        let b = *self.buf.get(self.pos).ok_or(GobError::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    /// Gob uint: one byte <= 0x7f, else a negated byte count followed by the
    /// big-endian value.
    fn uint(&mut self) -> Result<u64> {
        let first = self.byte()?;
        if first <= 0x7f {
            return Ok(first as u64);
        }
        let n = (first as i8).unsigned_abs() as usize;
        if n == 0 || n > 8 {
            return Err(GobError::BadUint);
        }
        let bytes = self.take(n)?;
        let mut v = 0u64;
        for b in bytes {
            v = (v << 8) | *b as u64;
        }
        Ok(v)
    }

    fn int(&mut self) -> Result<i64> {
        Ok(to_int(self.uint()?))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(GobError::Truncated)?;
        if end > self.buf.len() {
            return Err(GobError::Truncated);
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn byte_block(&mut self) -> Result<Vec<u8>> {
        let n = self.uint()? as usize;
        Ok(self.take(n)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        let n = self.uint()? as usize;
        let b = self.take(n)?;
        String::from_utf8(b.to_vec()).map_err(|e| GobError::BadValue(e.to_string()))
    }
}

/// gob's `toInt`: low bit is the sign, remaining bits the magnitude.
fn to_int(u: u64) -> i64 {
    let i = (u >> 1) as i64;
    if u & 1 != 0 {
        !i
    } else {
        i
    }
}

pub struct GobDecoder<'a> {
    buf: &'a [u8],
    pos: usize,
    wire: HashMap<i64, WireType>,
}

impl<'a> GobDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        GobDecoder {
            buf,
            pos: 0,
            wire: HashMap::new(),
        }
    }

    /// Decode every top-level value in the stream (type definitions are
    /// consumed but not returned). The session stores always contain exactly
    /// one value.
    pub fn decode_all(&mut self) -> Result<Vec<GobValue>> {
        let mut values = Vec::new();
        while self.pos < self.buf.len() {
            let n = {
                let mut b = Body::new(&self.buf[self.pos..]);
                let n = b.uint()? as usize;
                self.pos += b.pos;
                n
            };
            let end = self.pos.checked_add(n).ok_or(GobError::Truncated)?;
            if end > self.buf.len() {
                return Err(GobError::Truncated);
            }
            let body = &self.buf[self.pos..end];
            self.pos = end;
            if let Some(v) = self.parse_message(body)? {
                values.push(v);
            }
        }
        Ok(values)
    }

    fn parse_message(&mut self, body: &'a [u8]) -> Result<Option<GobValue>> {
        let mut b = Body::new(body);
        let id = b.int()?;
        if id < 0 {
            self.parse_type_def(-id, &mut b)?;
            if !b.at_end() {
                return Err(GobError::TrailingBytes);
            }
            Ok(None)
        } else {
            let v = self.value(&mut b, id)?;
            if !b.at_end() {
                return Err(GobError::TrailingBytes);
            }
            Ok(Some(v))
        }
    }

    fn parse_type_def(&mut self, id: i64, b: &mut Body<'a>) -> Result<()> {
        let field = b.uint()? as i64 - 1; // delta from fieldnum -1
        let wire = match field {
            0 => {
                expect_delta(b, 1)?;
                let _common = self.common(b)?;
                expect_delta(b, 1)?;
                let elem = b.int()?;
                expect_delta(b, 1)?;
                let len = b.int()?;
                WireType::Array { elem, len }
            }
            1 => {
                expect_delta(b, 1)?;
                let _common = self.common(b)?;
                expect_delta(b, 1)?;
                let elem = b.int()?;
                WireType::Slice { elem }
            }
            2 => {
                expect_delta(b, 1)?;
                let common = self.common(b)?;
                expect_delta(b, 1)?;
                let n = b.uint()? as usize;
                let mut fields = Vec::with_capacity(n);
                for _ in 0..n {
                    expect_delta(b, 1)?;
                    let name = b.string()?;
                    expect_delta(b, 1)?;
                    let fid = b.int()?;
                    expect_delta(b, 0)?; // end of fieldType
                    fields.push((name, fid));
                }
                WireType::Struct {
                    name: common.name,
                    fields,
                }
            }
            3 => {
                expect_delta(b, 1)?;
                let _common = self.common(b)?;
                expect_delta(b, 1)?;
                let key = b.int()?;
                expect_delta(b, 1)?;
                let elem = b.int()?;
                WireType::Map { key, elem }
            }
            4..=6 => {
                expect_delta(b, 1)?;
                let common = self.common(b)?;
                WireType::GobEncoder { name: common.name }
            }
            other => {
                return Err(GobError::BadValue(format!(
                    "unknown wireType field {other}"
                )))
            }
        };
        expect_delta(b, 0)?; // end of inner type
        expect_delta(b, 0)?; // end of wireType
        self.wire.insert(id, wire);
        Ok(())
    }

    fn common(&mut self, b: &mut Body<'a>) -> Result<CommonType> {
        let mut field = -1i64;
        let mut name = String::new();
        let mut id = 0i64;
        loop {
            let delta = b.uint()? as i64;
            if delta == 0 {
                break;
            }
            field += delta;
            match field {
                0 => name = b.string()?,
                1 => id = b.int()?,
                other => {
                    return Err(GobError::BadValue(format!(
                        "corrupted CommonType field {other}"
                    )))
                }
            }
        }
        Ok(CommonType { name, id })
    }

    /// A value as written by `value()` in debug.go: structs have no leading
    /// zero, everything else is a "singleton" preceded by a zero delta.
    fn value(&mut self, b: &mut Body<'a>, id: i64) -> Result<GobValue> {
        // Structs are encoded directly; every other top-level value (including
        // builtins and interfaces) is a "singleton" preceded by a zero delta.
        let is_struct = !is_builtin(id)
            && id != T_INTERFACE
            && matches!(self.wire_type(id)?, WireType::Struct { .. });
        if is_struct {
            return self.struct_value(b, id);
        }
        let zero = b.uint()?;
        if zero != 0 {
            return Err(GobError::BadValue(format!(
                "singleton {id} has non-zero field delta {zero}"
            )));
        }
        self.field_value(b, id)
    }

    fn field_value(&mut self, b: &mut Body<'a>, id: i64) -> Result<GobValue> {
        if id == T_INTERFACE {
            return self.interface_value(b);
        }
        if is_builtin(id) {
            return self.builtin(b, id);
        }
        match self.wire_type(id)?.clone() {
            WireType::Array { elem, len } => {
                let n = b.uint()? as usize;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(self.field_value(b, elem)?);
                }
                if n as i64 != len {
                    return Err(GobError::BadValue(format!(
                        "array length {n} != declared {len}"
                    )));
                }
                Ok(GobValue::Array(items))
            }
            WireType::Slice { elem } => {
                let n = b.uint()? as usize;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(self.field_value(b, elem)?);
                }
                Ok(GobValue::Slice(items))
            }
            WireType::Map { key, elem } => {
                let n = b.uint()? as usize;
                let mut pairs = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = self.field_value(b, key)?;
                    let v = self.field_value(b, elem)?;
                    pairs.push((k, v));
                }
                Ok(GobValue::Map(pairs))
            }
            WireType::Struct { .. } => self.struct_value(b, id),
            WireType::GobEncoder { .. } => Ok(GobValue::GobEncoded(b.byte_block()?)),
        }
    }

    fn struct_value(&mut self, b: &mut Body<'a>, id: i64) -> Result<GobValue> {
        let fields = match self.wire_type(id)?.clone() {
            WireType::Struct { fields, .. } => fields,
            _ => return Err(GobError::BadValue(format!("type {id} is not a struct"))),
        };
        let mut out = Vec::new();
        let mut fieldnum = -1i64;
        loop {
            let delta = b.uint()? as i64;
            if delta == 0 {
                break;
            }
            fieldnum += delta;
            let (name, fid) = fields.get(fieldnum as usize).ok_or_else(|| {
                GobError::BadValue(format!("struct field {fieldnum} out of range"))
            })?;
            let v = self.field_value(b, *fid)?;
            out.push((name.clone(), v));
        }
        Ok(GobValue::Struct(out))
    }

    fn interface_value(&mut self, b: &mut Body<'a>) -> Result<GobValue> {
        let name_len = b.uint()? as usize;
        if name_len == 0 {
            return Ok(GobValue::Interface(None));
        }
        let _concrete_name = b.take(name_len)?;
        let id = b.int()?;
        if id < 0 {
            // Go's encoder never emits this (type definitions are separate
            // messages); debug.go supports it for historical encodings.
            return Err(GobError::LegacyDelimitedType);
        }
        let _delimited_len = b.uint()?;
        let inner = self.value(b, id)?;
        Ok(GobValue::Interface(Some(Box::new(inner))))
    }

    fn builtin(&mut self, b: &mut Body<'a>, id: i64) -> Result<GobValue> {
        Ok(match id {
            T_BOOL => GobValue::Bool(b.uint()? != 0),
            T_INT => GobValue::Int(b.int()?),
            T_UINT => GobValue::Uint(b.uint()?),
            T_FLOAT => GobValue::Float(f64::from_bits(b.uint()?)),
            T_COMPLEX => {
                let r = f64::from_bits(b.uint()?);
                let i = f64::from_bits(b.uint()?);
                GobValue::Complex(r, i)
            }
            T_BYTES => GobValue::Bytes(b.byte_block()?),
            T_STRING => GobValue::String(b.string()?),
            other => return Err(GobError::UnknownType(other)),
        })
    }

    fn wire_type(&self, id: i64) -> Result<&WireType> {
        self.wire.get(&id).ok_or(GobError::UnknownType(id))
    }
}

struct CommonType {
    name: String,
    #[allow(dead_code)]
    id: i64,
}

fn is_builtin(id: i64) -> bool {
    (T_BOOL..=T_COMPLEX).contains(&id) || id == T_BYTES || id == T_STRING
}

fn expect_delta(b: &mut Body<'_>, want: u64) -> Result<()> {
    let got = b.uint()?;
    if got != want {
        return Err(GobError::BadValue(format!(
            "type descriptor delta {got}, want {want}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Encoding: the two shapes the session store writes.
// ---------------------------------------------------------------------------

/// Type definition emitted by a fresh `gob.Encoder` for
/// `map[interface{}]interface{}` (type id 64, key/elem = interface id 8).
const MAP_TYPE_DEF: &[u8] = &[
    0x0d, // message length 13
    0x7f, // int(-64)
    0x04, // wireType field 3: MapT
    0x01, // MapT field 0: CommonType
    0x02, // CommonType field 1: Id (Name is empty and omitted)
    0xff, 0x80, // Id = 64 (encodeInt(64) = encodeUint(128))
    0x00, // end CommonType
    0x01, 0x10, // key field, interface id 8
    0x01, 0x10, // elem field, interface id 8
    0x00, // end MapT
    0x00, // end wireType
];

/// Encode a gob uint (see `Body::uint`).
fn put_uint(out: &mut Vec<u8>, x: u64) {
    if x <= 0x7f {
        out.push(x as u8);
        return;
    }
    let be = x.to_be_bytes();
    let first = be.iter().position(|b| *b != 0).unwrap();
    let n = 8 - first;
    out.push((-(n as i8)) as u8);
    out.extend_from_slice(&be[first..]);
}

/// Encode a gob int (zigzag with inverted negative branch, as in Go).
fn put_int(out: &mut Vec<u8>, i: i64) {
    let u = if i < 0 {
        !((i as u64) << 1)
    } else {
        (i as u64) << 1
    };
    put_uint(out, u);
}

fn put_delimited(out: &mut Vec<u8>, body: &[u8]) {
    put_uint(out, body.len() as u64);
    out.extend_from_slice(body);
}

/// Gob-encode a bare string (a sourcegraph session ID). Matches
/// `securecookie.GobEncoder.Serialize("...")` exactly.
pub fn encode_string(s: &str) -> Vec<u8> {
    let mut body = Vec::new();
    put_int(&mut body, T_STRING);
    body.push(0); // singleton field delta
    put_uint(&mut body, s.len() as u64);
    body.extend_from_slice(s.as_bytes());
    let mut out = Vec::new();
    put_delimited(&mut out, &body);
    out
}

/// Encoding of a string/[]byte builtin: `uint(len) raw bytes`.
fn builtin_bytes_payload(data: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(data.len() + 2);
    put_uint(&mut p, data.len() as u64);
    p.extend_from_slice(data);
    p
}

/// `uint(nameLen) name int(typeId) uint(delimitedLen) zero payload`.
fn put_interface(out: &mut Vec<u8>, name: &str, type_id: i64, payload: &[u8]) {
    put_uint(out, name.len() as u64);
    out.extend_from_slice(name.as_bytes());
    put_int(out, type_id);
    // The delimited value starts with the singleton zero delta.
    put_uint(out, (payload.len() + 1) as u64);
    out.push(0);
    out.extend_from_slice(payload);
}

/// Encode `map[interface{}]interface{}` with string keys and []byte values,
/// exactly as a fresh Go `gob.Encoder` would. Map order is preserved as given
/// (Go randomizes it for more than one entry, so byte equality is only
/// meaningful for zero/one-entry maps).
pub fn encode_session_map(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    put_int(&mut body, FIRST_USER_ID);
    body.push(0); // singleton field delta
    put_uint(&mut body, entries.len() as u64);
    for (k, v) in entries {
        put_interface(
            &mut body,
            "string",
            T_STRING,
            &builtin_bytes_payload(k.as_bytes()),
        );
        put_interface(&mut body, "[]uint8", T_BYTES, &builtin_bytes_payload(v));
    }

    let mut out = Vec::with_capacity(MAP_TYPE_DEF.len() + body.len() + 2);
    out.extend_from_slice(MAP_TYPE_DEF);
    put_delimited(&mut out, &body);
    out
}

/// Decode one gob value from `buf`; errors if the stream does not contain
/// exactly one value.
pub fn decode(buf: &[u8]) -> Result<GobValue> {
    let mut values = GobDecoder::new(buf).decode_all()?;
    match values.len() {
        1 => Ok(values.remove(0)),
        n => Err(GobError::BadValue(format!(
            "expected exactly one gob value, found {n}"
        ))),
    }
}

pub fn decode_string(buf: &[u8]) -> Result<String> {
    decode(buf)?.into_string()
}

pub fn decode_session_map(buf: &[u8]) -> Result<Vec<(String, Vec<u8>)>> {
    decode(buf)?.into_session_map()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    // Reference vectors produced by Go 1.22 encoding/gob
    // (test_ws/p4-gob, see poc/p4-session/go-fixture for the checked-in
    // generator).
    #[test]
    fn decodes_go_string() {
        assert_eq!(decode_string(&hex("070c00044f555954")).unwrap(), "OUYT");
    }

    #[test]
    fn encodes_go_string() {
        assert_eq!(encode_string("OUYT"), hex("070c00044f555954"));
    }

    #[test]
    fn go_map_iface_single_entry() {
        let bytes = hex("0d7f040102ff800001100110000025ff80000106737472696e670c0700056163746f72075b5d75696e74380a07000568656c6c6f");
        assert_eq!(
            decode_session_map(&bytes).unwrap(),
            vec![("actor".to_string(), b"hello".to_vec())]
        );
        assert_eq!(encode_session_map(&[("actor", b"hello")]), bytes);
    }

    #[test]
    fn go_map_iface_empty() {
        let bytes = hex("0d7f040102ff800001100110000004ff800000");
        assert!(decode_session_map(&bytes).unwrap().is_empty());
        assert_eq!(encode_session_map(&[]), bytes);
    }

    #[test]
    fn go_map_iface_two_entries_decodes() {
        let bytes = hex("0d7f040102ff800001100110000037ff80000206737472696e670c03000161075b5d75696e74380a0300013106737472696e670c03000162075b5d75696e74380a0400023232");
        assert_eq!(
            decode_session_map(&bytes).unwrap(),
            vec![
                ("a".to_string(), b"1".to_vec()),
                ("b".to_string(), b"22".to_vec())
            ]
        );
    }

    #[test]
    fn go_map_iface_non_bytes_values() {
        let str_val = hex("0d7f040102ff80000110011000001cff80000106737472696e670c0300016106737472696e670c03000178");
        assert_eq!(
            decode(&str_val).unwrap(),
            GobValue::Map(vec![(
                GobValue::Interface(Some(Box::new(GobValue::String("a".into())))),
                GobValue::Interface(Some(Box::new(GobValue::String("x".into())))),
            )])
        );

        let int_val = hex(
            "0d7f040102ff80000110011000001aff80000106737472696e670c0300016105696e7436340402000e",
        );
        assert_eq!(
            decode(&int_val).unwrap(),
            GobValue::Map(vec![(
                GobValue::Interface(Some(Box::new(GobValue::String("a".into())))),
                GobValue::Interface(Some(Box::new(GobValue::Int(7)))),
            )])
        );

        let nil_val = hex("0d7f040102ff800001100110000011ff80000106737472696e670c0300016100");
        assert_eq!(
            decode(&nil_val).unwrap(),
            GobValue::Map(vec![(
                GobValue::Interface(Some(Box::new(GobValue::String("a".into())))),
                GobValue::Interface(None),
            )])
        );
    }

    #[test]
    fn rust_map_roundtrip_multi() {
        let entries: Vec<(&str, &[u8])> = vec![
            ("actor", br#"{"UID":1}"#.as_slice()),
            ("oauthState", b"x"),
            ("oidc@0", b""),
        ];
        let bytes = encode_session_map(&entries);
        let decoded = decode_session_map(&bytes).unwrap();
        assert_eq!(
            decoded,
            entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_vec()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes = encode_string("x");
        bytes.push(0);
        assert!(decode_string(&bytes).is_err());
    }
}
