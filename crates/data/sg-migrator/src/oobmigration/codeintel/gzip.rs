#![forbid(unsafe_code)]

//! Minimal gzip codec used by the code-intelligence migrations.
//!
//! The `data` columns written by the baseline are `gzip(gob(value))` (see
//! `serializer.go` in the Go tree). `sg-migrator` has no `flate2` dependency,
//! so this module implements the small slice of RFC 1951/1952 we need:
//!
//! * [`decompress`] implements a complete inflate reader (stored, fixed and
//!   dynamic Huffman blocks) so it can read anything Go's `compress/gzip`
//!   produces.
//! * [`compress`] always emits *stored* (uncompressed) deflate blocks wrapped
//!   in a gzip container. That is a valid gzip stream that Go's
//!   `gzip.Reader` accepts; it is not byte-identical to Go's output, which is
//!   fine because nothing compares the compressed bytes.
//!
//! CRC32 and ISIZE trailer values are written on compression and ignored on
//! decompression (the migrations only care that the payload decodes).

use std::fmt;

/// Errors produced while decompressing a gzip stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GzipError(pub String);

impl fmt::Display for GzipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gzip: {}", self.0)
    }
}

impl std::error::Error for GzipError {}

type Result<T> = std::result::Result<T, GzipError>;

fn err<T>(message: impl Into<String>) -> Result<T> {
    Err(GzipError(message.into()))
}

/// Decompress a complete gzip stream.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    let payload = strip_header(data)?;
    inflate(payload)
}

/// Compress `data` into a gzip stream using stored deflate blocks.
pub fn compress(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 32);
    // Fixed header: deflate, no flags, mtime 0, xfl 0, OS unknown (255).
    out.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff]);

    let mut remaining = data;
    loop {
        let chunk = &remaining[..remaining.len().min(0xffff)];
        remaining = &remaining[chunk.len()..];
        let final_block = remaining.is_empty();

        // A stored block starts byte-aligned. BFINAL is bit 0, BTYPE 00 is
        // bits 1-2, so the header byte is 0x00 or 0x01.
        out.push(u8::from(final_block));
        let len = chunk.len() as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(chunk);

        if final_block {
            break;
        }
    }

    out.extend_from_slice(&crc32(data).to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out
}

/// Parse and skip the gzip header, returning the raw deflate payload.
fn strip_header(data: &[u8]) -> Result<&[u8]> {
    if data.len() < 10 {
        return err("truncated header");
    }
    if data[0] != 0x1f || data[1] != 0x8b {
        return err("bad magic");
    }
    if data[2] != 0x08 {
        return err(format!("unsupported compression method {}", data[2]));
    }
    let flags = data[3];
    let mut pos = 10usize;

    if flags & 0x04 != 0 {
        // FEXTRA
        if pos + 2 > data.len() {
            return err("truncated extra length");
        }
        let xlen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + xlen;
    }
    if flags & 0x08 != 0 {
        // FNAME
        pos = skip_zero_terminated(data, pos)?;
    }
    if flags & 0x10 != 0 {
        // FCOMMENT
        pos = skip_zero_terminated(data, pos)?;
    }
    if flags & 0x02 != 0 {
        // FHCRC
        pos += 2;
    }

    if pos > data.len() {
        return err("truncated header");
    }
    // The trailer (CRC32 + ISIZE) is not needed for decoding.
    let end = data.len().saturating_sub(8);
    if end < pos {
        return err("truncated payload");
    }
    Ok(&data[pos..end])
}

fn skip_zero_terminated(data: &[u8], mut pos: usize) -> Result<usize> {
    while pos < data.len() {
        if data[pos] == 0 {
            return Ok(pos + 1);
        }
        pos += 1;
    }
    err("unterminated header field")
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

// ---------------------------------------------------------------------------
// Inflate
// ---------------------------------------------------------------------------

struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
    buf: u64,
    count: u32,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            buf: 0,
            count: 0,
        }
    }

    /// Read `n` bits, least-significant bit first.
    fn bits(&mut self, n: u32) -> Result<u32> {
        debug_assert!(n <= 32);
        while self.count < n {
            let byte = *self
                .data
                .get(self.pos)
                .ok_or_else(|| GzipError("unexpected end of deflate stream".to_string()))?;
            self.pos += 1;
            self.buf |= u64::from(byte) << self.count;
            self.count += 8;
        }
        let value = (self.buf & ((1u64 << n) - 1)) as u32;
        self.buf >>= n;
        self.count -= n;
        Ok(value)
    }

    /// Discard buffered bits and realign on the next byte boundary.
    fn align(&mut self) {
        self.buf = 0;
        self.count = 0;
    }
}

struct Huffman {
    /// `entries[len]` holds `(canonical_code, symbol)` pairs of that length.
    entries: Vec<Vec<(u32, u16)>>,
}

impl Huffman {
    fn build(lengths: &[u8]) -> Result<Self> {
        let max = lengths.iter().copied().max().unwrap_or(0) as usize;
        if max == 0 {
            return Ok(Self {
                entries: Vec::new(),
            });
        }
        if max > 15 {
            return err("code length exceeds 15 bits");
        }

        let mut counts = [0u32; 16];
        for &length in lengths {
            if length > 0 {
                counts[length as usize] += 1;
            }
        }

        let mut next = [0u32; 16];
        let mut code = 0u32;
        for bits in 1..=15 {
            code = (code + counts[bits - 1]) << 1;
            next[bits] = code;
        }

        let mut entries = vec![Vec::new(); 16];
        for (symbol, &length) in lengths.iter().enumerate() {
            if length > 0 {
                let length = length as usize;
                entries[length].push((next[length], symbol as u16));
                next[length] += 1;
            }
        }

        Ok(Self { entries })
    }

    fn decode(&self, reader: &mut BitReader<'_>) -> Result<u16> {
        let mut code = 0u32;
        for length in 1..self.entries.len() {
            code = (code << 1) | reader.bits(1)?;
            for &(candidate, symbol) in &self.entries[length] {
                if candidate == code {
                    return Ok(symbol);
                }
            }
        }
        err("invalid Huffman code")
    }
}

const LENGTH_BASE: [u16; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
const LENGTH_EXTRA: [u8; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DIST_BASE: [u16; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
];
const DIST_EXTRA: [u8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];

fn inflate(data: &[u8]) -> Result<Vec<u8>> {
    let mut reader = BitReader::new(data);
    let mut output = Vec::new();

    loop {
        let final_block = reader.bits(1)? == 1;
        match reader.bits(2)? {
            0 => inflate_stored(&mut reader, &mut output)?,
            1 => {
                let (litlen, dist) = fixed_tables()?;
                inflate_huffman(&mut reader, &mut output, &litlen, &dist)?;
            }
            2 => {
                let (litlen, dist) = dynamic_tables(&mut reader)?;
                inflate_huffman(&mut reader, &mut output, &litlen, &dist)?;
            }
            other => return err(format!("invalid deflate block type {other}")),
        }

        if final_block {
            break;
        }
    }

    Ok(output)
}

fn inflate_stored(reader: &mut BitReader<'_>, output: &mut Vec<u8>) -> Result<()> {
    reader.align();
    let len = read_u16_le(reader)? as usize;
    let nlen = read_u16_le(reader)?;
    if (len as u16) != !nlen {
        return err("stored block length check failed");
    }
    if reader.pos + len > reader.data.len() {
        return err("truncated stored block");
    }
    output.extend_from_slice(&reader.data[reader.pos..reader.pos + len]);
    reader.pos += len;
    Ok(())
}

fn read_u16_le(reader: &mut BitReader<'_>) -> Result<u16> {
    let low = *reader
        .data
        .get(reader.pos)
        .ok_or_else(|| GzipError("truncated stored block".to_string()))?;
    let high = *reader
        .data
        .get(reader.pos + 1)
        .ok_or_else(|| GzipError("truncated stored block".to_string()))?;
    reader.pos += 2;
    Ok(u16::from_le_bytes([low, high]))
}

fn fixed_tables() -> Result<(Huffman, Huffman)> {
    let mut litlen_lengths = vec![0u8; 288];
    for length in litlen_lengths.iter_mut().take(144) {
        *length = 8;
    }
    for length in litlen_lengths.iter_mut().take(256).skip(144) {
        *length = 9;
    }
    for length in litlen_lengths.iter_mut().take(280).skip(256) {
        *length = 7;
    }
    for length in litlen_lengths.iter_mut().take(288).skip(280) {
        *length = 8;
    }

    // Fixed distance codes are 5 bits each.
    let dist_lengths = vec![5u8; 32];

    Ok((
        Huffman::build(&litlen_lengths)?,
        Huffman::build(&dist_lengths)?,
    ))
}

fn dynamic_tables(reader: &mut BitReader<'_>) -> Result<(Huffman, Huffman)> {
    let hlit = reader.bits(5)? as usize + 257;
    let hdist = reader.bits(5)? as usize + 1;
    let hclen = reader.bits(4)? as usize + 4;

    const ORDER: [usize; 19] = [
        16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
    ];

    let mut code_length_lengths = vec![0u8; 19];
    for &index in ORDER.iter().take(hclen) {
        code_length_lengths[index] = reader.bits(3)? as u8;
    }
    let code_length_huffman = Huffman::build(&code_length_lengths)?;

    let mut lengths = Vec::with_capacity(hlit + hdist);
    while lengths.len() < hlit + hdist {
        let symbol = code_length_huffman.decode(reader)?;
        match symbol {
            0..=15 => lengths.push(symbol as u8),
            16 => {
                let previous = *lengths
                    .last()
                    .ok_or_else(|| GzipError("repeat with no previous length".to_string()))?;
                let repeat = reader.bits(2)? as usize + 3;
                lengths.extend(std::iter::repeat(previous).take(repeat));
            }
            17 => {
                let repeat = reader.bits(3)? as usize + 3;
                lengths.extend(std::iter::repeat(0u8).take(repeat));
            }
            18 => {
                let repeat = reader.bits(7)? as usize + 11;
                lengths.extend(std::iter::repeat(0u8).take(repeat));
            }
            other => return err(format!("invalid code length symbol {other}")),
        }
    }

    if lengths.len() != hlit + hdist {
        return err("code length overflow");
    }

    let litlen = Huffman::build(&lengths[..hlit])?;
    let dist = Huffman::build(&lengths[hlit..])?;
    Ok((litlen, dist))
}

fn inflate_huffman(
    reader: &mut BitReader<'_>,
    output: &mut Vec<u8>,
    litlen: &Huffman,
    dist: &Huffman,
) -> Result<()> {
    loop {
        let symbol = litlen.decode(reader)?;
        match symbol {
            0..=255 => output.push(symbol as u8),
            256 => return Ok(()),
            257..=285 => {
                let index = (symbol - 257) as usize;
                let length =
                    LENGTH_BASE[index] as usize + reader.bits(LENGTH_EXTRA[index] as u32)? as usize;

                let dist_symbol = dist.decode(reader)? as usize;
                if dist_symbol >= DIST_BASE.len() {
                    return err("invalid distance code");
                }
                let distance = DIST_BASE[dist_symbol] as usize
                    + reader.bits(DIST_EXTRA[dist_symbol] as u32)? as usize;
                if distance == 0 || distance > output.len() {
                    return err("invalid back-reference distance");
                }

                let start = output.len() - distance;
                for offset in 0..length {
                    let byte = output[start + offset];
                    output.push(byte);
                }
            }
            other => return err(format!("invalid literal/length symbol {other}")),
        }
    }
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
    fn round_trips_stored_blocks() {
        for size in [0usize, 1, 7, 4096, 70_000] {
            let data: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
            let compressed = compress(&data);
            assert_eq!(decompress(&compressed).unwrap(), data, "size {size}");
        }
    }

    #[test]
    fn decodes_go_gzip_of_short_text() {
        // `gzip.Write([]byte("hello hello ..."))` from Go 1.22.
        let compressed = hex("1f8b08000000000000ffca48cdc9c957209a04040000ffff6a762cb92f000000");
        assert_eq!(
            decompress(&compressed).unwrap(),
            b"hello hello hello hello hello hello hello hello"
        );
    }

    #[test]
    fn decodes_go_gzip_of_longer_text() {
        let compressed = hex(
            "1f8b08000000000000ff2ac94855282ccd4cce56482aca2fcf5348cbaf50c82acd2d2856\
             c82f4b2d520049e72456552aa4e4a75b2b90a218100000ffffaa2b314959000000",
        );
        assert_eq!(
            decompress(&compressed).unwrap(),
            b"the quick brown fox jumps over the lazy dog; the quick brown fox jumps over the lazy dog;"
        );
    }

    #[test]
    fn decodes_go_gzip_repetitive_text() {
        // Long run of the same byte exercises back-references.
        let compressed = hex("1f8b08000000000000ff4aa43200040000ffff7d8d991a50000000");
        assert_eq!(decompress(&compressed).unwrap(), vec![b'a'; 80]);
    }

    #[test]
    fn rejects_bad_magic() {
        assert!(decompress(b"not gzip").is_err());
    }
}
