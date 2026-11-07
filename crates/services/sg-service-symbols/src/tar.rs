#![forbid(unsafe_code)]

//! Async tar reader for `gitserver.Archive` streams.
//!
//! Mirrors `fetcher.readTar`: directories and global headers are skipped,
//! files larger than the limit are forwarded with empty contents, and pax/GNU
//! long-name metadata is honoured.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;

const BLOCK: usize = 512;
const MAX_METADATA_SIZE: u64 = 1 << 20;

#[derive(Debug, PartialEq, Eq)]
pub struct ParseRequest {
    pub path: String,
    pub data: Vec<u8>,
}

/// Read a tar stream, forwarding each file to `tx`. Returns when the stream
/// ends or the receiver is dropped.
pub async fn read_tar<R>(
    mut reader: R,
    max_file_size: u64,
    tx: &mpsc::Sender<ParseRequest>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut pending_path: Option<String> = None;

    loop {
        let mut header = [0u8; BLOCK];
        if !read_block(&mut reader, &mut header).await? {
            return Ok(());
        }
        if header.iter().all(|byte| *byte == 0) {
            return Ok(());
        }

        let size = parse_size(&header[124..136])?;
        let typeflag = header[156];

        match typeflag {
            b'L' => {
                let payload = read_metadata(&mut reader, size).await?;
                pending_path = Some(trim_nul(&payload));
                continue;
            }
            b'x' => {
                let payload = read_metadata(&mut reader, size).await?;
                if let Some(path) = parse_pax_path(&payload) {
                    pending_path = Some(path);
                }
                continue;
            }
            b'g' => {
                skip_payload(&mut reader, size).await?;
                continue;
            }
            _ => {}
        }

        let path = pending_path.take().unwrap_or_else(|| header_path(&header));

        // Directories and links carry no file content.
        if matches!(typeflag, b'5' | b'2' | b'1') {
            skip_payload(&mut reader, size).await?;
            continue;
        }

        if size > max_file_size {
            // Baseline forwards the path with empty contents so that the file
            // is visible to the parser but contributes no symbols.
            skip_payload(&mut reader, size).await?;
            if tx
                .send(ParseRequest {
                    path,
                    data: Vec::new(),
                })
                .await
                .is_err()
            {
                return Ok(());
            }
            continue;
        }

        let data = read_payload(&mut reader, size).await?;
        if tx.send(ParseRequest { path, data }).await.is_err() {
            return Ok(());
        }
    }
}

async fn read_block<R>(reader: &mut R, block: &mut [u8; BLOCK]) -> io::Result<bool>
where
    R: AsyncRead + Unpin,
{
    let mut read = 0;
    while read < BLOCK {
        match reader.read(&mut block[read..]).await? {
            0 if read == 0 => return Ok(false),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated tar header",
                ))
            }
            n => read += n,
        }
    }
    Ok(true)
}

async fn read_payload<R>(reader: &mut R, size: u64) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut data = vec![0u8; size as usize];
    reader.read_exact(&mut data).await?;
    skip_padding(reader, size).await?;
    Ok(data)
}

async fn read_metadata<R>(reader: &mut R, size: u64) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    if size > MAX_METADATA_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tar metadata entry too large",
        ));
    }
    read_payload(reader, size).await
}

async fn skip_payload<R>(reader: &mut R, size: u64) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut remaining = size;
    let mut scratch = [0u8; 8192];
    while remaining > 0 {
        let chunk = remaining.min(scratch.len() as u64) as usize;
        reader.read_exact(&mut scratch[..chunk]).await?;
        remaining -= chunk as u64;
    }
    skip_padding(reader, size).await
}

async fn skip_padding<R>(reader: &mut R, size: u64) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let padding = (BLOCK as u64 - size % BLOCK as u64) % BLOCK as u64;
    if padding > 0 {
        let mut scratch = [0u8; BLOCK];
        reader.read_exact(&mut scratch[..padding as usize]).await?;
    }
    Ok(())
}

fn parse_size(field: &[u8]) -> io::Result<u64> {
    // GNU base-256 encoding sets the high bit of the first byte.
    if field[0] & 0x80 != 0 {
        let mut value: u64 = (field[0] & 0x7f) as u64;
        for byte in &field[1..] {
            value = value
                .checked_shl(8)
                .and_then(|v| v.checked_add(*byte as u64))
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "tar size overflow"))?;
        }
        return Ok(value);
    }

    let text = trim_nul(field);
    let text = text.trim();
    if text.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(text, 8)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("bad tar size: {err}")))
}

fn header_path(header: &[u8; BLOCK]) -> String {
    let name = trim_nul(&header[0..100]);
    let prefix = trim_nul(&header[345..500]);
    if prefix.is_empty() {
        name
    } else {
        format!("{prefix}/{name}")
    }
}

fn parse_pax_path(payload: &[u8]) -> Option<String> {
    for line in payload.split(|byte| *byte == b'\n') {
        let line = trim_nul(line);
        if let Some(path) = line.strip_prefix("path=") {
            return Some(path.to_owned());
        }
        // pax records are `"<len> key=value"`.
        if let Some((_, rest)) = line.split_once(' ') {
            if let Some(path) = rest.strip_prefix("path=") {
                return Some(path.to_owned());
            }
        }
    }
    None
}

fn trim_nul(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(name: &str, size: usize, typeflag: u8) -> [u8; BLOCK] {
        let mut header = [0u8; BLOCK];
        header[..name.len()].copy_from_slice(name.as_bytes());
        let size_octal = format!("{size:o}");
        header[124..124 + size_octal.len()].copy_from_slice(size_octal.as_bytes());
        header[156] = typeflag;
        header
    }

    fn archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buffer = Vec::new();
        for (name, data) in entries {
            buffer.extend_from_slice(&header(name, data.len(), b'0'));
            buffer.extend_from_slice(data);
            let padding = (BLOCK - data.len() % BLOCK) % BLOCK;
            buffer.extend(std::iter::repeat_n(0, padding));
        }
        buffer.extend(std::iter::repeat_n(0, BLOCK * 2));
        buffer
    }

    #[tokio::test]
    async fn reads_regular_files() {
        let data = archive(&[("a.go", b"package a\n"), ("dir/b.txt", b"hi")]);
        let (tx, mut rx) = mpsc::channel(8);
        let read =
            tokio::spawn(async move { read_tar(std::io::Cursor::new(data), 1024, &tx).await });

        let mut entries = Vec::new();
        while let Some(request) = rx.recv().await {
            entries.push(request);
        }
        read.await.expect("join").expect("read tar");
        assert_eq!(
            entries,
            vec![
                ParseRequest {
                    path: "a.go".into(),
                    data: b"package a\n".to_vec()
                },
                ParseRequest {
                    path: "dir/b.txt".into(),
                    data: b"hi".to_vec()
                },
            ]
        );
    }

    #[tokio::test]
    async fn skips_oversized_file_contents() {
        let data = archive(&[("big.go", b"0123456789")]);
        let (tx, mut rx) = mpsc::channel(8);
        let read = tokio::spawn(async move { read_tar(std::io::Cursor::new(data), 4, &tx).await });

        let request = rx.recv().await.expect("entry");
        assert_eq!(request.path, "big.go");
        assert!(request.data.is_empty());
        read.await.expect("join").expect("read tar");
    }

    #[tokio::test]
    async fn honours_pax_path_override() {
        let long_path = "a/very/long/path/that/does/not/fit/in/the/header/name/field.go";
        let pax = format!("path={long_path}\n");
        let mut data = Vec::new();
        data.extend_from_slice(&header("pax", pax.len(), b'x'));
        data.extend_from_slice(pax.as_bytes());
        let padding = (BLOCK - pax.len() % BLOCK) % BLOCK;
        data.extend(std::iter::repeat_n(0, padding));
        data.extend_from_slice(&header("short.go", 1, b'0'));
        data.extend_from_slice(b"x");
        data.extend(std::iter::repeat_n(0, BLOCK - 1));
        data.extend(std::iter::repeat_n(0, BLOCK * 2));

        let (tx, mut rx) = mpsc::channel(8);
        read_tar(std::io::Cursor::new(data), 1024, &tx)
            .await
            .expect("read tar");
        let request = rx.recv().await.expect("entry");
        assert_eq!(request.path, long_path);
    }
}
