#![forbid(unsafe_code)]

//! Shared access to prepared archive zip files (`zipcache.go`).
//!
//! The baseline memory-maps each zip. The pilot keeps a file handle and reads
//! entries with `pread` (`FileExt::read_exact_at`): safe Rust, bounded memory,
//! and the same one-copy-per-search-worker cost for the ≤2 MiB files that can
//! carry content.

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use std::os::unix::fs::FileExt;

use zip::CompressionMethod;

const SHARDS: usize = 64;

/// One file inside a prepared zip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SrcFile {
    pub name: String,
    pub off: u64,
    pub len: u32,
}

/// A single zip file, shared between concurrent searches.
#[derive(Debug)]
pub struct ZipFile {
    files: Vec<SrcFile>,
    max_len: usize,
    file: File,
    path: PathBuf,
}

impl ZipFile {
    /// Files ordered by data offset (the writer emits them sequentially).
    pub fn files(&self) -> &[SrcFile] {
        &self.files
    }

    /// Largest file length, used to size the case-folding scratch buffer.
    pub fn max_len(&self) -> usize {
        self.max_len
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The entry with the given repo-relative path, if present.
    pub fn file_by_name(&self, name: &str) -> Option<&SrcFile> {
        // Paths are stored in offset order, not sorted by name; structural
        // search does one lookup per comby result, which is rare enough that a
        // linear scan beats keeping a second index.
        self.files.iter().find(|file| file.name == name)
    }

    /// `zip.Reader.Open` by name, used by structural search.
    pub fn data_by_name(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        match self.file_by_name(name) {
            Some(file) => self.data_for(file).map(Some),
            None => Ok(None),
        }
    }

    /// `zipFile.DataFor`: the contents are copies so they may outlive the
    /// cache entry.
    pub fn data_for(&self, file: &SrcFile) -> io::Result<Vec<u8>> {
        let mut data = vec![0u8; file.len as usize];
        if !data.is_empty() {
            self.file.read_exact_at(&mut data, file.off)?;
        }
        Ok(data)
    }
}

/// Failure to read a prepared zip.
#[derive(Debug)]
pub enum ZipReadError {
    /// The file is not a valid stored-entry zip; the caller may delete and
    /// refetch it (`getZipFileWithRetry`).
    Invalid(String),
    Other(String),
}

impl fmt::Display for ZipReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Other(message) => f.write_str(message),
        }
    }
}

impl ZipReadError {
    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid(_))
    }
}

/// 64-shard cache, mirroring the baseline's lock sharding.
#[derive(Debug, Default)]
pub struct ZipCache {
    shards: Vec<Mutex<HashMap<PathBuf, Arc<ZipFile>>>>,
}

impl ZipCache {
    pub fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    fn shard_for(&self, path: &Path) -> &Mutex<HashMap<PathBuf, Arc<ZipFile>>> {
        // FNV-1a over the path bytes; the baseline uses FNV-32.
        let mut hash: u32 = 2_166_136_261;
        for byte in path.as_os_str().as_encoded_bytes() {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(16_777_619);
        }
        &self.shards[(hash as usize) % SHARDS]
    }

    /// `zipCache.Get`. The returned `Arc` keeps the file readable even if the
    /// entry is evicted from the cache meanwhile.
    pub fn get(&self, path: &Path) -> Result<Arc<ZipFile>, ZipReadError> {
        let mut shard = self.shard_for(path).lock().expect("zip cache poisoned");
        if let Some(zip) = shard.get(path) {
            return Ok(Arc::clone(zip));
        }

        // Cache miss: populate in-band, which single-flights concurrent reads
        // within this process (the disk-level single-flight is in `Store`).
        let zip = Arc::new(read_zip_file(path)?);
        shard.insert(path.to_path_buf(), Arc::clone(&zip));
        Ok(zip)
    }

    /// `zipCache.delete`: drop the cache entry. In-flight `Arc` clones keep
    /// the file readable; the kernel releases the pages when the last one is
    /// dropped.
    pub fn delete(&self, path: &Path) {
        self.shard_for(path)
            .lock()
            .expect("zip cache poisoned")
            .remove(path);
    }

    /// Number of cached archives (testing helper).
    pub fn count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().expect("zip cache poisoned").len())
            .sum()
    }
}

fn read_zip_file(path: &Path) -> Result<ZipFile, ZipReadError> {
    let file = File::open(path)
        .map_err(|err| ZipReadError::Other(format!("open {}: {err}", path.display())))?;
    let metadata = file
        .metadata()
        .map_err(|err| ZipReadError::Other(format!("stat {}: {err}", path.display())))?;

    // The archive reader gets its own handle so `file` stays usable for
    // positional reads.
    let archive_file = file
        .try_clone()
        .map_err(|err| ZipReadError::Other(format!("clone {}: {err}", path.display())))?;
    let mut archive = zip::ZipArchive::new(archive_file).map_err(|err| match err {
        zip::result::ZipError::Io(io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof => {
            ZipReadError::Invalid(format!(
                "not a valid zip file: {}: {io_err}",
                path.display()
            ))
        }
        err => ZipReadError::Invalid(format!("not a valid zip file: {}: {err}", path.display())),
    })?;

    let mut files = Vec::with_capacity(archive.len());
    let mut max_len = 0usize;
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .map_err(|err| ZipReadError::Invalid(format!("read {}: {err}", path.display())))?;
        if entry.compression() != CompressionMethod::Stored {
            return Err(ZipReadError::Invalid(format!(
                "file {} stored with compression {:?}, want Stored",
                entry.name(),
                entry.compression()
            )));
        }
        let size = entry.size();
        if size > u64::from(u32::MAX) {
            return Err(ZipReadError::Invalid(format!(
                "file {} has size > 4gb: {size}",
                entry.name()
            )));
        }
        // Keep the baseline's 2 GiB per-entry contract.
        if size > i32::MAX as u64 {
            return Err(ZipReadError::Invalid(format!(
                "file {} has size > 2gb: {size}",
                entry.name()
            )));
        }
        let name = String::from_utf8_lossy(entry.name_raw()).into_owned();
        let len = size as u32;
        max_len = max_len.max(size as usize);
        files.push(SrcFile {
            name,
            off: entry.data_start(),
            len,
        });
    }

    // We wrote this zip ourselves in one pass, but sort to be sure that reads
    // are sequential.
    files.sort_by_key(|file| file.off);

    let _ = metadata;
    Ok(ZipFile {
        files,
        max_len,
        file,
        path: path.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn write_zip(path: &Path, files: &[(&str, &[u8])]) {
        let file = File::create(path).expect("create");
        let mut writer = zip::ZipWriter::new(file);
        for (name, data) in files {
            writer
                .start_file(
                    *name,
                    zip::write::SimpleFileOptions::default()
                        .compression_method(CompressionMethod::Stored),
                )
                .expect("start file");
            writer.write_all(data).expect("write");
        }
        writer.finish().expect("finish");
    }

    #[test]
    fn reads_stored_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("archive.zip");
        write_zip(&path, &[("a.txt", b"hello\n"), ("b.txt", b"")]);

        let cache = ZipCache::new();
        let zip = cache.get(&path).expect("read zip");
        assert_eq!(zip.files().len(), 2);
        assert_eq!(zip.max_len(), 6);
        assert_eq!(zip.files()[0].name, "a.txt");
        assert_eq!(zip.data_for(&zip.files()[0]).expect("data"), b"hello\n");
        assert_eq!(zip.data_for(&zip.files()[1]).expect("data"), b"");
        // Second get reuses the same Arc.
        let again = cache.get(&path).expect("cached");
        assert!(Arc::ptr_eq(&zip, &again));
        assert_eq!(cache.count(), 1);
        cache.delete(&path);
        assert_eq!(cache.count(), 0);
    }

    #[test]
    fn rejects_compressed_and_invalid_archives() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.zip");
        std::fs::write(&path, b"not a zip").expect("write");
        assert!(ZipCache::new()
            .get(&path)
            .expect_err("invalid")
            .is_invalid());
    }
}
