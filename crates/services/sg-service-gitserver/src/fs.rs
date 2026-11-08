//! Port of the write-path helpers in
//! `cmd/gitserver/internal/gitserverfs/fs.go`: repository layout queries and
//! the staging area for clones (`.tmp` under the repos dir).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::repo::repo_dir;

/// `tempDirName`: clones are staged in `<repos_dir>/.tmp`, which repository
/// listing ignores.
pub const TEMP_DIR_NAME: &str = ".tmp";

/// `repoCloned`: a repository is cloned when `<gitdir>/HEAD` exists.
pub fn repo_cloned(repos_dir: &Path, name: &str) -> std::io::Result<bool> {
    let Some(git_dir) = repo_dir(repos_dir, name) else {
        return Ok(false);
    };
    match std::fs::metadata(git_dir.join("HEAD")) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

/// Port of `tempDir`: `os.MkdirTemp(filepath.Join(reposDir, ".tmp"), prefix)`.
///
/// The directory is created (not just named) and the caller owns removing it.
/// Go uses a random suffix; here the suffix is pid + nanosecond clock +
/// counter, retried on collision.
pub fn temp_dir(repos_dir: &Path, prefix: &str) -> std::io::Result<PathBuf> {
    let root = repos_dir.join(TEMP_DIR_NAME);
    std::fs::create_dir_all(&root)?;

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    for _ in 0..100 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!("{prefix}{}-{nanos}-{counter}", std::process::id()));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "failed to create temporary directory",
    ))
}

/// Removes a staging directory when dropped (`defer os.RemoveAll(tmpDir)`).
pub struct TempDirCleanup(pub PathBuf);

impl Drop for TempDirCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_cloned_repositories_by_head() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path();
        assert!(!repo_cloned(repos_dir, "example.com/owner/repo").expect("query"));

        let git_dir = repo_dir(repos_dir, "example.com/owner/repo").expect("dir");
        std::fs::create_dir_all(&git_dir).expect("mkdir");
        std::fs::write(git_dir.join("HEAD"), b"ref: refs/heads/main").expect("write");
        assert!(repo_cloned(repos_dir, "example.com/owner/repo").expect("query"));
    }

    #[test]
    fn stages_clones_under_the_temp_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path();
        let first = temp_dir(repos_dir, "clone-").expect("temp");
        let second = temp_dir(repos_dir, "clone-").expect("temp");

        assert!(first.starts_with(repos_dir.join(".tmp")));
        assert!(first.is_dir());
        assert_ne!(first, second);
        assert!(first
            .file_name()
            .expect("name")
            .to_string_lossy()
            .starts_with("clone-"));

        drop(TempDirCleanup(first.clone()));
        assert!(!first.exists());
    }
}
