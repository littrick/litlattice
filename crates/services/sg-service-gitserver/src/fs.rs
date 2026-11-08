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

/// Port of `DirSize`/`dirSize`: the total size in bytes of the regular files
/// under `dir`. `BestEffortWalk` ignores unreadable entries, so only a failure
/// on the root itself is reported. The directory must be absolute.
pub fn dir_size(dir: &Path) -> std::io::Result<i64> {
    if !dir.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "dir must be absolute",
        ));
    }
    let mut size = 0i64;
    walk_sizes(dir, &mut size)?;
    Ok(size)
}

fn walk_sizes(dir: &Path, size: &mut i64) -> std::io::Result<()> {
    // `filepath.WalkDir` never follows symlinks; `read_dir` entries and
    // `DirEntry::metadata` are lstat-like on Unix.
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        // Unreadable entries are skipped (`BestEffortWalk`).
        let Ok(entry) = entry else { continue };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            // A directory that cannot be read is skipped, like WalkDir's
            // per-entry error being swallowed.
            let _ = walk_sizes(&entry.path(), size);
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        *size += metadata.len() as i64;
    }
    Ok(())
}

/// `p4HomeName`: the Perforce home directory is ignored by listing, like
/// `.tmp`.
pub const P4_HOME_NAME: &str = ".p4home";

/// `ignorePath`: top-level entries of the repos dir that are not repositories.
pub fn ignore_path(repos_dir: &Path, path: &Path) -> bool {
    if path.parent() != Some(repos_dir) {
        return false;
    }
    path.file_name()
        .map(|name| {
            let name = name.to_string_lossy();
            name.starts_with(TEMP_DIR_NAME) || name.starts_with(P4_HOME_NAME)
        })
        .unwrap_or(false)
}

/// `CanonicalPath`: the path with the repos dir prefix removed, as returned by
/// `ListRepositories` (e.g. `/example.com/owner/repo/.git`).
pub fn canonical_path(repos_dir: &Path, git_dir: &Path) -> String {
    match git_dir.strip_prefix(repos_dir) {
        Ok(rest) => format!("/{}", rest.to_string_lossy()),
        Err(_) => git_dir.to_string_lossy().into_owned(),
    }
}

/// `repoNameFromDir`: infer the repository name from a `<repos_dir>/<name>/.git`
/// directory, then normalize it like `protocol.NormalizeRepo`.
pub fn resolve_repo_name(repos_dir: &Path, git_dir: &Path) -> String {
    let parent = git_dir.parent().unwrap_or(git_dir);
    let name = match parent.strip_prefix(repos_dir) {
        Ok(rest) => rest.to_string_lossy().into_owned(),
        Err(_) => parent.to_string_lossy().into_owned(),
    };
    let name = name.trim_matches('/');
    crate::repo::normalize_repo(name)
}

/// `ForEachRepo`: walk the repos dir (lexically, like `filepath.WalkDir`) and
/// visit every `.git` directory. `visit` returning `true` stops the walk.
///
/// Errors below the root are ignored, exactly like `BestEffortWalk`.
pub fn for_each_repo<F>(repos_dir: &Path, mut visit: F)
where
    F: FnMut(&str, &Path) -> bool,
{
    walk_repos(repos_dir, repos_dir, &mut visit);
}

fn walk_repos(dir: &Path, repos_dir: &Path, visit: &mut impl FnMut(&str, &Path) -> bool) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        // `BestEffortWalk` discards walk errors, including the root's.
        return false;
    };
    let mut paths: Vec<_> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    for path in paths {
        if ignore_path(repos_dir, &path) {
            continue;
        }
        let Ok(file_type) = std::fs::symlink_metadata(&path).map(|meta| meta.file_type()) else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        if path.file_name() == Some(std::ffi::OsStr::new(".git")) {
            let name = resolve_repo_name(repos_dir, &path);
            if visit(&name, &path) {
                return true;
            }
            continue;
        }
        if walk_repos(&path, repos_dir, visit) {
            return true;
        }
    }
    false
}

/// `removeRepoDirectory`: move the repository out of the tree atomically, then
/// delete it and any now-empty parent directories.
///
/// Only the atomic rename can fail the call; cleanup problems are logged and
/// ignored, matching the baseline.
pub fn remove_repo_directory(repos_dir: &Path, git_dir: &Path) -> std::io::Result<()> {
    match std::fs::metadata(git_dir) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        // Other stat errors are ignored; the rename below reports the real
        // problem (same as `os.IsNotExist` handling in the baseline).
        Err(_) => {}
    }

    let tmp = temp_dir(repos_dir, "delete-repo")?;
    // The deferred cleanup also runs when the rename fails, like the Go
    // `defer os.RemoveAll(tmp)`.
    let result = crate::postfetch::rename_and_sync(git_dir, &tmp.join("repo"));
    if let Err(err) = std::fs::remove_dir_all(&tmp) {
        tracing::warn!(
            dir = %tmp.display(),
            error = %err,
            "failed to cleanup after removing dir"
        );
    }
    result?;

    cleanup_empty_parents(repos_dir, git_dir);
    Ok(())
}

fn cleanup_empty_parents(repos_dir: &Path, git_dir: &Path) {
    use std::os::unix::fs::MetadataExt;

    let Ok(root_meta) = std::fs::metadata(repos_dir) else {
        tracing::warn!(
            repos_dir = %repos_dir.display(),
            "failed to stat repos dir"
        );
        return;
    };
    let mut current = git_dir;
    while let Some(parent) = current.parent() {
        if parent == current {
            break;
        }
        current = parent;
        let meta = match std::fs::metadata(current) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => {
                tracing::warn!(
                    dir = %current.display(),
                    error = %err,
                    "failed to stat parent directory"
                );
                return;
            }
        };
        // `os.SameFile`: same device and inode.
        if meta.dev() == root_meta.dev() && meta.ino() == root_meta.ino() {
            break;
        }
        // A failure means the directory has other children; stop.
        if std::fs::remove_dir(current).is_err() {
            break;
        }
    }
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
    fn sums_regular_file_sizes_without_following_symlinks() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("repo.git");
        std::fs::create_dir_all(root.join("objects")).expect("mkdir");
        std::fs::write(root.join("HEAD"), b"ref: refs/heads/main").expect("write");
        std::fs::write(root.join("objects").join("blob"), b"12345").expect("write");
        // A symlink is counted by its own (lstat) size, not the target's.
        std::os::unix::fs::symlink(root.join("objects").join("blob"), root.join("link"))
            .expect("symlink");

        let size = dir_size(&root).expect("size");
        let head = std::fs::metadata(root.join("HEAD")).expect("stat").len() as i64;
        let blob = 5i64;
        let link = std::fs::symlink_metadata(root.join("link"))
            .expect("lstat")
            .len() as i64;
        assert_eq!(size, head + blob + link);

        // Relative paths are rejected like `realGitserverFS.DirSize`.
        let err = dir_size(Path::new("relative")).expect_err("relative");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
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
