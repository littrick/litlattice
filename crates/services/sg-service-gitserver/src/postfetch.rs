//! Post-fetch bookkeeping from `cmd/gitserver/internal/postfetch.go`.
//!
//! Only `setLastChanged` is ported so far: it maintains `sg_refhash_v2` in
//! the git directory, whose mtime approximates the last time a fetch changed
//! the set of refs. The file's content is the hash reported by
//! [`crate::git::GitRepo::ref_hash`], so an unchanged ref set leaves the file
//! (and its timestamp) alone.

use std::fs::FileTimes;
use std::io::Write;
use std::path::Path;
use std::time::SystemTime;

use crate::git::{GitError, GitRepo};

/// Port of `fileutil.UpdateFileIfDifferent`: write `content` to `path` unless
/// the file already contains exactly those bytes. Writes go through a
/// temporary file in the same directory, are fsynced, renamed into place, and
/// keep the existing file's permissions (chmod failures are ignored).
pub fn update_file_if_different(path: &Path, content: &[u8]) -> std::io::Result<bool> {
    match std::fs::read(path) {
        Ok(current) if current == content => return Ok(false),
        Ok(_) => {}
        // A missing file is written like `os.ReadFile` returning `IsNotExist`.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let prefix = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut temp = tempfile::Builder::new().prefix(&prefix).tempfile_in(dir)?;
    temp.as_file_mut().write_all(content)?;
    temp.as_file().sync_all()?;

    // Preserve permissions; the baseline silently ignores failures.
    if let Ok(metadata) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(temp.path(), metadata.permissions());
    }

    // The `NamedTempFile` drop removes the temp path; after a successful
    // rename it no longer exists, which the drop ignores (same as the
    // baseline's best-effort `defer os.Remove`).
    rename_and_sync(temp.path(), path)?;
    Ok(true)
}

/// Port of `fileutil.RenameAndSync` (posix): rename, then fsync the new
/// parent directory so the rename survives a crash.
pub(crate) fn rename_and_sync(old_path: &Path, new_path: &Path) -> std::io::Result<()> {
    std::fs::rename(old_path, new_path).map_err(|err| {
        std::io::Error::new(
            err.kind(),
            format!("failed to rename {:?} to {:?}: {err}", old_path, new_path),
        )
    })?;
    let parent = new_path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)?.sync_all()
}

/// Port of `setLastChanged`: refresh `sg_refhash_v2` after a fetch.
///
/// The mtime is the time the ref set last changed. A newly created file gets
/// the timestamp of the most recent commit instead (or the current time while
/// the repository is empty), so a repository's age is not reset by the first
/// `git fetch` that happens to run today.
pub async fn set_last_changed(git_dir: &Path, backend: &GitRepo) -> Result<(), GitError> {
    let hash_file = git_dir.join("sg_refhash_v2");

    // Best effort delete the old refhash file.
    let _ = std::fs::remove_file(git_dir.join("sg_refhash"));

    let hash = backend.ref_hash().await.map_err(|err| {
        GitError::Message(format!(
            "computing ref hash failed for {}: {}",
            git_dir.display(),
            err.message()
        ))
    })?;

    // Only a file we are about to create gets the latest-commit timestamp; an
    // existing file keeps the write time of the update, i.e. the last change.
    let stamp = match std::fs::metadata(&hash_file) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            match backend.latest_commit_timestamp().await {
                Ok(stamp) => Some(stamp),
                Err(err) => {
                    tracing::warn!(
                        error = %err.message(),
                        "failed to get latest commit timestamp, using current time"
                    );
                    Some(SystemTime::now())
                }
            }
        }
        _ => None,
    };

    update_file_if_different(&hash_file, &hash).map_err(|err| {
        GitError::Message(format!("failed to update {}: {err}", hash_file.display()))
    })?;

    if let Some(stamp) = stamp {
        let result = std::fs::OpenOptions::new()
            .write(true)
            .open(&hash_file)
            .and_then(|file| {
                file.set_times(FileTimes::new().set_accessed(stamp).set_modified(stamp))
            });
        if let Err(err) = result {
            return Err(GitError::Message(format!(
                "failed to set mtime to the lastest commit timestamp for {}: {err}",
                git_dir.display()
            )));
        }
    }

    Ok(())
}

/// Which side owns `git gc --auto` (baseline `gitGCMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcMode {
    /// Neither janitor nor `sg maintenance`: leave git's configuration alone.
    GitAutoGc,
    /// Git gc runs from the janitor; `gc.auto` must be unset so a fetch does
    /// not trigger a concurrent gc.
    JanitorAutoGc,
    /// `sg maintenance` owns repacking; keep `git gc --auto` disabled.
    Maintenance,
}

/// `SRC_ENABLE_GC_AUTO` / `SRC_ENABLE_SG_MAINTENANCE`, with the baseline's
/// `strconv.ParseBool` semantics (unparseable values are false).
pub fn gc_mode() -> GcMode {
    fn parse(value: Option<String>, default: &str) -> bool {
        let raw = value.unwrap_or_else(|| default.to_owned());
        matches!(raw.to_ascii_lowercase().as_str(), "1" | "t" | "true")
    }

    let enable_gc_auto = parse(std::env::var("SRC_ENABLE_GC_AUTO").ok(), "true");
    let enable_sg_maintenance = parse(std::env::var("SRC_ENABLE_SG_MAINTENANCE").ok(), "false");
    if enable_gc_auto && !enable_sg_maintenance {
        GcMode::JanitorAutoGc
    } else if enable_sg_maintenance && !enable_gc_auto {
        GcMode::Maintenance
    } else {
        GcMode::GitAutoGc
    }
}

/// `gitSetAutoGC`: unset `gc.auto` when the janitor manages gc, disable it
/// when `sg maintenance` does, and leave it alone otherwise.
async fn set_auto_gc(backend: &GitRepo) -> Result<(), GitError> {
    match gc_mode() {
        GcMode::GitAutoGc | GcMode::JanitorAutoGc => backend.config_unset("gc.auto").await,
        GcMode::Maintenance => backend.config_set("gc.auto", "0").await,
    }
}

/// Port of `SetGitAttributes`: write `info/attributes` so `git archive` keeps
/// every file (`.gitattributes` `export-ignore` otherwise drops files).
pub fn set_git_attributes(git_dir: &Path) -> std::io::Result<()> {
    let info_dir = git_dir.join("info");
    match std::fs::create_dir(&info_dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => {
            return Err(std::io::Error::new(
                err.kind(),
                format!("failed to set git attributes: {err}"),
            ))
        }
    }

    update_file_if_different(
        &info_dir.join("attributes"),
        b"# Managed by Sourcegraph gitserver.\n\n# We want every file to be present in git archive.\n* -export-ignore\n",
    )
    .map(|_| ())
    .map_err(|err| {
        std::io::Error::new(
            err.kind(),
            format!("failed to set git attributes: {err}"),
        )
    })
}

/// The refs `RemoveBadRefs` deletes: every case variant of `HEAD`.
fn bad_refs() -> Vec<String> {
    (0u8..(1 << 4))
        .map(|bits| {
            let mut name: Vec<u8> = b"HEAD".to_vec();
            for (index, byte) in name.iter_mut().enumerate() {
                if bits & (1 << index) != 0 {
                    *byte = byte.to_ascii_lowercase();
                }
            }
            String::from_utf8(name).expect("ascii")
        })
        .collect()
}

/// Port of `RemoveBadRefs`: delete branches/tags named like `HEAD` so git
/// stops warning about ambiguous refnames. Exit code 1 ("none of them exist")
/// is the expected case and not an error.
pub async fn remove_bad_refs(git_dir: &Path) -> Result<(), GitError> {
    let refs = bad_refs();
    let mut errors: Vec<String> = Vec::new();
    // `branch -D` force-deletes; `tag` only has the lowercase `-d`.
    for (subcommand, flag) in [("branch", "-D"), ("tag", "-d")] {
        let mut args = vec![subcommand, flag];
        args.extend(refs.iter().map(String::as_str));
        let output = tokio::process::Command::new("git")
            .args(&args)
            .current_dir(git_dir)
            .env("GIT_DIR", git_dir)
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .map_err(|err| GitError::Message(format!("failed to run git: {err}")))?;
        // `dir.Set` inherits the parent environment, so no `env_clear` here.
        match output.status.code() {
            Some(1) | Some(0) => {}
            _ => errors.push(
                String::from_utf8_lossy(&output.stderr)
                    .trim_end()
                    .to_owned(),
            ),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(GitError::Message(errors.join("\n")))
    }
}

/// Port of `postRepoFetchActions`: run after a clone or fetch, accumulating as
/// many successful steps as possible. `SetRepoSize` is skipped until the
/// `gitserver_repos` backend exists.
pub async fn post_fetch_actions(git_dir: &Path, repo: &str) -> Result<(), GitError> {
    let mut errors: Vec<String> = Vec::new();

    if let Err(err) = remove_bad_refs(git_dir).await {
        errors.push(format!(
            "failed to remove bad refs for repo {:?}: {}",
            repo,
            err.message()
        ));
    }

    let backend = GitRepo::new(git_dir);
    if let Err(err) = backend.config_set("sourcegraph.type", "git").await {
        errors.push(format!(
            "failed to set repository type for repo {:?}: {}",
            repo,
            err.message()
        ));
    }

    if let Err(err) = set_git_attributes(git_dir) {
        errors.push(format!("setting git attributes: {err}"));
    }

    if let Err(err) = set_auto_gc(&backend).await {
        errors.push(format!("setting git gc mode: {}", err.message()));
    }

    if let Err(err) = set_last_changed(git_dir, &backend).await {
        errors.push(format!(
            "failed to update last changed time: {}",
            err.message()
        ));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(GitError::Message(errors.join("\n")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn git(dir: &Path, args: &[&str]) -> std::process::Output {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("run git");
        assert!(output.status.success(), "git {args:?} failed");
        output
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    #[test]
    fn updates_files_only_when_the_content_changes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("sg_refhash_v2");

        assert!(update_file_if_different(&path, b"abc").expect("create"));
        assert_eq!(std::fs::read(&path).expect("read"), b"abc");

        // Same bytes: no write, so the mtime is untouched.
        let before = std::fs::metadata(&path)
            .expect("stat")
            .modified()
            .expect("mtime");
        assert!(!update_file_if_different(&path, b"abc").expect("same"));
        assert_eq!(
            std::fs::metadata(&path)
                .expect("stat")
                .modified()
                .expect("mtime"),
            before
        );

        // Permissions of the existing file are carried over to the update.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).expect("chmod");
        assert!(update_file_if_different(&path, b"def").expect("update"));
        assert_eq!(std::fs::read(&path).expect("read"), b"def");
        assert_eq!(
            std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777,
            0o640
        );
    }

    #[tokio::test]
    async fn set_last_changed_tracks_the_ref_set() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("repo.git");
        std::fs::create_dir_all(&dir).expect("mkdir");
        git(&dir, &["init", "--bare", "-q"]);
        let repo = GitRepo::new(&dir);

        // The legacy file is removed, and an empty repository hashes the empty
        // ref listing; its first timestamp is "now" (no commits yet).
        std::fs::write(dir.join("sg_refhash"), b"old").expect("write");
        set_last_changed(&dir, &repo).await.expect("first run");
        assert!(!dir.join("sg_refhash").exists());
        let hash_file = dir.join("sg_refhash_v2");
        assert_eq!(
            std::fs::read_to_string(&hash_file).expect("read"),
            sha256_hex(b"")
        );

        // A changed ref set rewrites the file and bumps its mtime. Tags may
        // point at any object, so a blob suffices without building a commit.
        let blob = String::from_utf8(git(&dir, &["hash-object", "-w", "--stdin"]).stdout)
            .expect("utf8")
            .trim()
            .to_owned();
        git(&dir, &["update-ref", "refs/tags/v1", &blob]);
        let before = std::fs::metadata(&hash_file)
            .expect("stat")
            .modified()
            .expect("mtime");
        set_last_changed(&dir, &repo).await.expect("second run");
        let updated = std::fs::read_to_string(&hash_file).expect("read");
        assert_ne!(updated, sha256_hex(b""));
        assert!(
            std::fs::metadata(&hash_file)
                .expect("stat")
                .modified()
                .expect("mtime")
                >= before
        );
    }
}
