//! Baseline `protocol.NormalizeRepo` and `gitserverfs.RepoDir`.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Port of Go `path.Clean` for slash-separated paths.
fn clean_path(path: &str) -> String {
    // `path.Clean` is byte-oriented; repo names and revision paths are both
    // arbitrary byte strings, so the shared implementation stays on bytes.
    String::from_utf8(clean_path_bytes(path.as_bytes()))
        .expect("cleaning valid UTF-8 yields valid UTF-8")
}

/// Byte-string version of Go `path.Clean`; also used for revision paths,
/// which are not guaranteed to be valid UTF-8.
pub(crate) fn clean_path_bytes(path: &[u8]) -> Vec<u8> {
    if path.is_empty() {
        return b".".to_vec();
    }
    let rooted = path[0] == b'/';
    let mut parts: Vec<&[u8]> = Vec::new();
    for part in path.split(|byte| *byte == b'/') {
        match part {
            b"" | b"." => {}
            b".." => match parts.last() {
                Some(last) if *last != b".." => {
                    parts.pop();
                }
                _ => {
                    if !rooted {
                        parts.push(part);
                    }
                }
            },
            other => parts.push(other),
        }
    }
    let mut out = Vec::new();
    if rooted {
        out.push(b'/');
    }
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            out.push(b'/');
        }
        out.extend_from_slice(part);
    }
    if out.is_empty() {
        out.extend_from_slice(b".");
    }
    out
}

/// Port of `protocol.NormalizeRepo`: clean the path, lowercase the host and
/// lowercase the whole path for GitHub (which is case-insensitive).
pub fn normalize_repo(input: &str) -> String {
    let cleaned = clean_path(&format!("/{input}"));
    let cleaned = cleaned.trim_start_matches('/');
    match cleaned.find('/') {
        None => cleaned.to_owned(),
        Some(slash) => {
            let host = cleaned[..slash].to_lowercase();
            let repo_path = &cleaned[slash..];
            if host == "github.com" {
                format!("{host}{}", repo_path.to_lowercase())
            } else {
                format!("{host}{repo_path}")
            }
        }
    }
}

/// Port of `api.UndeletedRepoName`: when a repo is soft-deleted the database
/// renames it to `DELETED-<major>.<minor>-<original>`; gitserver strips that
/// marker (`^DELETED-[0-9]+\.[0-9]+-`) before touching the filesystem.
pub fn undeleted_repo_name(name: &str) -> String {
    let Some(rest) = name.strip_prefix("DELETED-") else {
        return name.to_owned();
    };
    let major = rest.bytes().take_while(u8::is_ascii_digit).count();
    let Some(rest) = rest[major..].strip_prefix('.') else {
        return name.to_owned();
    };
    let minor = rest.bytes().take_while(u8::is_ascii_digit).count();
    let Some(rest) = rest[minor..].strip_prefix('-') else {
        return name.to_owned();
    };
    if major == 0 || minor == 0 {
        return name.to_owned();
    }
    rest.to_owned()
}

/// `<repos_dir>/<normalized repo>/.git`, like `repoDirFromName`.
///
/// Returns `None` when the cleaned name escapes `repos_dir`; the baseline
/// panics in that case because it is unreachable, but a drop-in server must
/// not crash on a hostile repo name.
pub fn repo_dir(repos_dir: &Path, name: &str) -> Option<PathBuf> {
    let name = undeleted_repo_name(name);
    let dir = repos_dir.join(normalize_repo(&name)).join(".git");
    dir.starts_with(repos_dir).then_some(dir)
}

/// Baseline `repoLastFetched` (`serverutil.go`): the mtime of `FETCH_HEAD`,
/// which is the date of the last successful fetch. A repository that has been
/// cloned but never fetched has no `FETCH_HEAD`; the mtime of `HEAD` is used.
pub fn repo_last_fetched(git_dir: &Path) -> std::io::Result<SystemTime> {
    let path = git_dir.join("FETCH_HEAD");
    match std::fs::metadata(&path) {
        Ok(metadata) => metadata.modified(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => stat_mtime(&git_dir.join("HEAD")),
        Err(err) => Err(stat_error(&path, err)),
    }
}

/// Baseline `repoLastChanged` (`serverutil.go`): the mtime of `sg_refhash_v2`
/// (written by `setLastChanged` after a fetch changed the ref set). Until that
/// file exists it is the last-fetched time.
pub fn repo_last_changed(git_dir: &Path) -> std::io::Result<SystemTime> {
    let path = git_dir.join("sg_refhash_v2");
    match std::fs::metadata(&path) {
        Ok(metadata) => metadata.modified(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => repo_last_fetched(git_dir),
        Err(err) => Err(stat_error(&path, err)),
    }
}

fn stat_mtime(path: &Path) -> std::io::Result<SystemTime> {
    std::fs::metadata(path)
        .map_err(|err| stat_error(path, err))?
        .modified()
}

/// Go's `os.Stat` errors read `stat <path>: <reason>`; Rust's `io::Error`
/// displays the opposite order and appends `(os error N)`, so translate the
/// common reasons by hand.
pub(crate) fn stat_error(path: &Path, err: std::io::Error) -> std::io::Error {
    let reason = match err.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory",
        std::io::ErrorKind::PermissionDenied => "permission denied",
        _ => return std::io::Error::new(err.kind(), format!("stat {}: {err}", path.display())),
    };
    std::io::Error::new(err.kind(), format!("stat {}: {reason}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_like_the_baseline() {
        assert_eq!(normalize_repo("foo/bar"), "foo/bar");
        assert_eq!(normalize_repo("/foo/bar/"), "foo/bar");
        assert_eq!(normalize_repo("foo//bar/./baz"), "foo/bar/baz");
        assert_eq!(normalize_repo("foo/../bar"), "bar");
        assert_eq!(normalize_repo("../bar"), "bar");
        assert_eq!(
            normalize_repo("GitHub.com/Owner/Repo"),
            "github.com/owner/repo"
        );
        assert_eq!(
            normalize_repo("gitlab.com/Owner/Repo"),
            "gitlab.com/Owner/Repo"
        );
        assert_eq!(normalize_repo("github.com/../foo/bar"), "foo/bar");
    }

    #[test]
    fn cleans_paths_like_go() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b"."),
            (b".", b"."),
            (b"/", b"/"),
            (b"//", b"/"),
            (b"a//b/./c", b"a/b/c"),
            (b"a/../b", b"b"),
            (b"../a", b"../a"),
            (b"a/../../b", b"../b"),
            (b"/..", b"/"),
            (b"./", b"."),
            (b"dir/", b"dir"),
            (b"dir/sub/", b"dir/sub"),
        ];
        for (input, expected) in cases {
            assert_eq!(clean_path_bytes(input), *expected, "clean {:?}", input);
        }
    }

    #[test]
    fn joins_the_git_directory() {
        let dir = repo_dir(Path::new("/data/repos"), "test/symbols-fixture");
        assert_eq!(
            dir,
            Some(PathBuf::from("/data/repos/test/symbols-fixture/.git"))
        );
        // `..` is resolved by the rooted clean, so the name stays inside.
        assert_eq!(
            repo_dir(Path::new("/data/repos"), "../etc"),
            Some(PathBuf::from("/data/repos/etc/.git"))
        );
        // Soft-deleted names resolve to the original repository.
        assert_eq!(
            repo_dir(
                Path::new("/data/repos"),
                "DELETED-12.0-test/symbols-fixture"
            ),
            Some(PathBuf::from("/data/repos/test/symbols-fixture/.git"))
        );
    }

    #[test]
    fn strips_delete_markers_like_the_baseline() {
        assert_eq!(undeleted_repo_name("DELETED-1.2-foo/bar"), "foo/bar");
        assert_eq!(
            undeleted_repo_name("DELETED-12.0-github.com/owner/repo"),
            "github.com/owner/repo"
        );
        assert_eq!(undeleted_repo_name("DELETED-1.2-"), "");
        // Anything that does not match `^DELETED-[0-9]+\.[0-9]+-` is kept.
        for unchanged in [
            "foo/bar",
            "deleted-1.2-foo",
            "Deleted-1.2-foo",
            "DELETED-x-foo",
            "DELETED-1-foo",
            "DELETED-1.-foo",
            "DELETED-.2-foo",
            "DELETED-1.2foo",
            "foo/DELETED-1.2-bar",
        ] {
            assert_eq!(undeleted_repo_name(unchanged), unchanged);
        }
        // Only the leading marker is removed, once.
        assert_eq!(undeleted_repo_name("DELETED-1.2-3.4-foo"), "3.4-foo");
    }

    #[test]
    fn reports_repo_timestamps_like_the_baseline() {
        fn touch(path: &Path, seconds: u64) -> SystemTime {
            std::fs::write(path, b"").expect("write");
            let time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds);
            std::fs::File::options()
                .write(true)
                .open(path)
                .expect("open")
                .set_modified(time)
                .expect("set mtime");
            time
        }

        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        // Neither FETCH_HEAD nor HEAD exists: the error is propagated.
        assert!(repo_last_fetched(dir).is_err());
        assert!(repo_last_changed(dir).is_err());

        // Cloned but never fetched: last-fetched is HEAD's mtime, and
        // last-changed falls back to it while sg_refhash_v2 is absent.
        let head = touch(&dir.join("HEAD"), 1_600_000_000);
        assert_eq!(repo_last_fetched(dir).expect("fetched"), head);
        assert_eq!(repo_last_changed(dir).expect("changed"), head);

        // After a fetch, FETCH_HEAD wins; sg_refhash_v2 wins once written.
        let fetch_head = touch(&dir.join("FETCH_HEAD"), 1_700_000_000);
        assert_eq!(repo_last_fetched(dir).expect("fetched"), fetch_head);
        assert_eq!(repo_last_changed(dir).expect("changed"), fetch_head);

        let refhash = touch(&dir.join("sg_refhash_v2"), 1_800_000_000);
        assert_eq!(repo_last_changed(dir).expect("changed"), refhash);
        // last-fetched still reports FETCH_HEAD, not the ref hash.
        assert_eq!(repo_last_fetched(dir).expect("fetched"), fetch_head);
    }
}
