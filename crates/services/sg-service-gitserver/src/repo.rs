//! Baseline `protocol.NormalizeRepo` and `gitserverfs.RepoDir`.

use std::path::{Path, PathBuf};

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
}
