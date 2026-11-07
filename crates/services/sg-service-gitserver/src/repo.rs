//! Baseline `protocol.NormalizeRepo` and `gitserverfs.RepoDir`.

use std::path::{Path, PathBuf};

/// Port of Go `path.Clean` for slash-separated paths.
fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => match parts.last() {
                Some(&last) if last != ".." => {
                    parts.pop();
                }
                _ => {
                    if !rooted {
                        parts.push("..");
                    }
                }
            },
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    if rooted {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_owned()
    } else {
        joined
    }
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

/// `<repos_dir>/<normalized repo>/.git`, like `repoDirFromName`.
///
/// Returns `None` when the cleaned name escapes `repos_dir`; the baseline
/// panics in that case because it is unreachable, but a drop-in server must
/// not crash on a hostile repo name.
pub fn repo_dir(repos_dir: &Path, name: &str) -> Option<PathBuf> {
    let dir = repos_dir.join(normalize_repo(name)).join(".git");
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
        assert_eq!(normalize_repo("GitHub.com/Owner/Repo"), "github.com/owner/repo");
        assert_eq!(normalize_repo("gitlab.com/Owner/Repo"), "gitlab.com/Owner/Repo");
        assert_eq!(normalize_repo("github.com/../foo/bar"), "foo/bar");
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
    }
}
