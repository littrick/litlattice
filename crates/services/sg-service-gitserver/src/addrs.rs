//! Port of `internal/gitserver/connection` address computation
//! (`NewGitserverAddresses`, `AddrForRepo`, `hostnameMatch`).
//!
//! The implementation lives in `sg-git` because repo-updater must compute the
//! same shard for a repository; this module keeps the historical import path
//! for the gitserver service.

pub use sg_git::addresses::{hostname_match, GitserverAddresses};
