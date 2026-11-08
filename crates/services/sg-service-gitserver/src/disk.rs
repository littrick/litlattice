//! Filesystem usage of the repositories volume.
//!
//! Baseline: `internal/diskusage` (used by `gitserverfs.DiskUsage`) and the
//! `DiskInfo` RPC in `cmd/gitserver/internal/server_grpc.go`. Go calls
//! `syscall.Statfs` and reports
//!
//! * `total_space = Blocks * Bsize`
//! * `free_space  = Bfree * Bsize`
//! * `available_space = Bavail * Bsize` (`Available()`, used by metrics)
//! * `percent_used = (total - free) / total * 100`, computed in `float32`.
//!
//! Note that `DiskUsage`/`percent_used` use `Bfree` (all free blocks), while
//! the disk metrics use `Bavail` (blocks available to unprivileged users),
//! so reserved blocks count as used for the RPC but not for the gauges.

use std::io;
use std::path::Path;

/// Snapshot of the filesystem hosting the repositories.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskInfo {
    pub total_space: u64,
    pub free_space: u64,
    /// `diskusage.Available()`: blocks available to unprivileged users.
    pub available_space: u64,
    pub percent_used: f32,
}

/// `diskusage.New(volumePath)` for the filesystem containing `path`.
pub fn disk_usage(path: &Path) -> io::Result<DiskInfo> {
    let stat = rustix::fs::statvfs(path)?;
    // `syscall.Statfs_t.Bsize` on Linux is `f_bsize`; Go multiplies the block
    // counts by it rather than by `f_frsize`.
    let block_size = stat.f_bsize as u64;
    let total_space = stat.f_blocks as u64 * block_size;
    let free_space = stat.f_bfree as u64 * block_size;
    let available_space = stat.f_bavail as u64 * block_size;
    let used = total_space - free_space;
    // Same operations (and `float32` rounding) as the Go baseline.
    let percent_used = (used as f32) / (total_space as f32) * 100.0;
    Ok(DiskInfo {
        total_space,
        free_space,
        available_space,
        percent_used,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_filesystem_usage() {
        let info = disk_usage(Path::new(env!("CARGO_MANIFEST_DIR"))).expect("statvfs");
        assert!(info.total_space > 0);
        assert!(info.free_space <= info.total_space);
        assert!(info.available_space <= info.free_space);
        assert!(info.percent_used >= 0.0 && info.percent_used <= 100.0);
        let expected =
            ((info.total_space - info.free_space) as f32 / info.total_space as f32) * 100.0;
        assert_eq!(info.percent_used, expected);
    }

    #[test]
    fn missing_path_is_an_error() {
        let err = disk_usage(Path::new("/nonexistent/sg-gitserver-disk-test")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
