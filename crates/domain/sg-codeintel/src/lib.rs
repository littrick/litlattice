#![forbid(unsafe_code)]

//! Code-intelligence shared configuration and storage helpers.
//!
//! The first resident is `internal/codeintel/shared/lsifuploadstore`: both
//! `cmd/precise-code-intel-worker` and the `codeintel-uploadstore-expirer`
//! worker job construct the same upload store from the same environment
//! variables, so the loader lives here instead of in either service.

pub mod lsifuploadstore;
pub mod packagefilters;

pub use lsifuploadstore::Config as UploadStoreConfig;
