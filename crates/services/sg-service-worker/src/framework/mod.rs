#![forbid(unsafe_code)]

//! `internal/goroutine`: the background-routine framework.
//!
//! Only the periodic routine is ported so far (9.8b); the DB-queue framework
//! (`internal/workerutil`) follows with the queue job groups.

pub mod periodic;
