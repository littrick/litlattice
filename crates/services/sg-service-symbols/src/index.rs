#![forbid(unsafe_code)]

//! Build a SQLite symbol index for one `repo@commit`.
//!
//! Mirrors `cmd/symbols/internal/database/writer` + `internal/parser`:
//! stream `gitserver.Archive`, parse files with a bounded pool of ctags
//! processes, and write symbols to a temporary database that is atomically
//! renamed into place.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;

use crate::config::SymbolsConfig;
use crate::ctags::{CtagsProcess, Entry};
use crate::gitserver::{ChangeStatus, GitserverReader};
use crate::language::{self, ParserType};
use crate::store::{self, Symbol};
use crate::tar;

pub struct Indexer {
    config: SymbolsConfig,
    gitserver: Arc<dyn GitserverReader>,
}

impl Indexer {
    pub fn new(config: SymbolsConfig, gitserver: Arc<dyn GitserverReader>) -> Self {
        Self { config, gitserver }
    }

    /// Write the database for `repo@commit` to `db_path`, reusing the newest
    /// database for the repository when a diff is available.
    pub async fn write_database(
        &self,
        repo: &str,
        commit: &str,
        db_path: &Path,
        token: &CancellationToken,
    ) -> Result<(), String> {
        let repo_dir = db_path
            .parent()
            .ok_or_else(|| "database path has no parent directory".to_owned())?;

        let result = match self.incremental_base(repo_dir) {
            Some((newest, old_commit)) => {
                self.write_incrementally(repo, commit, db_path, &newest, &old_commit, token)
                    .await
            }
            None => self.write_fully(repo, commit, db_path, token).await,
        };

        if result.is_err() {
            let _ = std::fs::remove_file(part_path(db_path));
        }
        result
    }

    /// Newest database in the repository directory plus its indexed revision.
    fn incremental_base(&self, repo_dir: &Path) -> Option<(PathBuf, String)> {
        let mut newest: Option<(SystemTime, PathBuf)> = None;
        let entries = std::fs::read_dir(repo_dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("zip") {
                continue;
            }
            if !entry
                .file_type()
                .map(|kind| kind.is_file())
                .unwrap_or(false)
            {
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            if newest
                .as_ref()
                .map(|(time, _)| modified > *time)
                .unwrap_or(true)
            {
                newest = Some((modified, path));
            }
        }
        let (_, newest) = newest?;
        let commit = store::read_commit(&newest)?;
        Some((newest, commit))
    }

    async fn write_fully(
        &self,
        repo: &str,
        commit: &str,
        db_path: &Path,
        token: &CancellationToken,
    ) -> Result<(), String> {
        create_parent(db_path)?;
        let _ = std::fs::remove_file(part_path(db_path));
        let mut writer = self.spawn_writer(db_path.to_owned(), commit.to_owned(), true, Vec::new());
        let parsed = self
            .parse_paths(repo, commit, &[], token, &mut writer)
            .await;
        let written = writer.finish().await;
        match parsed {
            Err(err) => Err(err),
            Ok(()) => written,
        }
    }

    async fn write_incrementally(
        &self,
        repo: &str,
        commit: &str,
        db_path: &Path,
        newest: &Path,
        old_commit: &str,
        token: &CancellationToken,
    ) -> Result<(), String> {
        create_parent(db_path)?;

        let changed = self
            .gitserver
            .changed_files(repo, old_commit, commit)
            .await?;

        let mut added_or_modified = Vec::new();
        let mut all_changed = Vec::new();
        for file in changed {
            match file.status {
                ChangeStatus::Added | ChangeStatus::Modified => {
                    added_or_modified.push(file.path.clone());
                    all_changed.push(file.path);
                }
                ChangeStatus::Deleted => all_changed.push(file.path),
                // A type change does not change file contents.
                ChangeStatus::TypeChanged => {}
            }
        }

        let part = part_path(db_path);
        let _ = std::fs::remove_file(&part);
        std::fs::copy(newest, &part)
            .map_err(|err| format!("copy {} to {}: {err}", newest.display(), part.display()))?;

        let mut writer =
            self.spawn_writer(db_path.to_owned(), commit.to_owned(), false, all_changed);
        let parsed = self
            .parse_paths(repo, commit, &added_or_modified, token, &mut writer)
            .await;
        let written = writer.finish().await;
        match parsed {
            Err(err) => Err(err),
            Ok(()) => written,
        }
    }

    fn spawn_writer(
        &self,
        db_path: PathBuf,
        commit: String,
        full: bool,
        deleted_paths: Vec<String>,
    ) -> WriterHandle {
        let (tx, rx) =
            mpsc::channel::<Result<Symbol, String>>(self.config.request_buffer_size.max(16));
        let final_path = db_path.clone();
        let part = part_path(&db_path);
        let task = tokio::task::spawn_blocking(move || {
            let mut rx = rx;
            let result = if full {
                store::write_full(&part, &commit, &mut rx)
            } else {
                store::write_incremental(&part, &commit, &deleted_paths, &mut rx)
            };
            match result {
                Ok(()) => finish_atomically(&part, &final_path),
                Err(err) => Err(err),
            }
        });
        WriterHandle { tx, task }
    }

    async fn parse_paths(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
        token: &CancellationToken,
        writer: &mut WriterHandle,
    ) -> Result<(), String> {
        let request_buffer_size = self.config.request_buffer_size.max(16);
        let (request_tx, request_rx) = mpsc::channel::<tar::ParseRequest>(request_buffer_size);
        let request_rx = Arc::new(Mutex::new(request_rx));

        let mut workers = JoinSet::new();
        for _ in 0..self.config.ctags_processes.max(1) {
            workers.spawn(parse_worker(
                Arc::clone(&request_rx),
                writer.tx.clone(),
                self.config.clone(),
                token.clone(),
            ));
        }
        drop(request_rx);

        let batches: Vec<Vec<String>> = if paths.is_empty() {
            vec![Vec::new()]
        } else {
            batch_by_total_length(paths, self.config.max_total_paths_length)
        };

        let mut produced = Ok(());
        for batch in batches {
            if token.is_cancelled() {
                produced = Err("canceled".to_owned());
                break;
            }
            let fetched = self.gitserver.fetch_tar(repo, commit, &batch).await;
            let stream = match fetched {
                Ok(stream) => stream,
                Err(err) => {
                    produced = Err(err);
                    break;
                }
            };
            let reader = StreamReader::new(stream);
            let result = tokio::select! {
                result = tar::read_tar(reader, self.config.max_file_size_bytes, &request_tx) => result,
                _ = token.cancelled() => Err(std::io::Error::other("canceled")),
            };
            if let Err(err) = result {
                produced = Err(format!("read archive: {err}"));
                break;
            }
        }
        drop(request_tx);

        while let Some(joined) = workers.join_next().await {
            if let Ok(Err(err)) = joined {
                if produced.is_ok() {
                    produced = Err(err);
                }
            }
        }

        // Make the writer roll back instead of committing a partial index.
        if let Err(err) = &produced {
            let _ = writer.tx.send(Err(err.clone())).await;
        }
        produced
    }
}

struct WriterHandle {
    tx: mpsc::Sender<Result<Symbol, String>>,
    task: tokio::task::JoinHandle<Result<(), String>>,
}

impl WriterHandle {
    async fn finish(self) -> Result<(), String> {
        drop(self.tx);
        self.task.await.map_err(|err| err.to_string())?
    }
}

async fn parse_worker(
    request_rx: Arc<Mutex<mpsc::Receiver<tar::ParseRequest>>>,
    symbol_tx: mpsc::Sender<Result<Symbol, String>>,
    config: SymbolsConfig,
    token: CancellationToken,
) -> Result<(), String> {
    let mut universal: Option<CtagsProcess> = None;
    let mut scip: Option<CtagsProcess> = None;

    loop {
        let request = {
            let mut receiver = request_rx.lock().await;
            receiver.recv().await
        };
        let Some(request) = request else {
            break;
        };
        if token.is_cancelled() {
            break;
        }

        let Some(language) = language::detect(&request.path, &request.data) else {
            continue;
        };
        let (parser_type, bin) = match language::parser_type(&language) {
            ParserType::Scip => (ParserType::Scip, config.scip_ctags_command.clone()),
            ParserType::Universal => (ParserType::Universal, config.ctags_command.clone()),
        };

        let slot = match parser_type {
            ParserType::Scip => &mut scip,
            ParserType::Universal => &mut universal,
        };
        let slot_value = slot.take();
        let max_file_size = config.ctags_max_file_size;
        let max_symbols = config.ctags_max_symbols;
        let pattern_length_limit = config.ctags_pattern_length_limit;
        let path = request.path.clone();
        let data = request.data;

        // A missing/broken ctags binary aborts the build: without it every
        // file would silently index to nothing. Per-file parse errors are
        // logged and skipped, like the baseline.
        let (symbols, parser) = tokio::task::spawn_blocking(move || {
            parse_file(
                slot_value,
                &bin,
                pattern_length_limit,
                max_file_size,
                max_symbols,
                &path,
                &data,
            )
        })
        .await
        .map_err(|err| format!("ctags worker panicked: {err}"))??;
        *slot = parser;

        for symbol in symbols {
            if symbol_tx.send(Ok(symbol)).await.is_err() {
                return Ok(());
            }
        }
    }

    if let Some(mut parser) = universal {
        parser.close();
    }
    if let Some(mut parser) = scip {
        parser.close();
    }
    Ok(())
}

/// Apply the baseline filters and turn ctags entries into store symbols.
///
/// `Err` means the parser could not be created; a file that fails to parse is
/// logged and yields no symbols (the parser is dropped so the next request
/// starts a fresh one).
fn parse_file(
    parser: Option<CtagsProcess>,
    bin: &str,
    pattern_length_limit: u32,
    max_file_size: u64,
    max_symbols: u64,
    path: &str,
    data: &[u8],
) -> Result<(Vec<Symbol>, Option<CtagsProcess>), String> {
    // `parser.FilteringParser.Parse`.
    if data.len() as u64 > max_file_size {
        return Ok((Vec::new(), parser));
    }
    let probe = &data[..data.len().min(256)];
    if probe.contains(&0) {
        return Ok((Vec::new(), parser));
    }

    let mut parser = match parser {
        Some(parser) => parser,
        None => CtagsProcess::spawn(bin, pattern_length_limit)
            .map_err(|err| format!("start ctags {bin}: {err}"))?,
    };

    let entries = match parser.parse(path, data) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::error!(path, error = %err, "ctags parse failed");
            parser.close();
            return Ok((Vec::new(), None));
        }
    };

    if entries.len() as u64 > max_symbols {
        return Ok((Vec::new(), Some(parser)));
    }

    Ok((build_symbols(path, data, entries), Some(parser)))
}

fn build_symbols(path: &str, data: &[u8], entries: Vec<Entry>) -> Vec<Symbol> {
    let text = String::from_utf8_lossy(data);
    let lines: Vec<&str> = text.split('\n').collect();
    let mut symbols = Vec::with_capacity(entries.len());

    for entry in entries {
        if !should_persist(&entry) {
            continue;
        }

        // Careful: ctags lines are 1-indexed.
        let line = entry.line - 1;
        if line < 0 || line as usize >= lines.len() {
            tracing::warn!(
                path,
                line = entry.line,
                len_lines = lines.len(),
                symbol = entry.name,
                "ctags returned an invalid line number"
            );
            continue;
        }

        let character = lines[line as usize]
            .find(&entry.name)
            .map(|index| index as i32)
            .unwrap_or(0);

        symbols.push(Symbol {
            name: entry.name,
            path: if entry.path.is_empty() {
                path.to_owned()
            } else {
                entry.path
            },
            line,
            character,
            kind: entry.kind,
            language: entry.language,
            parent: entry.parent,
            parent_kind: entry.parent_kind,
            signature: entry.signature,
            file_limited: entry.file_limited,
        });
    }

    symbols
}

/// `parser.shouldPersistEntry`.
fn should_persist(entry: &Entry) -> bool {
    if entry.name.is_empty() {
        return false;
    }
    ["__anon", "AnonymousFunction"]
        .iter()
        .all(|prefix| !entry.name.starts_with(prefix) && !entry.parent.starts_with(prefix))
}

/// `fetcher.batchByTotalLength`.
fn batch_by_total_length(paths: &[String], max_total_length: usize) -> Vec<Vec<String>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_length = 0;
    for path in paths {
        if !current.is_empty() && current_length + path.len() > max_total_length {
            batches.push(std::mem::take(&mut current));
            current_length = 0;
        }
        current.push(path.clone());
        current_length += path.len();
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

fn create_parent(db_path: &Path) -> Result<(), String> {
    let Some(parent) = db_path.parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(parent)
        .map_err(|err| format!("create cache directory {}: {err}", parent.display()))
}

pub(crate) fn part_path(db_path: &Path) -> PathBuf {
    let mut part = db_path.as_os_str().to_owned();
    part.push(".part");
    PathBuf::from(part)
}

/// `diskcache.doFetch` equivalent: fsync the file, rename it into place, then
/// fsync the parent directory.
fn finish_atomically(part: &Path, final_path: &Path) -> Result<(), String> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(part)
        .map_err(|err| format!("open {}: {err}", part.display()))?;
    file.sync_all()
        .map_err(|err| format!("sync {}: {err}", part.display()))?;
    drop(file);

    std::fs::rename(part, final_path).map_err(|err| {
        format!(
            "rename {} to {}: {err}",
            part.display(),
            final_path.display()
        )
    })?;

    if let Some(parent) = final_path.parent() {
        if let Ok(directory) = std::fs::File::open(parent) {
            let _ = directory.sync_all();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::*;
    use crate::gitserver::ChangedFile;

    fn ctags_binary(candidates: &[&str]) -> Option<String> {
        candidates.iter().find_map(|candidate| {
            std::process::Command::new(candidate)
                .arg("--version")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .ok()
                .filter(|status| status.success())
                .map(|_| (*candidate).to_owned())
        })
    }

    fn config(cache_dir: &Path, ctags: String) -> SymbolsConfig {
        SymbolsConfig {
            cache_dir: cache_dir.to_path_buf(),
            cache_size_bytes: 1_000_000_000,
            ctags_processes: 1,
            request_buffer_size: 16,
            processing_timeout: std::time::Duration::from_secs(60),
            max_concurrently_indexing: 1,
            ctags_command: ctags.clone(),
            scip_ctags_command: ctags,
            ctags_pattern_length_limit: 250,
            ctags_log_errors: false,
            ctags_max_file_size: 524_288,
            ctags_max_symbols: 2000,
            max_total_paths_length: 100_000,
            max_file_size_bytes: 1_000_000,
            use_rockskip: false,
            gitserver_addrs: vec!["127.0.0.1:3178".to_owned()],
        }
    }

    #[derive(Default)]
    struct FakeGitserver {
        archives: Mutex<VecDeque<Vec<u8>>>,
        changed: Mutex<Vec<ChangedFile>>,
    }

    #[async_trait::async_trait]
    impl GitserverReader for FakeGitserver {
        async fn fetch_tar(
            &self,
            _repo: &str,
            _commit: &str,
            _paths: &[String],
        ) -> Result<crate::gitserver::TarByteStream, String> {
            let archive = self
                .archives
                .lock()
                .expect("archives")
                .pop_front()
                .unwrap_or_default();
            Ok(Box::pin(tokio_stream::once(Ok(Bytes::from(archive)))))
        }

        async fn changed_files(
            &self,
            _repo: &str,
            _base: &str,
            _head: &str,
        ) -> Result<Vec<ChangedFile>, String> {
            Ok(self.changed.lock().expect("changed").clone())
        }

        async fn read_file(
            &self,
            _repo: &str,
            _commit: &str,
            _path: &str,
        ) -> Result<Vec<u8>, String> {
            Ok(Vec::new())
        }
    }

    fn tar_header(name: &str, size: usize) -> [u8; 512] {
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        let size = format!("{size:o}");
        header[124..124 + size.len()].copy_from_slice(size.as_bytes());
        header[156] = b'0';
        header
    }

    fn tar(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut data = Vec::new();
        for (name, content) in entries {
            data.extend_from_slice(&tar_header(name, content.len()));
            data.extend_from_slice(content.as_bytes());
            let padding = (512 - content.len() % 512) % 512;
            data.extend(std::iter::repeat_n(0, padding));
        }
        data.extend(std::iter::repeat_n(0, 1024));
        data
    }

    #[tokio::test]
    async fn indexes_and_searches_a_repository() {
        let Some(ctags) = ctags_binary(&[
            "/usr/local/bin/universal-ctags-sg",
            "universal-ctags",
            "/usr/bin/ctags-universal",
        ]) else {
            eprintln!("skipping: no universal-ctags binary");
            return;
        };

        let dir = TempDir::new().expect("tempdir");
        let gitserver = Arc::new(FakeGitserver::default());
        gitserver
            .archives
            .lock()
            .expect("archives")
            .push_back(tar(&[(
                "script.pl",
                "package Foo;\n\nsub bar { return 1; }\n",
            )]));
        let indexer = Indexer::new(config(dir.path(), ctags), gitserver.clone());

        let db_path = dir.path().join("repo").join("commit.zip");
        let token = CancellationToken::new();
        indexer
            .write_database("repo", "commit", &db_path, &token)
            .await
            .expect("build index");

        let result = store::search(
            &db_path,
            &store::SearchParams {
                query: "^bar$".to_owned(),
                is_case_sensitive: true,
                first: 10,
                ..store::SearchParams::default()
            },
        )
        .expect("search");
        assert_eq!(result.symbols.len(), 1);
        let symbol = &result.symbols[0];
        assert_eq!(symbol.name, "bar");
        assert_eq!(symbol.path, "script.pl");
        assert_eq!(symbol.language, "Perl");
        assert_eq!(symbol.line, 2);
        assert_eq!(symbol.character, 4);

        // A newer commit re-parses only the changed paths.
        gitserver
            .changed
            .lock()
            .expect("changed")
            .push(ChangedFile {
                path: "script.pl".to_owned(),
                status: ChangeStatus::Modified,
            });
        gitserver
            .archives
            .lock()
            .expect("archives")
            .push_back(tar(&[(
                "script.pl",
                "package Foo;\n\nsub baz { return 2; }\n",
            )]));
        let next = dir.path().join("repo").join("next.zip");
        indexer
            .write_database("repo", "next", &next, &token)
            .await
            .expect("incremental index");

        let result = store::search(
            &next,
            &store::SearchParams {
                query: "^baz$".to_owned(),
                is_case_sensitive: true,
                first: 10,
                ..store::SearchParams::default()
            },
        )
        .expect("search");
        assert_eq!(result.symbols.len(), 1);
        assert_eq!(store::read_commit(&next).as_deref(), Some("next"));
    }

    #[test]
    fn batches_paths_by_total_length() {
        let paths = vec!["aaaa".to_owned(), "bbbb".to_owned(), "cc".to_owned()];
        let batches = batch_by_total_length(&paths, 9);
        assert_eq!(
            batches,
            vec![
                vec!["aaaa".to_owned(), "bbbb".to_owned()],
                vec!["cc".to_owned()],
            ]
        );
        assert_eq!(batch_by_total_length(&[], 10), Vec::<Vec<String>>::new());
    }

    #[test]
    fn skips_anonymous_entries() {
        let entry = |name: &str, parent: &str| Entry {
            name: name.to_owned(),
            path: String::new(),
            line: 1,
            kind: String::new(),
            language: String::new(),
            parent: parent.to_owned(),
            parent_kind: String::new(),
            pattern: String::new(),
            signature: String::new(),
            file_limited: false,
        };
        assert!(should_persist(&entry("foo", "")));
        assert!(!should_persist(&entry("", "")));
        assert!(!should_persist(&entry("__anon1", "")));
        assert!(!should_persist(&entry("foo", "AnonymousFunction0")));
    }
}
