#![forbid(unsafe_code)]

//! Unindexed regexp search over a prepared archive (`search_regex.go`).

use std::io;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

use crate::chunk::locs_to_file_match;
use crate::langmatch::LangMatcher;
use crate::matchtree::MatchTree;
use crate::pathmatch::PathMatcher;
use crate::protocol::FileMatch;
use crate::sender::LimitedStream;
use crate::zipcache::{SrcFile, ZipFile};

/// Baseline `numWorkers`.
pub const NUM_WORKERS: usize = 8;

/// Failure while searching an archive.
#[derive(Debug)]
pub enum SearchError {
    /// Reading a file from the prepared zip failed.
    Io(io::Error),
}

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "read archive file: {err}"),
        }
    }
}

impl std::error::Error for SearchError {}

impl From<io::Error> for SearchError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Everything `regexSearch` needs; the callers compile the trees once.
pub struct RegexSearch<'a> {
    pub match_tree: &'a MatchTree,
    pub path_matcher: &'a PathMatcher,
    pub lang_matcher: &'a LangMatcher,
    pub zip: &'a ZipFile,
    pub pattern_matches_content: bool,
    pub pattern_matches_path: bool,
    pub context_lines: i32,
}

impl RegexSearch<'_> {
    /// Baseline `regexSearch`: search every file in `zip` with
    /// [`NUM_WORKERS`] threads. Returns once all workers have stopped, either
    /// because the archive was exhausted or `cancel` fired.
    pub fn run(
        &self,
        cancel: &CancellationToken,
        sender: &LimitedStream,
    ) -> Result<(), SearchError> {
        let mut pattern_matches_content = self.pattern_matches_content;
        let pattern_matches_path = self.pattern_matches_path;
        if !pattern_matches_content && !pattern_matches_path {
            pattern_matches_content = true;
        }

        let files = self.zip.files();
        let last_file_index = AtomicI32::new(-1);
        let error: Mutex<Option<io::Error>> = Mutex::new(None);

        std::thread::scope(|scope| {
            for _ in 0..NUM_WORKERS {
                scope.spawn(|| {
                    let mut loader = FileLoader::new(self.zip);
                    loop {
                        if cancel.is_cancelled() {
                            break;
                        }
                        let index = last_file_index.fetch_add(1, Ordering::Relaxed);
                        if index < 0 || index as usize >= files.len() {
                            break;
                        }
                        let file = &files[index as usize];

                        match self.search_file(
                            index as usize,
                            file,
                            &mut loader,
                            pattern_matches_content,
                            pattern_matches_path,
                            sender,
                        ) {
                            Ok(Some(file_match)) => sender.send(file_match),
                            Ok(None) => {}
                            Err(err) => {
                                let mut slot = error.lock().expect("error slot");
                                if slot.is_none() {
                                    *slot = Some(err);
                                }
                                cancel.cancel();
                                break;
                            }
                        }
                    }
                });
            }
        });

        match error.into_inner().expect("error slot") {
            Some(err) => Err(SearchError::Io(err)),
            None => Ok(()),
        }
    }

    fn search_file(
        &self,
        index: usize,
        file: &SrcFile,
        loader: &mut FileLoader<'_>,
        pattern_matches_content: bool,
        pattern_matches_path: bool,
        sender: &LimitedStream,
    ) -> io::Result<Option<FileMatch>> {
        if !self.path_matcher.matches(&file.name) {
            return Ok(None);
        }

        let mut matched = false;
        let mut file_match = FileMatch {
            path: file.name.clone(),
            ..FileMatch::default()
        };

        if pattern_matches_path {
            matched = self.match_tree.matches_string(&file.name);
        }

        if !matched && pattern_matches_content {
            if self.match_tree.is_all() {
                // Avoid loading the file if this pattern always matches.
                matched = true;
            } else {
                loader.load(index, file)?;
                // Find `limit + 1` matches so we know whether we hit the
                // limit.
                let limit = usize::try_from(sender.remaining())
                    .unwrap_or(0)
                    .saturating_add(1);
                let (file_matched, locs) =
                    self.match_tree.matches_file(loader.file_match_buf(), limit);
                matched = file_matched;
                file_match =
                    locs_to_file_match(loader.file_buf(), &file.name, &locs, self.context_lines);
            }
        }

        if !matched {
            return Ok(None);
        }

        // Apply language filters, loading the content lazily when detection
        // needs it.
        let (lang_match, language) = self
            .lang_matcher
            .matches(&file.name, || Ok(loader.load(index, file)?.to_vec()));
        if !lang_match {
            return Ok(None);
        }
        file_match.language = language;
        Ok(Some(file_match))
    }
}

/// Baseline `fileLoader`: keeps the last loaded file for the language check.
struct FileLoader<'a> {
    zip: &'a ZipFile,
    current: Option<usize>,
    buf: Vec<u8>,
}

impl<'a> FileLoader<'a> {
    fn new(zip: &'a ZipFile) -> Self {
        Self {
            zip,
            current: None,
            buf: Vec::new(),
        }
    }

    fn load(&mut self, index: usize, file: &SrcFile) -> io::Result<&[u8]> {
        if self.current != Some(index) {
            self.buf = self.zip.data_for(file)?;
            self.current = Some(index);
        }
        Ok(&self.buf)
    }

    fn file_buf(&self) -> &[u8] {
        &self.buf
    }

    /// With `(?i)` matching, the match buffer is the file content itself, so
    /// offsets in `file_buf` and `file_match_buf` agree.
    fn file_match_buf(&self) -> &[u8] {
        &self.buf
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use zip::CompressionMethod;

    use super::*;
    use crate::langmatch::to_lang_matcher;
    use crate::matchtree::to_match_tree;
    use crate::pathmatch::to_path_matcher;
    use crate::protocol::{PatternInfo, PatternNode, QueryNode};
    use crate::zipcache::ZipCache;

    fn write_zip(path: &std::path::Path, files: &[(&str, &[u8])]) {
        let file = std::fs::File::create(path).expect("create");
        let mut writer = zip::ZipWriter::new(file);
        for (name, data) in files {
            writer
                .start_file(
                    *name,
                    zip::write::SimpleFileOptions::default()
                        .compression_method(CompressionMethod::Stored),
                )
                .expect("start file");
            writer.write_all(data).expect("write");
        }
        writer.finish().expect("finish");
    }

    fn archive() -> (tempfile::TempDir, Arc<ZipFile>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("archive.zip");
        write_zip(
            &path,
            &[
                ("a.txt", b"needle one\nneedle two\n"),
                ("b.txt", b"nothing here\n"),
                ("sub/c.rs", b"fn main() { println!(\"needle\"); }\n"),
            ],
        );
        let zip = ZipCache::new().get(&path).expect("zip");
        (dir, zip)
    }

    fn pattern(value: &str) -> MatchTree {
        let node = QueryNode::Pattern(PatternNode {
            value: value.to_owned(),
            is_negated: false,
            is_regexp: false,
            boost: false,
        });
        to_match_tree(&node, true).expect("tree")
    }

    fn search(
        match_tree: &MatchTree,
        info: &PatternInfo,
        zip: &ZipFile,
        limit: i32,
        cancel: &CancellationToken,
    ) -> Vec<FileMatch> {
        let path_matcher = to_path_matcher(info).expect("path matcher");
        let lang_matcher = to_lang_matcher(info);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        let sender = LimitedStream::new(
            limit,
            Box::new(move |file_match| {
                sink.lock().expect("collector").push(file_match);
            }),
            cancel.clone(),
        );
        let search = RegexSearch {
            match_tree,
            path_matcher: &path_matcher,
            lang_matcher: &lang_matcher,
            zip,
            pattern_matches_content: true,
            pattern_matches_path: false,
            context_lines: 0,
        };
        search.run(cancel, &sender).expect("search");
        let matches = collected.lock().expect("collector").clone();
        matches
    }

    #[test]
    fn finds_content_matches() {
        let (_dir, zip) = archive();
        let cancel = CancellationToken::new();
        let mut matches = search(
            &pattern("needle"),
            &PatternInfo::default(),
            &zip,
            100,
            &cancel,
        );
        matches.sort_by(|left, right| left.path.cmp(&right.path));

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].path, "a.txt");
        // The two matches are on adjacent lines; with no context they are
        // separate chunks.
        assert_eq!(matches[0].chunk_matches.len(), 2);
        assert_eq!(matches[0].chunk_matches[0].content, "needle one\n");
        assert_eq!(matches[0].chunk_matches[1].content, "needle two\n");
        assert_eq!(matches[0].match_count(), 2);
        assert_eq!(matches[1].path, "sub/c.rs");
        assert_eq!(matches[1].match_count(), 1);
        assert_eq!(
            matches[1].chunk_matches[0].content,
            "fn main() { println!(\"needle\"); }\n"
        );
    }

    #[test]
    fn applies_path_and_language_filters() {
        let (_dir, zip) = archive();
        let cancel = CancellationToken::new();

        let info = PatternInfo {
            include_paths: vec![r"\.rs$".to_owned()],
            ..PatternInfo::default()
        };
        let matches = search(&pattern("needle"), &info, &zip, 100, &cancel);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].path, "sub/c.rs");

        let info = PatternInfo {
            include_langs: vec!["Rust".to_owned()],
            ..PatternInfo::default()
        };
        let matches = search(&pattern("needle"), &info, &zip, 100, &cancel);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].path, "sub/c.rs");
        assert_eq!(matches[0].language, "Rust");
    }

    #[test]
    fn matches_paths_without_content() {
        let (_dir, zip) = archive();
        let cancel = CancellationToken::new();
        let path_matcher = to_path_matcher(&PatternInfo::default()).expect("path");
        let lang_matcher = to_lang_matcher(&PatternInfo::default());
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        let sender = LimitedStream::new(
            100,
            Box::new(move |file_match| {
                sink.lock().expect("collector").push(file_match);
            }),
            cancel.clone(),
        );
        let tree = pattern("c.rs");
        let search = RegexSearch {
            match_tree: &tree,
            path_matcher: &path_matcher,
            lang_matcher: &lang_matcher,
            zip: &zip,
            pattern_matches_content: false,
            pattern_matches_path: true,
            context_lines: 0,
        };
        search.run(&cancel, &sender).expect("search");
        let matches = collected.lock().expect("collector");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].path, "sub/c.rs");
        assert!(matches[0].chunk_matches.is_empty());
        assert_eq!(matches[0].match_count(), 1);
    }

    #[test]
    fn truncates_at_the_limit_and_cancels() {
        let (_dir, zip) = archive();
        let cancel = CancellationToken::new();
        let matches = search(
            &pattern("needle"),
            &PatternInfo::default(),
            &zip,
            1,
            &cancel,
        );
        assert!(cancel.is_cancelled());
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].match_count(), 1);
    }

    #[test]
    fn honours_context_lines() {
        let (_dir, zip) = archive();
        let cancel = CancellationToken::new();
        let path_matcher = to_path_matcher(&PatternInfo::default()).expect("path");
        let lang_matcher = to_lang_matcher(&PatternInfo::default());
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        let sender = LimitedStream::new(
            100,
            Box::new(move |file_match| {
                sink.lock().expect("collector").push(file_match);
            }),
            cancel.clone(),
        );
        let tree = pattern("nothing");
        let search = RegexSearch {
            match_tree: &tree,
            path_matcher: &path_matcher,
            lang_matcher: &lang_matcher,
            zip: &zip,
            pattern_matches_content: true,
            pattern_matches_path: false,
            context_lines: 1,
        };
        search.run(&cancel, &sender).expect("search");
        let matches = collected.lock().expect("collector");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].chunk_matches[0].content, "nothing here\n");
    }
}
