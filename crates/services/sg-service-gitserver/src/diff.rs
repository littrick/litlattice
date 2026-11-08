//! A port of the parts of `github.com/sourcegraph/go-diff/diff` the commit
//! search needs.
//!
//! Baseline: `cmd/gitserver/internal/search/lazy_commit.go` reads the output
//! of the persistent `git diff-tree` process with
//! `godiff.NewMultiFileDiffReader(...).ReadAllFiles()`. This module reproduces
//! that reader closely enough that the same patches yield the same
//! `FileDiff`/`Hunk` values (names, section headers, hunk bodies) — including
//! its quirks: extension headers overflow detection, `handleEmpty` for diffs
//! without hunks, `\ No newline at end of file`, and Go `strconv.Unquote` for
//! quoted filenames.
//!
//! Only the values used by the search match tree and `FormatDiff` are kept;
//! timestamps and `OrigNoNewlineAt` are parsed but not stored.
//!
//! Known divergence: `strings.TrimSpace` is approximated with ASCII
//! whitespace (filenames/branches in fixtures are ASCII). Parse error text
//! uses the same `line %d, char %d: %s` shape but is not byte-identical in
//! line/offset accounting.

/// One file in a unified diff (`godiff.FileDiff`), reduced to the fields the
/// search uses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileDiff {
    pub orig_name: Vec<u8>,
    pub new_name: Vec<u8>,
    /// Extended header lines; only needed while parsing.
    pub extended: Vec<Vec<u8>>,
    pub hunks: Vec<Hunk>,
}

/// One hunk (`godiff.Hunk`); `body` holds the prefixed lines, each ending in
/// `\n` unless the file had no trailing newline.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Hunk {
    pub orig_start_line: i32,
    pub orig_lines: i32,
    pub new_start_line: i32,
    pub new_lines: i32,
    pub section: Vec<u8>,
    pub body: Vec<u8>,
}

/// A parse failure; the search RPC surfaces it as an `UNKNOWN` status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiffError {
    message: String,
}

impl DiffError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn parse(line: usize, offset: usize, message: impl std::fmt::Display) -> Self {
        Self::new(format!("line {line}, char {offset}: {message}"))
    }
}

impl std::fmt::Display for DiffError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for DiffError {}

const NO_FILE_HEADER: &str = "expected file header, got EOF";
const BAD_FILE_HEADER: &str = "bad file header";
const NO_HUNK_HEADER: &str = "no hunk header";
const NO_NEWLINE_MESSAGE: &[u8] = b"\\ No newline at end of file";
const HUNK_PREFIX: &[u8] = b"@@ ";
const ONLY_IN_PREFIX: &[u8] = b"Only in ";

/// Read every file diff in `raw`. Trailing non-diff content ends the list, as
/// in `ReadAllFiles`.
pub fn parse_multi_file_diff(raw: &[u8]) -> Result<Vec<FileDiff>, DiffError> {
    let mut parser = Parser::new(raw);
    let mut files = Vec::new();
    while let Some(file) = parser.read_file()? {
        files.push(file);
    }
    Ok(files)
}

/// A port of go-diff's `lineReader`: it caches one line ahead so hunk parsing
/// can peek at the following line, and drops a trailing `\r` on terminated
/// lines (but not on the final unterminated one).
struct LineReader<'a> {
    data: &'a [u8],
    position: usize,
    line: usize,
    offset: usize,
    cached: Option<Result<&'a [u8], ()>>,
}

impl<'a> LineReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            position: 0,
            line: 0,
            offset: 0,
            cached: None,
        }
    }

    fn read_raw(&mut self) -> Result<&'a [u8], ()> {
        if self.position >= self.data.len() {
            return Err(());
        }
        let start = self.position;
        match self.data[start..].iter().position(|&byte| byte == b'\n') {
            Some(index) => {
                self.position = start + index + 1;
                self.line += 1;
                self.offset = self.position;
                let mut line = &self.data[start..start + index];
                if line.last() == Some(&b'\r') {
                    line = &line[..line.len() - 1];
                }
                Ok(line)
            }
            None => {
                self.position = self.data.len();
                self.line += 1;
                self.offset = self.position;
                Ok(&self.data[start..])
            }
        }
    }

    fn read_line(&mut self) -> Result<&'a [u8], ()> {
        let next = match self.cached.take() {
            Some(result) => result?,
            None => self.read_raw()?,
        };
        self.cached = Some(self.read_raw());
        Ok(next)
    }

    fn ensure_cached(&mut self) {
        if self.cached.is_none() {
            self.cached = Some(self.read_raw());
        }
    }

    fn next_line_starts_with(&mut self, prefix: &[u8]) -> bool {
        self.ensure_cached();
        matches!(self.cached.as_ref(), Some(Ok(line)) if line.starts_with(prefix))
    }

    fn next_next_line_starts_with(&mut self, prefix: &[u8]) -> bool {
        self.ensure_cached();
        self.data[self.position..].starts_with(prefix)
    }
}

/// What `read_extended_headers` found.
enum ExtendedHeaders<'a> {
    /// The next line is a `--- ` (or `Only in `) header.
    Done(Vec<Vec<u8>>),
    /// A second `diff --git` line started the next file.
    Overflow(Vec<Vec<u8>>, &'a [u8]),
    /// EOF before any file header.
    Eof(Vec<Vec<u8>>),
}

/// The outcome of reading a file's hunks.
enum HunksOutcome<'a> {
    Complete(Vec<Hunk>),
    /// A line that does not belong to a hunk started the next file.
    NextFile(Vec<Hunk>, &'a [u8]),
}

struct Parser<'a> {
    reader: LineReader<'a>,
    /// `FileDiffReader.fileHeaderLine`: the first header line, already read.
    file_header_line: Option<&'a [u8]>,
    /// `MultiFileDiffReader.nextFileFirstLine`: the first line of the next
    /// file, consumed by a hunk or by extended-header parsing.
    next_file_first_line: Option<&'a [u8]>,
    next_hunk_header_line: Option<&'a [u8]>,
}

impl<'a> Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            reader: LineReader::new(data),
            file_header_line: None,
            next_file_first_line: None,
            next_hunk_header_line: None,
        }
    }

    /// `MultiFileDiffReader.ReadFileWithTrailingContent`, returning `None` at
    /// the end of the input.
    fn read_file(&mut self) -> Result<Option<FileDiff>, DiffError> {
        self.file_header_line = self.next_file_first_line.take();
        let mut file = FileDiff::default();

        match self.read_extended_headers() {
            ExtendedHeaders::Done(extended) => file.extended = extended,
            ExtendedHeaders::Overflow(extended, line) => {
                file.extended = extended;
                self.next_file_first_line = Some(line);
                // `handleEmpty` fills in the names when the extended headers
                // described a hunk-less file; the file is returned either way.
                handle_empty(&mut file);
                return Ok(Some(file));
            }
            ExtendedHeaders::Eof(extended) => {
                file.extended = extended;
                if handle_empty(&mut file) {
                    return Ok(Some(file));
                }
                return Ok(None);
            }
        }

        let (orig_name, new_name) = self.read_file_headers()?;
        file.orig_name = orig_name;
        file.new_name = new_name;
        if file.new_name.is_empty() {
            return Ok(Some(file));
        }

        let next_line = self.reader.read_line().ok();
        let Some(next_line) = next_line else {
            return Ok(Some(file));
        };
        if next_line.starts_with(HUNK_PREFIX) {
            self.next_hunk_header_line = Some(next_line);
            match self.read_hunks()? {
                HunksOutcome::Complete(hunks) => file.hunks = hunks,
                HunksOutcome::NextFile(hunks, line) => {
                    file.hunks = hunks;
                    self.next_file_first_line = Some(line);
                }
            }
        } else {
            self.next_file_first_line = Some(next_line);
        }
        Ok(Some(file))
    }

    /// `FileDiffReader.ReadExtendedHeaders`.
    fn read_extended_headers(&mut self) -> ExtendedHeaders<'a> {
        let mut headers = Vec::new();
        let mut first_line = true;
        loop {
            let line = match self.file_header_line.take() {
                Some(line) => line,
                None => match self.reader.read_line() {
                    Ok(line) => line,
                    Err(()) => return ExtendedHeaders::Eof(headers),
                },
            };
            if line.starts_with(b"diff --git ") {
                if first_line {
                    first_line = false;
                } else {
                    return ExtendedHeaders::Overflow(headers, line);
                }
            }
            if line.starts_with(b"--- ") {
                self.file_header_line = Some(line);
                return ExtendedHeaders::Done(headers);
            }
            if parse_only_in_message(line).is_some() {
                self.file_header_line = Some(line);
                return ExtendedHeaders::Done(headers);
            }
            headers.push(line.to_vec());
        }
    }

    /// `FileDiffReader.ReadFileHeaders`.
    fn read_file_headers(&mut self) -> Result<(Vec<u8>, Vec<u8>), DiffError> {
        if let Some(line) = self.file_header_line {
            if let Some((source, filename)) = parse_only_in_message(line) {
                self.file_header_line = None;
                return Ok((join_path(&source, &filename), Vec::new()));
            }
        }
        let orig = self.read_one_file_header(b"--- ")?;
        let new = self.read_one_file_header(b"+++ ")?;
        Ok((
            go_unquote(&orig).unwrap_or(orig),
            go_unquote(&new).unwrap_or(new),
        ))
    }

    /// `FileDiffReader.readOneFileHeader`.
    fn read_one_file_header(&mut self, prefix: &[u8]) -> Result<Vec<u8>, DiffError> {
        let line = match self.file_header_line.take() {
            Some(line) => line,
            None => match self.reader.read_line() {
                Ok(line) => line,
                Err(()) => {
                    return Err(DiffError::parse(
                        self.reader.line,
                        self.reader.offset,
                        NO_FILE_HEADER,
                    ))
                }
            },
        };
        if !line.starts_with(prefix) {
            return Err(DiffError::parse(
                self.reader.line,
                self.reader.offset,
                BAD_FILE_HEADER,
            ));
        }
        let rest = &line[prefix.len()..];
        // `strings.TrimSpace` then `SplitN("\t", 2)`; timestamps are ignored.
        let trimmed = trim_space(rest);
        let filename = match trimmed.iter().position(|&byte| byte == b'\t') {
            Some(index) => &trimmed[..index],
            None => trimmed,
        };
        Ok(filename.to_vec())
    }

    /// `HunksReader.ReadAllHunks`, driven by `next_hunk_header_line`.
    fn read_hunks(&mut self) -> Result<HunksOutcome<'a>, DiffError> {
        let mut hunks = Vec::new();
        loop {
            match self.read_hunk() {
                Ok(Some(hunk)) => hunks.push(hunk),
                Ok(None) => return Ok(HunksOutcome::Complete(hunks)),
                Err(HunkError::BadLine(line, hunk)) => {
                    self.next_hunk_header_line = None;
                    // Baseline `ReadAllHunks` appends the in-progress hunk
                    // before returning `ErrBadHunkLine`; the caller still
                    // keeps it for the current file.
                    if let Some(hunk) = hunk {
                        hunks.push(hunk);
                    }
                    return Ok(HunksOutcome::NextFile(hunks, line));
                }
                Err(HunkError::Error(error)) => return Err(error),
            }
        }
    }

    /// `HunksReader.ReadHunk`. `Ok(None)` is `io.EOF`.
    fn read_hunk(&mut self) -> Result<Option<Hunk>, HunkError<'a>> {
        let mut hunk: Option<Hunk> = None;
        let mut last_line_from_orig = true;
        loop {
            let line = match self.next_hunk_header_line.take() {
                Some(line) => line,
                None => match self.reader.read_line() {
                    Ok(line) => line,
                    Err(()) => {
                        return match hunk {
                            Some(hunk) => Ok(Some(hunk)),
                            None => Ok(None),
                        }
                    }
                },
            };

            let Some(current) = hunk.as_mut() else {
                if !line.starts_with(HUNK_PREFIX) {
                    return Err(HunkError::Error(DiffError::parse(
                        self.reader.line,
                        self.reader.offset,
                        NO_HUNK_HEADER,
                    )));
                }
                hunk = Some(parse_hunk_header(
                    line,
                    self.reader.line,
                    self.reader.offset,
                )?);
                continue;
            };

            if line.starts_with(b"---")
                && self.reader.next_line_starts_with(b"+++")
                && self.reader.next_next_line_starts_with(HUNK_PREFIX)
            {
                self.next_hunk_header_line = None;
                return Err(HunkError::BadLine(line, hunk.take()));
            }

            if line.starts_with(HUNK_PREFIX) {
                self.next_hunk_header_line = Some(line);
                return Ok(Some(hunk.take().expect("checked above")));
            }

            if !line.is_empty() && !is_line_prefix(line[0]) {
                return Err(HunkError::BadLine(line, hunk.take()));
            }
            if line == NO_NEWLINE_MESSAGE {
                if last_line_from_orig {
                    // Keep the newline: the next line is from the new file.
                } else if !current.body.is_empty() {
                    current.body.pop();
                }
                continue;
            }

            if !line.is_empty() {
                last_line_from_orig = line[0] == b'-';
            }
            current.body.extend_from_slice(line);
            current.body.push(b'\n');
        }
    }
}

enum HunkError<'a> {
    /// `ErrBadHunkLine`: the line belongs to the next file. The in-progress
    /// hunk is carried along because the baseline keeps it (see
    /// `HunksReader.ReadAllHunks`).
    BadLine(&'a [u8], Option<Hunk>),
    Error(DiffError),
}

impl<'a> From<DiffError> for HunkError<'a> {
    fn from(error: DiffError) -> Self {
        Self::Error(error)
    }
}

/// `HunksReader.ReadHunk`'s header parsing (`normalizeHeader` + `Sscanf`).
fn parse_hunk_header(line: &[u8], line_number: usize, offset: usize) -> Result<Hunk, DiffError> {
    let bad = || {
        DiffError::parse(
            line_number,
            offset,
            format!("bad hunk header: {}", String::from_utf8_lossy(line)),
        )
    };
    let pieces = splitn(line, b' ', 5);
    if pieces.len() < 4 || pieces[0] != b"@@" {
        return Err(bad());
    }
    let mut first = pieces[1].to_vec();
    if !first.contains(&b',') {
        first.extend_from_slice(b",1");
    }
    let mut second = pieces[2].to_vec();
    if !second.contains(&b',') {
        second.extend_from_slice(b",1");
    }
    if pieces[3] != b"@@" {
        return Err(bad());
    }
    let section = if pieces.len() == 5 {
        trim_space(pieces[4]).to_vec()
    } else {
        Vec::new()
    };

    let parse_range = |range: &[u8], expect_plus: bool| -> Option<(i32, i32)> {
        let (sign, rest) = match range.first() {
            Some(b'-') => (true, &range[1..]),
            Some(b'+') => (false, &range[1..]),
            _ => (false, range),
        };
        if sign == expect_plus {
            return None;
        }
        let comma = rest.iter().position(|&byte| byte == b',')?;
        let start = parse_i32(&rest[..comma])?;
        let count = parse_i32(&rest[comma + 1..])?;
        Some((start, count))
    };

    let Some((orig_start, orig_lines)) = parse_range(&first, false) else {
        return Err(bad());
    };
    let Some((new_start, new_lines)) = parse_range(&second, true) else {
        return Err(bad());
    };

    Ok(Hunk {
        orig_start_line: orig_start,
        orig_lines,
        new_start_line: new_start,
        new_lines,
        section,
        body: Vec::new(),
    })
}

fn parse_i32(bytes: &[u8]) -> Option<i32> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn is_line_prefix(byte: u8) -> bool {
    matches!(byte, b' ' | b'-' | b'+' | b'\\')
}

fn splitn(bytes: &[u8], separator: u8, limit: usize) -> Vec<&[u8]> {
    let mut parts = Vec::with_capacity(limit);
    let mut start = 0;
    while parts.len() + 1 < limit {
        match bytes[start..].iter().position(|&byte| byte == separator) {
            Some(index) => {
                parts.push(&bytes[start..start + index]);
                start += index + 1;
            }
            None => break,
        }
    }
    parts.push(&bytes[start..]);
    parts
}

fn trim_space(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

/// `parseOnlyInMessage`; `(source, filename)`.
fn parse_only_in_message(line: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let rest = line.strip_prefix(ONLY_IN_PREFIX)?;
    let index = rest.windows(2).position(|window| window == b": ")?;
    Some((rest[..index].to_vec(), rest[index + 2..].to_vec()))
}

/// `filepath.Join` for the two-segment `Only in` case.
fn join_path(source: &[u8], filename: &[u8]) -> Vec<u8> {
    if source.is_empty() {
        return filename.to_vec();
    }
    let mut joined = source.to_vec();
    if !joined.ends_with(b"/") {
        joined.push(b'/');
    }
    joined.extend_from_slice(filename);
    joined
}

/// go-diff's `handleEmpty`: fills in names for diffs with extended headers but
/// no `---`/`+++` file headers (mode changes, renames, new/deleted empty files,
/// binary diffs). Returns whether the extended headers described such a diff.
fn handle_empty(file: &mut FileDiff) -> bool {
    let line_count = file.extended.len();
    if line_count > 0 && !file.extended[0].starts_with(b"diff --git ") {
        return false;
    }
    let has = |index: usize, prefix: &[u8]| file.extended[index].starts_with(prefix);
    let both = |i1: usize, p1: &[u8], i2: usize, p2: &[u8]| has(i1, p1) && has(i2, p2);

    let is_copy = (line_count == 4 && both(2, b"copy from ", 3, b"copy to "))
        || (line_count == 6 && both(2, b"copy from ", 3, b"copy to ") && has(5, b"Binary files "))
        || (line_count == 6
            && both(1, b"old mode ", 2, b"new mode ")
            && both(4, b"copy from ", 5, b"copy to "));
    let is_rename = (line_count == 4 && both(2, b"rename from ", 3, b"rename to "))
        || (line_count == 5
            && both(2, b"rename from ", 3, b"rename to ")
            && has(4, b"Binary files "))
        || (line_count == 6
            && both(2, b"rename from ", 3, b"rename to ")
            && has(5, b"Binary files "))
        || (line_count == 6
            && both(1, b"old mode ", 2, b"new mode ")
            && both(4, b"rename from ", 5, b"rename to "));
    let is_deleted_file = (line_count == 3
        || line_count == 4 && has(3, b"Binary files ")
        || line_count > 4 && has(3, b"GIT binary patch"))
        && has(1, b"deleted file mode ");
    let is_new_file = (line_count == 3
        || line_count == 4 && has(3, b"Binary files ")
        || line_count > 4 && has(3, b"GIT binary patch"))
        && has(1, b"new file mode ");
    let is_mode_change = line_count == 3 && both(1, b"old mode ", 2, b"new mode ");
    let is_binary_patch = line_count == 3 && has(2, b"Binary files ")
        || line_count > 3 && has(2, b"GIT binary patch");

    if !is_mode_change
        && !is_copy
        && !is_rename
        && !is_binary_patch
        && !is_new_file
        && !is_deleted_file
    {
        return false;
    }

    let diff_args = &file.extended[0][b"diff --git ".len()..];
    let (mut orig, mut new, success) = parse_diff_git_args(diff_args);
    if is_new_file {
        orig = b"/dev/null".to_vec();
    }
    if is_deleted_file {
        new = b"/dev/null".to_vec();
    }

    if success && (is_copy || is_rename) && orig.is_empty() && new.is_empty() {
        let try_reconstruct = |header: &[u8], prefix: &[u8], which: usize| -> Option<Vec<u8>> {
            let raw = header.strip_prefix(prefix)?;
            let index = if which == 1 {
                0
            } else {
                diff_args.len().checked_sub(raw.len() + 2)?
            };
            if diff_args.get(index + 1) != Some(&b'/') {
                return None;
            }
            let mut result = diff_args[index..index + 2].to_vec();
            result.extend_from_slice(raw);
            Some(result)
        };
        for header in &file.extended {
            if let Some(value) = try_reconstruct(header, b"copy from ", 1) {
                orig = value;
            }
            if let Some(value) = try_reconstruct(header, b"copy to ", 2) {
                new = value;
            }
            if let Some(value) = try_reconstruct(header, b"rename from ", 1) {
                orig = value;
            }
            if let Some(value) = try_reconstruct(header, b"rename to ", 2) {
                new = value;
            }
        }
    }

    file.orig_name = orig;
    file.new_name = new;
    success
}

/// `parseDiffGitArgs`: extracts the two names from a `diff --git` line.
fn parse_diff_git_args(diff_args: &[u8]) -> (Vec<u8>, Vec<u8>, bool) {
    let length = diff_args.len();
    if length < 3 {
        return (Vec::new(), Vec::new(), false);
    }

    if diff_args[0] != b'"' && diff_args[length - 1] != b'"' {
        let first_space = diff_args.iter().position(|&byte| byte == b' ');
        let Some(first_space) = first_space else {
            return (Vec::new(), Vec::new(), false);
        };
        if first_space == 0 || first_space == length - 1 {
            return (Vec::new(), Vec::new(), false);
        }
        let rest = &diff_args[first_space + 1..];
        match rest.iter().position(|&byte| byte == b' ') {
            None => {
                if rest[0] == b'"' {
                    return (Vec::new(), Vec::new(), false);
                }
                (diff_args[..first_space].to_vec(), rest.to_vec(), true)
            }
            Some(_) => {
                let first = &diff_args[..length / 2];
                let second = &diff_args[length / 2 + 1..];
                if length % 2 == 1 {
                    if first.len() >= 3 && first[1] == b'/' && first[1..] == second[1..] {
                        return (first.to_vec(), second.to_vec(), true);
                    }
                    if !(first.starts_with(b"a/") && second.starts_with(b"b/")) && first == second {
                        return (first.to_vec(), second.to_vec(), true);
                    }
                }
                (Vec::new(), Vec::new(), true)
            }
        }
    } else if diff_args[0] == b'"' {
        let Some((first, remainder)) = read_quoted_filename(diff_args) else {
            return (Vec::new(), Vec::new(), false);
        };
        if remainder.len() < 2 || remainder[0] != b' ' {
            return (Vec::new(), Vec::new(), false);
        }
        if remainder[1] == b'"' {
            let Some((second, trailing)) = read_quoted_filename(&remainder[1..]) else {
                return (Vec::new(), Vec::new(), false);
            };
            if !trailing.is_empty() {
                return (Vec::new(), Vec::new(), false);
            }
            (first, second, true)
        } else {
            (first, remainder[1..].to_vec(), true)
        }
    } else {
        let Some(index) = diff_args.iter().position(|&byte| byte == b'"') else {
            return (Vec::new(), Vec::new(), false);
        };
        if index + 2 >= length || diff_args[index - 1] != b' ' {
            return (Vec::new(), Vec::new(), false);
        }
        let Some((second, trailing)) = read_quoted_filename(&diff_args[index..]) else {
            return (Vec::new(), Vec::new(), false);
        };
        if !trailing.is_empty() {
            return (Vec::new(), Vec::new(), false);
        }
        (diff_args[..index - 1].to_vec(), second, true)
    }
}

/// `readQuotedFilename`: returns the unquoted value and the remaining text.
fn read_quoted_filename(text: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    if text.first() != Some(&b'"') {
        return None;
    }
    let mut backslashes = 0;
    for (index, &byte) in text.iter().enumerate() {
        if byte == b'"' && index > 0 && backslashes % 2 == 0 {
            let value = go_unquote(&text[..=index])?;
            return Some((value, &text[index + 1..]));
        } else if byte == b'\\' {
            backslashes += 1;
        } else {
            backslashes = 0;
        }
    }
    None
}

/// `strconv.Unquote` for double-quoted Go string literals, returning the raw
/// bytes (which may be invalid UTF-8 after octal escapes).
fn go_unquote(text: &[u8]) -> Option<Vec<u8>> {
    if text.len() < 2 || text[0] != b'"' || text[text.len() - 1] != b'"' {
        return None;
    }
    let body = &text[1..text.len() - 1];
    let mut out = Vec::with_capacity(body.len());
    let mut index = 0;
    while index < body.len() {
        let byte = body[index];
        if byte == b'"' || byte == b'\n' {
            return None;
        }
        if byte != b'\\' {
            out.push(byte);
            index += 1;
            continue;
        }
        index += 1;
        let escape = *body.get(index)?;
        index += 1;
        match escape {
            b'a' => out.push(0x07),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'\\' => out.push(b'\\'),
            b'\'' => out.push(b'\''),
            b'"' => out.push(b'"'),
            b'x' => {
                let value = parse_hex(&body[index..], 2)?;
                out.push(value as u8);
                index += 2;
            }
            b'0'..=b'7' => {
                let mut value = (escape - b'0') as u32;
                for _ in 0..2 {
                    let digit = *body.get(index)?;
                    if !(b'0'..=b'7').contains(&digit) {
                        return None;
                    }
                    value = value * 8 + (digit - b'0') as u32;
                    index += 1;
                }
                if value > 0xff {
                    return None;
                }
                out.push(value as u8);
            }
            b'u' => {
                let value = parse_hex(&body[index..], 4)?;
                index += 4;
                push_rune(&mut out, value)?;
            }
            b'U' => {
                let value = parse_hex(&body[index..], 8)?;
                index += 8;
                push_rune(&mut out, value)?;
            }
            _ => return None,
        }
    }
    Some(out)
}

fn parse_hex(bytes: &[u8], count: usize) -> Option<u32> {
    if bytes.len() < count {
        return None;
    }
    let mut value = 0u32;
    for &byte in &bytes[..count] {
        let digit = (byte as char).to_digit(16)?;
        value = value * 16 + digit;
    }
    Some(value)
}

fn push_rune(out: &mut Vec<u8>, value: u32) -> Option<()> {
    let character = char::from_u32(value)?;
    let mut buffer = [0u8; 4];
    out.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `git diff-tree` output for the fixture repo's root commit. The first
    /// two files used to lose their hunks because `ErrBadHunkLine` dropped the
    /// in-progress hunk (the baseline returns it alongside the error).
    #[test]
    fn parses_root_commit_multi_file_diff() {
        let raw = include_bytes!("../testdata/root.diff");
        let files = parse_multi_file_diff(raw).expect("root diff parses");
        let names: Vec<(&[u8], &[u8])> = files
            .iter()
            .map(|file| (file.orig_name.as_slice(), file.new_name.as_slice()))
            .collect();
        assert_eq!(
            names,
            vec![
                (&b"/dev/null"[..], &b"file.txt"[..]),
                (&b"/dev/null"[..], "héllo.txt".as_bytes()),
                (&b"/dev/null"[..], &b"other.txt"[..]),
            ]
        );
        let bodies: Vec<Vec<&[u8]>> = files
            .iter()
            .map(|file| file.hunks.iter().map(|hunk| hunk.body.as_slice()).collect())
            .collect();
        assert_eq!(
            bodies,
            vec![
                vec![&b"+alpha\n+beta\n+gamma\n+delta\n"[..]],
                vec![&b"+unicode\n"[..]],
                vec![&b"+other\n"[..]],
            ]
        );
        assert_eq!(files[0].hunks[0].new_start_line, 1);
        assert_eq!(files[0].hunks[0].new_lines, 4);
    }
}
