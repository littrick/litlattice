//! `internal/search/zoekt/indexed_search.go:443-578`: conversion of zoekt
//! `FileMatch` values into the frontend result model
//! ([`crate::search_stream::convert`]).
//!
//! Only the pure conversions are ported: `zoektFileMatchToMultilineMatches`,
//! `zoektFileMatchToPathMatchRanges` and `zoektFileMatchToSymbolResults`.
//! Sending streaming events (`sendMatches`) and driving the zoekt search stay
//! out of scope.

use sg_zoekt::types::FileMatch;

use super::convert;
use super::events::{Location, Range};

/// `zoektFileMatchToMultilineMatches`: convert content chunks (excluding the
/// file-name chunk) to frontend chunk matches.
///
/// Offsets stay byte-based; lines and columns are decremented because zoekt
/// reports them 1-based while the frontend model is 0-based.
pub fn zoekt_file_match_to_multiline_matches(file: &FileMatch) -> Vec<convert::ChunkMatch> {
    let mut chunks = Vec::with_capacity(file.chunk_matches.len());
    for chunk in &file.chunk_matches {
        if chunk.file_name {
            continue;
        }

        let ranges = chunk
            .ranges
            .iter()
            .map(|range| Range {
                start: to_location(range.start),
                end: to_location(range.end),
            })
            .collect();

        chunks.push(convert::ChunkMatch {
            content: String::from_utf8_lossy(&chunk.content).into_owned(),
            content_start: to_location(chunk.content_start),
            ranges,
        });
    }
    chunks
}

fn to_location(location: sg_zoekt::types::Location) -> Location {
    Location {
        offset: i64::from(location.byte_offset),
        line: i64::from(location.line_number) - 1,
        column: i64::from(location.column) - 1,
    }
}

/// `zoektFileMatchToPathMatchRanges`: find every match of each path regexp in
/// the file name, appending the regexps' results in order.
///
/// `offset` is a byte offset; `column` is a rune count (`utf8.RuneCountInString`
/// in the baseline), and path matches are treated as a single line (`line = 0`).
pub fn zoekt_file_match_to_path_match_ranges(
    file: &FileMatch,
    path_regexps: &[regex::Regex],
) -> Vec<Range> {
    let file_name = String::from_utf8_lossy(&file.file_name);
    let mut ranges = Vec::new();
    for regexp in path_regexps {
        for found in regexp.find_iter(&file_name) {
            ranges.push(Range {
                start: Location {
                    offset: found.start() as i64,
                    line: 0,
                    column: file_name[..found.start()].chars().count() as i64,
                },
                end: Location {
                    offset: found.end() as i64,
                    line: 0,
                    column: file_name[..found.end()].chars().count() as i64,
                },
            });
        }
    }
    ranges
}

/// `zoektFileMatchToSymbolResults`: convert chunk symbol info to symbol
/// matches, then deduplicate adjacent duplicates.
///
/// A chunk is skipped when it is a file-name chunk or carries no symbol info.
/// `symbol_info[i]` corresponds to `ranges[i]`; a missing or `None` entry is
/// skipped. The line is **not** decremented (zoekt and `result.Symbol` are both
/// 1-based there); the character is `column - 1`.
pub fn zoekt_file_match_to_symbol_results(
    repo: &convert::MinimalRepo,
    input_rev: &str,
    file: &FileMatch,
) -> Vec<convert::SymbolMatch> {
    let file_ref = convert::FileRef {
        repo: repo.clone(),
        commit_id: file.version.clone(),
        path: String::from_utf8_lossy(&file.file_name).into_owned(),
        input_rev: Some(input_rev.to_owned()),
        precise_language: file.language.clone(),
    };

    let mut symbols = Vec::with_capacity(file.chunk_matches.len());
    for chunk in &file.chunk_matches {
        if chunk.file_name || chunk.symbol_info.is_empty() {
            continue;
        }

        for (i, range) in chunk.ranges.iter().enumerate() {
            let Some(Some(symbol_info)) = chunk.symbol_info.get(i) else {
                continue;
            };

            symbols.push(convert::SymbolMatch {
                symbol: convert::Symbol {
                    name: symbol_info.sym.clone(),
                    path: file_ref.path.clone(),
                    line: range.start.line_number as i32,
                    character: range.start.column as i32 - 1,
                    kind: symbol_info.kind.clone(),
                    language: file.language.clone(),
                    parent: symbol_info.parent.clone(),
                    parent_kind: symbol_info.parent_kind.clone(),
                },
                file: file_ref.clone(),
            });
        }
    }

    convert::dedup_symbols(symbols)
}

#[cfg(test)]
mod tests {
    use sg_zoekt::types::{ChunkMatch, Location as ZoektLocation, Range as ZoektRange, SymbolInfo};

    use super::*;

    fn location(byte_offset: u32, line_number: u32, column: u32) -> ZoektLocation {
        ZoektLocation {
            byte_offset,
            line_number,
            column,
        }
    }

    fn range(start: (u32, u32, u32), end: (u32, u32, u32)) -> ZoektRange {
        ZoektRange {
            start: location(start.0, start.1, start.2),
            end: location(end.0, end.1, end.2),
        }
    }

    fn symbol(sym: &str, kind: &str) -> Option<SymbolInfo> {
        Some(SymbolInfo {
            sym: sym.to_owned(),
            kind: kind.to_owned(),
            parent: String::new(),
            parent_kind: String::new(),
        })
    }

    #[test]
    fn multiline_matches_skip_file_name_chunks_and_decrement_lines() {
        let file = FileMatch {
            chunk_matches: vec![
                ChunkMatch {
                    content: b"needle\n".to_vec(),
                    content_start: location(10, 2, 1),
                    ranges: vec![range((10, 2, 1), (16, 2, 7))],
                    ..ChunkMatch::default()
                },
                ChunkMatch {
                    content: b"file.go".to_vec(),
                    file_name: true,
                    ..ChunkMatch::default()
                },
            ],
            ..FileMatch::default()
        };

        let chunks = zoekt_file_match_to_multiline_matches(&file);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, "needle\n");
        assert_eq!(chunks[0].content_start.line, 1);
        assert_eq!(chunks[0].content_start.column, 0);
        assert_eq!(chunks[0].ranges[0].start.offset, 10);
        assert_eq!(chunks[0].ranges[0].end.line, 1);
        assert_eq!(chunks[0].ranges[0].end.column, 6);
    }

    #[test]
    fn path_ranges_append_each_regexp_and_count_runes() {
        let file = FileMatch {
            file_name: "src/文件名foo.go".as_bytes().to_vec(),
            ..FileMatch::default()
        };
        let regexps = vec![
            regex::Regex::new("foo").unwrap(),
            regex::Regex::new("o").unwrap(),
            regex::Regex::new("missing").unwrap(),
        ];

        let ranges = zoekt_file_match_to_path_match_ranges(&file, &regexps);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].start.offset, "src/文件名".len() as i64);
        assert_eq!(ranges[0].start.column, "src/文件名".chars().count() as i64);
        assert_eq!(ranges[0].end.column, "src/文件名foo".chars().count() as i64);
        assert_eq!(ranges[1].start.offset, "src/文件名f".len() as i64);
        assert_eq!(ranges[1].start.line, 0);
    }

    #[test]
    fn symbols_skip_missing_info_and_dedup_adjacent_duplicates() {
        let file = FileMatch {
            file_name: b"src/file.go".to_vec(),
            language: "Go".to_owned(),
            version: "abc123".to_owned(),
            chunk_matches: vec![
                ChunkMatch {
                    content: b"Foo Bar\n".to_vec(),
                    content_start: location(0, 3, 1),
                    ranges: vec![range((0, 3, 1), (3, 3, 4)), range((4, 3, 5), (7, 3, 8))],
                    symbol_info: vec![symbol("Foo", "function"), None],
                    ..ChunkMatch::default()
                },
                ChunkMatch {
                    content: b"Foo\n".to_vec(),
                    content_start: location(8, 4, 1),
                    ranges: vec![range((8, 4, 1), (11, 4, 4))],
                    symbol_info: vec![symbol("Foo", "function")],
                    ..ChunkMatch::default()
                },
                // Adjacent duplicate of the previous symbol (same line, name,
                // kind, parent and parent_kind): `DedupSymbols` drops it.
                ChunkMatch {
                    content: b"Foo\n".to_vec(),
                    content_start: location(8, 4, 1),
                    ranges: vec![range((8, 4, 1), (11, 4, 4))],
                    symbol_info: vec![symbol("Foo", "function")],
                    ..ChunkMatch::default()
                },
                ChunkMatch {
                    content: b"file.go".to_vec(),
                    file_name: true,
                    symbol_info: vec![symbol("file", "file")],
                    ranges: vec![range((0, 1, 1), (7, 1, 8))],
                    ..ChunkMatch::default()
                },
            ],
            ..FileMatch::default()
        };
        let repo = convert::MinimalRepo {
            id: 7,
            name: "github.com/test/repo".to_owned(),
        };

        let symbols = zoekt_file_match_to_symbol_results(&repo, "HEAD", &file);
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].symbol.name, "Foo");
        assert_eq!(symbols[0].symbol.line, 3);
        assert_eq!(symbols[0].symbol.character, 0);
        assert_eq!(symbols[0].symbol.path, "src/file.go");
        assert_eq!(symbols[0].file.commit_id, "abc123");
        assert_eq!(symbols[0].file.input_rev.as_deref(), Some("HEAD"));
        // The adjacent duplicate (third chunk) and the file-name chunk are
        // dropped.
        assert_eq!(symbols[1].symbol.line, 4);
    }
}
