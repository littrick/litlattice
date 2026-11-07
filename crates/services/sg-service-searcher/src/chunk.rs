#![forbid(unsafe_code)]

//! Group match ranges into context chunks (`chunk.go`).
//!
//! Every location reported in a chunk match is relative to the file, not to
//! the chunk content; `content_start` tells the client where the content
//! slice begins.

use crate::protocol::{ChunkMatch, FileMatch, Location, Range};

/// A chunk being assembled: the range covering all matches plus the matches
/// themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeChunk {
    pub cover: Range,
    pub ranges: Vec<Range>,
}

/// Baseline `locsToFileMatch`.
pub fn locs_to_file_match(
    file_buf: &[u8],
    name: &str,
    locs: &[(usize, usize)],
    context_lines: i32,
) -> FileMatch {
    if locs.is_empty() {
        return FileMatch {
            path: name.to_owned(),
            ..FileMatch::default()
        };
    }
    let ranges = locs_to_ranges(file_buf, locs);
    let chunks = chunk_ranges(ranges, context_lines * 2);
    FileMatch {
        path: name.to_owned(),
        chunk_matches: chunks_to_matches(file_buf, &chunks, context_lines),
        ..FileMatch::default()
    }
}

/// Baseline `locsToRanges`: convert byte ranges to line/column locations.
/// `locs` must be sorted, non-overlapping and valid slices of `buf`.
pub fn locs_to_ranges(buf: &[u8], locs: &[(usize, usize)]) -> Vec<Range> {
    let mut ranges = Vec::with_capacity(locs.len());
    let mut previous_start = 0usize;
    let mut previous_start_line = 0i32;
    let mut columns = ColumnHelper::default();

    for &(start, end) in locs {
        let start_line = previous_start_line + count_newlines(&buf[previous_start..start]) as i32;
        let end_line = start_line + count_newlines(&buf[start..end]) as i32;

        let first_line_start = buf[..start]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let last_line_start = buf[..end]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);

        ranges.push(Range {
            start: Location {
                offset: start as i32,
                line: start_line,
                column: columns.get(buf, first_line_start, start),
            },
            end: Location {
                offset: end as i32,
                line: end_line,
                column: columns.get(buf, last_line_start, end),
            },
        });

        previous_start = start;
        previous_start_line = start_line;
    }

    ranges
}

/// Baseline `chunkRanges`: group ranges that are at most `inter_chunk_lines`
/// lines apart.
pub fn chunk_ranges(mut ranges: Vec<Range>, inter_chunk_lines: i32) -> Vec<RangeChunk> {
    ranges.sort_by_key(|range| range.start.offset);

    let mut chunks: Vec<RangeChunk> = Vec::with_capacity(ranges.len() / 2);
    for (index, range) in ranges.iter().copied().enumerate() {
        if index == 0 {
            chunks.push(RangeChunk {
                cover: range,
                ranges: vec![range],
            });
            continue;
        }

        let last_chunk = chunks.last_mut().expect("first chunk exists");
        if last_chunk.cover.end.line + inter_chunk_lines >= range.start.line {
            last_chunk.ranges.push(range);
            if range.end.offset > last_chunk.cover.end.offset {
                last_chunk.cover.end = range.end;
            }
        } else {
            chunks.push(RangeChunk {
                cover: range,
                ranges: vec![range],
            });
        }
    }
    chunks
}

/// Baseline `chunksToMatches`.
pub fn chunks_to_matches(buf: &[u8], chunks: &[RangeChunk], context_lines: i32) -> Vec<ChunkMatch> {
    chunks
        .iter()
        .map(|chunk| {
            let extended = extend_range_to_lines(chunk.cover, buf);
            let with_context = add_context_lines(extended, buf, context_lines);
            let content = String::from_utf8_lossy(
                &buf[with_context.start.offset as usize..with_context.end.offset as usize],
            )
            .into_owned();
            ChunkMatch {
                content,
                content_start: with_context.start,
                ranges: chunk.ranges.clone(),
            }
        })
        .collect()
}

/// Baseline `extendRangeToLines`: widen a range to complete lines.
pub fn extend_range_to_lines(input_range: Range, buf: &[u8]) -> Range {
    let first_line_start = line_start(buf, input_range.start.offset);
    let last_line_start = line_start(buf, input_range.end.offset);
    // End of the line containing the last byte of the match. A zero-width
    // match between lines prefers the following line.
    let last_line_end = line_end(buf, input_range.end.offset.max(0));

    Range {
        start: Location {
            offset: first_line_start,
            line: input_range.start.line,
            column: 0,
        },
        end: Location {
            offset: last_line_end,
            line: input_range.end.line,
            column: go_rune_count(&buf[last_line_start as usize..last_line_end as usize]),
        },
    }
}

/// Baseline `addContextLines`.
pub fn add_context_lines(input_range: Range, buf: &[u8], context_lines: i32) -> Range {
    if context_lines == 0 {
        return input_range;
    }
    let mut first_line_start = input_range.start.offset;
    let mut last_line_end = input_range.end.offset;
    let mut preceding_lines_added = 0i32;
    let mut succeeding_lines_added = 0i32;

    for _ in 0..context_lines {
        if first_line_start > 0 {
            first_line_start = line_start(buf, first_line_start - 1);
            preceding_lines_added += 1;
        }
        if (last_line_end as usize) < buf.len() {
            last_line_end = line_end(buf, last_line_end);
            succeeding_lines_added += 1;
        }
    }

    let last_line_start = line_start(buf, last_line_end);

    Range {
        start: Location {
            offset: first_line_start,
            line: input_range.start.line - preceding_lines_added,
            column: 0,
        },
        end: Location {
            offset: last_line_end,
            line: input_range.end.line + succeeding_lines_added,
            column: go_rune_count(&buf[last_line_start as usize..last_line_end as usize]),
        },
    }
}

fn line_start(buf: &[u8], offset: i32) -> i32 {
    let offset = offset.max(0) as usize;
    let start = buf[..offset.min(buf.len())]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    start as i32
}

fn line_end(buf: &[u8], offset: i32) -> i32 {
    let offset = offset.max(0) as usize;
    let end = buf[offset.min(buf.len())..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(buf.len(), |index| index + offset + 1);
    end as i32
}

fn count_newlines(buf: &[u8]) -> usize {
    buf.iter().filter(|byte| **byte == b'\n').count()
}

/// Baseline `columnHelper`: cache the last rune count for the common case of
/// increasing offsets on the same line.
#[derive(Debug, Default)]
pub struct ColumnHelper {
    last_line_offset: usize,
    last_offset: usize,
    last_rune_count: i32,
}

impl ColumnHelper {
    pub fn get(&mut self, buf: &[u8], line_offset: usize, offset: usize) -> i32 {
        let rune_count = if line_offset == self.last_line_offset && offset >= self.last_offset {
            self.last_rune_count + go_rune_count(&buf[self.last_offset..offset])
        } else {
            go_rune_count(&buf[line_offset..offset])
        };
        self.last_line_offset = line_offset;
        self.last_offset = offset;
        self.last_rune_count = rune_count;
        rune_count
    }
}

/// Go `utf8.RuneCount`: each invalid byte counts as one rune.
fn go_rune_count(buf: &[u8]) -> i32 {
    let mut index = 0usize;
    let mut count = 0i32;
    while index < buf.len() {
        let first = buf[index];
        let width = match first {
            0x00..=0x7f => 1,
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => 0,
        };
        if width > 0
            && index + width <= buf.len()
            && std::str::from_utf8(&buf[index..index + width]).is_ok()
        {
            index += width;
        } else {
            index += 1;
        }
        count += 1;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(offset: i32, line: i32, column: i32) -> Location {
        Location {
            offset,
            line,
            column,
        }
    }

    fn range(start: (i32, i32, i32), end: (i32, i32, i32)) -> Range {
        Range {
            start: loc(start.0, start.1, start.2),
            end: loc(end.0, end.1, end.2),
        }
    }

    #[test]
    fn chunks_ranges_like_the_baseline() {
        // A single range forms one chunk.
        let single = range((0, 0, 0), (20, 1, 10));
        let chunks = chunk_ranges(vec![single], 0);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].cover, single);

        // Overlapping ranges merge and expand the cover.
        let chunks = chunk_ranges(
            vec![range((0, 0, 0), (20, 1, 10)), range((5, 0, 5), (25, 1, 15))],
            0,
        );
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].cover, range((0, 0, 0), (25, 1, 15)));
        assert_eq!(chunks[0].ranges.len(), 2);

        // Adjacent lines merge only with a positive threshold.
        let adjacent = vec![range((0, 0, 0), (10, 0, 10)), range((11, 1, 0), (20, 1, 9))];
        assert_eq!(chunk_ranges(adjacent.clone(), 0).len(), 2);
        let merged = chunk_ranges(adjacent, 1);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].cover, range((0, 0, 0), (20, 1, 9)));
    }

    #[test]
    fn adds_context_lines() {
        let cases: &[(&str, i32, Range, &str)] = &[
            ("", 0, range((0, 0, 0), (0, 0, 0)), ""),
            ("\n", 1, range((0, 0, 0), (0, 0, 0)), "\n"),
            ("\n\n\n\n", 1, range((1, 1, 0), (1, 1, 0)), "\n\n\n"),
            ("abc\ndef\nghi\n", 0, range((1, 0, 1), (1, 0, 1)), "abc\n"),
            (
                "abc\ndef\nghi\n",
                1,
                range((1, 0, 1), (1, 0, 1)),
                "abc\ndef\n",
            ),
            (
                "abc\ndef\nghi",
                2,
                range((5, 1, 1), (6, 1, 2)),
                "abc\ndef\nghi",
            ),
            ("abc", 1, range((1, 0, 1), (2, 0, 2)), "abc"),
            (
                "abc\r\ndef\r\nghi\r\n",
                1,
                range((1, 0, 1), (2, 0, 2)),
                "abc\r\ndef\r\n",
            ),
        ];
        for (file, context_lines, input, expected) in cases {
            let buf = file.as_bytes();
            let extended = extend_range_to_lines(*input, buf);
            let with_context = add_context_lines(extended, buf, *context_lines);
            let got = &buf[with_context.start.offset as usize..with_context.end.offset as usize];
            assert_eq!(
                std::str::from_utf8(got).expect("utf8"),
                *expected,
                "file {file:?} context {context_lines}"
            );
        }
    }

    #[test]
    fn computes_columns() {
        let mut helper = ColumnHelper::default();
        let buf = b"hello\nworld";
        assert_eq!(helper.get(buf, 6, 8), 2);
        assert_eq!(helper.get(buf, 6, 8), 2);
        // Going backwards must not reuse the cache.
        assert_eq!(helper.get(buf, 6, 6), 0);
    }

    #[test]
    fn counts_invalid_bytes_like_go() {
        assert_eq!(go_rune_count(b"abc"), 3);
        assert_eq!(go_rune_count("é".as_bytes()), 1);
        // U+FFFD byte sequence is one rune; raw invalid bytes are one each.
        assert_eq!(go_rune_count(&[0xe2, 0x9d, 0x89]), 1);
        assert_eq!(go_rune_count(&[0xff, 0xff]), 2);
    }

    #[test]
    fn builds_file_matches_with_locations() {
        let buf = b"line one\nline two\nline three\n";
        let locs = vec![(9, 13), (14, 17)];
        let file_match = locs_to_file_match(buf, "file.txt", &locs, 0);
        assert_eq!(file_match.path, "file.txt");
        assert_eq!(file_match.chunk_matches.len(), 1);
        let chunk = &file_match.chunk_matches[0];
        assert_eq!(chunk.content, "line two\n");
        assert_eq!(chunk.content_start, loc(9, 1, 0));
        assert_eq!(chunk.ranges[0], range((9, 1, 0), (13, 1, 4)));
        assert_eq!(chunk.ranges[1], range((14, 1, 5), (17, 1, 8)));

        // A zero-width match still yields a file match.
        let file_match = locs_to_file_match(buf, "file.txt", &[], 0);
        assert_eq!(file_match.match_count(), 1);
        assert!(file_match.chunk_matches.is_empty());
    }
}
