#![forbid(unsafe_code)]

//! `internal/search/query/range.go`.

/// A position in the original query string. The baseline keeps every column on
/// line 0 because a query is a single line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Position {
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

impl Range {
    /// `newRange`: column positions are in the interval `[start, end]`.
    pub fn new(start: usize, end: usize) -> Range {
        Range {
            start: Position {
                line: 0,
                column: start,
            },
            end: Position {
                line: 0,
                column: end,
            },
        }
    }
}

impl std::fmt::Display for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Go's `Range.String` marshals the struct to JSON:
        // {"start":{"line":0,"column":1},"end":{"line":0,"column":2}}
        write!(
            f,
            "{{\"start\":{{\"line\":{},\"column\":{}}},\"end\":{{\"line\":{},\"column\":{}}}}}",
            self.start.line, self.start.column, self.end.line, self.end.column
        )
    }
}
