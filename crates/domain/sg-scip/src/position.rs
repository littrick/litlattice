#![forbid(unsafe_code)]

//! Port of `position.go`: the `[]int32` range encoding helpers.

use std::cmp::Ordering;

/// `scip.Position`: a zero-based line/character offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Position {
    pub line: i32,
    pub character: i32,
}

impl Position {
    pub fn new(line: i32, character: i32) -> Self {
        Self { line, character }
    }

    /// `Position.Compare`.
    pub fn compare(&self, other: &Position) -> Ordering {
        self.line
            .cmp(&other.line)
            .then_with(|| self.character.cmp(&other.character))
    }

    /// `Position.Less`.
    pub fn less(&self, other: &Position) -> bool {
        self.compare(other) == Ordering::Less
    }
}

/// `scip.Range`: `[start, end)` between two positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

impl Range {
    /// `NewRangeUnchecked`. The caller must have already filtered ranges to
    /// length 3 or 4 (`RemoveIllegalOccurrences`).
    pub fn new_unchecked(scip_range: &[i32]) -> Self {
        assert!(
            scip_range.len() == 3 || scip_range.len() == 4,
            "SCIP range must have 3 or 4 elements"
        );
        let mut end_character = scip_range[2];
        let mut end_line = scip_range[0];
        if scip_range.len() == 4 {
            end_character = scip_range[3];
            end_line = scip_range[2];
        }
        Self {
            start: Position::new(scip_range[0], scip_range[1]),
            end: Position::new(end_line, end_character),
        }
    }

    /// `Range.SCIPRange`: encode as three components when the range is
    /// single-line, four otherwise.
    pub fn scip_range(&self) -> Vec<i32> {
        if self.start.line == self.end.line {
            vec![self.start.line, self.start.character, self.end.character]
        } else {
            vec![
                self.start.line,
                self.start.character,
                self.end.line,
                self.end.character,
            ]
        }
    }

    /// `Range.CompareStrict`.
    pub fn compare_strict(&self, other: &Range) -> Ordering {
        self.start
            .compare(&other.start)
            .then_with(|| self.end.compare(&other.end))
    }

    /// `Range.LessStrict`.
    pub fn less_strict(&self, other: &Range) -> bool {
        self.compare_strict(other) == Ordering::Less
    }
}
