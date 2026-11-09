#![forbid(unsafe_code)]

//! `internal/search/query/labels.go`.

/// General-purpose annotations attached to a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Labels(u16);

impl Labels {
    pub const NONE: Labels = Labels(0);
    pub const LITERAL: Labels = Labels(1 << 0);
    pub const REGEXP: Labels = Labels(1 << 1);
    pub const QUOTED: Labels = Labels(1 << 2);
    pub const HEURISTIC_PARENS_AS_PATTERNS: Labels = Labels(1 << 3);
    pub const HEURISTIC_DANGLING_PARENS: Labels = Labels(1 << 4);
    pub const HEURISTIC_HOISTED: Labels = Labels(1 << 5);
    pub const STRUCTURAL: Labels = Labels(1 << 6);
    pub const IS_PREDICATE: Labels = Labels(1 << 7);
    pub const IS_ALIAS: Labels = Labels(1 << 8);
    pub const STANDARD: Labels = Labels(1 << 9);
    pub const QUOTES_AS_LITERALS: Labels = Labels(1 << 10);
    pub const BOOST: Labels = Labels(1 << 11);
    pub const IS_CONTENT: Labels = Labels(1 << 12);

    pub const fn raw(self) -> u16 {
        self.0
    }

    pub fn is_set(self, label: Labels) -> bool {
        self.0 & label.0 != 0
    }

    pub fn set(&mut self, label: Labels) {
        self.0 |= label.0;
    }

    pub fn unset(&mut self, label: Labels) {
        self.0 &= !label.0;
    }

    /// `labels.String()`, sorted names of the set labels.
    pub fn names(self) -> Vec<&'static str> {
        if self.0 == 0 {
            return vec!["None"];
        }
        let all = [
            (Self::LITERAL, "Literal"),
            (Self::REGEXP, "Regexp"),
            (Self::QUOTED, "Quoted"),
            (
                Self::HEURISTIC_PARENS_AS_PATTERNS,
                "HeuristicParensAsPatterns",
            ),
            (Self::HEURISTIC_DANGLING_PARENS, "HeuristicDanglingParens"),
            (Self::HEURISTIC_HOISTED, "HeuristicHoisted"),
            (Self::STRUCTURAL, "Structural"),
            (Self::IS_PREDICATE, "IsPredicate"),
            (Self::IS_ALIAS, "IsAlias"),
            (Self::STANDARD, "Standard"),
            (Self::QUOTES_AS_LITERALS, "QuotesAsLiterals"),
            (Self::BOOST, "Boost"),
            (Self::IS_CONTENT, "IsContent"),
        ];
        let mut names: Vec<&'static str> = all
            .iter()
            .filter(|(label, _)| self.is_set(*label))
            .map(|(_, name)| *name)
            .collect();
        names.sort_unstable();
        names
    }
}

impl std::ops::BitOr for Labels {
    type Output = Labels;
    fn bitor(self, rhs: Labels) -> Labels {
        Labels(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for Labels {
    fn bitor_assign(&mut self, rhs: Labels) {
        self.0 |= rhs.0;
    }
}
