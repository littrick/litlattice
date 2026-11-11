//! Port of `github.com/kljensen/snowball@v0.6.0`'s English stemmer
//! (`english/*.go`, `snowballword/snowballword.go`, `romance/common.go`).
//!
//! `codycontext.stemTerm` calls `snowball.Stem(input, "english", false)`; the
//! only observable result is the stemmed string, so the port keeps the rune
//! vector and the R1/R2 bookkeeping of the original and reproduces the step
//! functions verbatim.

use super::stop_words::is_english_stop_word;

struct SnowballWord {
    /// `RS []rune`.
    rs: Vec<char>,
    /// `R1start`.
    r1start: usize,
    /// `R2start`.
    r2start: usize,
}

impl SnowballWord {
    fn new(word: &str) -> Self {
        let rs: Vec<char> = word.chars().collect();
        let len = rs.len();
        SnowballWord {
            rs,
            r1start: len,
            r2start: len,
        }
    }

    fn len(&self) -> usize {
        self.rs.len()
    }

    fn text(&self) -> String {
        self.rs.iter().collect()
    }

    /// `RemoveLastNRunes`.
    fn remove_last_n(&mut self, n: usize) {
        self.rs.truncate(self.rs.len() - n);
        self.reset_r1_r2();
    }

    /// `ReplaceSuffixRunes(..., true)`: the caller has already checked the
    /// suffix, matching every `english` step.
    fn replace_suffix_runes(&mut self, suffix_len: usize, replacement: &[char]) {
        let len_without_suffix = self.rs.len() - suffix_len;
        self.rs.truncate(len_without_suffix);
        self.rs.extend_from_slice(replacement);
        self.reset_r1_r2();
    }

    /// `resetR1R2`.
    fn reset_r1_r2(&mut self) {
        let rs_len = self.rs.len();
        if self.r1start > rs_len {
            self.r1start = rs_len;
        }
        if self.r2start > rs_len {
            self.r2start = rs_len;
        }
    }

    /// `FirstPrefix`.
    fn first_prefix(&self, prefixes: &[&str]) -> Option<usize> {
        for prefix in prefixes {
            let prefix: Vec<char> = prefix.chars().collect();
            if prefix.len() > self.rs.len() {
                continue;
            }
            if self.rs[..prefix.len()] == prefix[..] {
                return Some(prefix.len());
            }
        }
        None
    }

    /// `HasSuffixRunesIn(startPos, endPos, suffixRunes)`.
    fn has_suffix_runes_in(&self, start_pos: usize, end_pos: usize, suffix: &str) -> bool {
        let suffix: Vec<char> = suffix.chars().collect();
        let max_len = end_pos.saturating_sub(start_pos);
        if suffix.len() > max_len {
            return false;
        }
        for i in 0..max_len.min(suffix.len()) {
            if self.rs[end_pos - i - 1] != suffix[suffix.len() - i - 1] {
                return false;
            }
        }
        true
    }

    /// `FirstSuffixIfIn(0, len(RS), suffixes...)` (`FirstSuffix`).
    fn first_suffix(&self, suffixes: &[&'static str]) -> Option<&'static str> {
        let end_pos = self.rs.len();
        for suffix in suffixes {
            if self.has_suffix_runes_in(0, end_pos, suffix) {
                let suffix_len = suffix.chars().count();
                if end_pos >= suffix_len {
                    return Some(suffix);
                }
                return None;
            }
        }
        None
    }
}

/// `isLowerVowel`.
fn is_lower_vowel(c: char) -> bool {
    matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y')
}

/// `romance.VnvSuffix`.
fn vnv_suffix(word: &SnowballWord, start: usize) -> usize {
    for i in 1..(word.rs.len() - start) {
        let j = start + i;
        if is_lower_vowel(word.rs[j - 1]) && !is_lower_vowel(word.rs[j]) {
            return j + 1;
        }
    }
    word.rs.len()
}

/// `r1r2`.
fn r1r2(word: &SnowballWord) -> (usize, usize) {
    let r1start = match word.first_prefix(&["gener", "commun", "arsen"]) {
        Some(prefix_len) => prefix_len,
        None => vnv_suffix(word, 0),
    };
    (r1start, vnv_suffix(word, r1start))
}

/// `normalizeApostrophes`.
fn normalize_apostrophes(word: &mut SnowballWord) {
    for c in &mut word.rs {
        if matches!(*c as u32, 8217 | 8216 | 8219) {
            *c = '\'';
        }
    }
}

/// `trimLeftApostrophes`. The original also adjusts R1/R2 here, but
/// `preprocess` recomputes them immediately afterwards, so the adjustment is
/// unobservable.
fn trim_left_apostrophes(word: &mut SnowballWord) {
    let mut count = 0;
    for c in &word.rs {
        if *c == '\'' {
            count += 1;
        } else {
            break;
        }
    }
    // Go's `for idx, r := range RS` leaves idx at len-1 for an all-apostrophe
    // word, so it removes one fewer rune than the run of apostrophes.
    if count == word.rs.len() && !word.rs.is_empty() {
        count -= 1;
    }
    if count > 0 {
        word.rs.drain(0..count);
    }
}

/// `capitalizeYs`.
fn capitalize_ys(word: &mut SnowballWord) {
    for i in 0..word.rs.len() {
        if word.rs[i] == 'y' && (i == 0 || is_lower_vowel(word.rs[i - 1])) {
            word.rs[i] = 'Y';
        }
    }
}

/// `uncapitalizeYs`.
fn uncapitalize_ys(word: &mut SnowballWord) {
    for c in &mut word.rs {
        if *c == 'Y' {
            *c = 'y';
        }
    }
}

/// `preprocess`.
fn preprocess(word: &mut SnowballWord) {
    normalize_apostrophes(word);
    trim_left_apostrophes(word);
    capitalize_ys(word);
    let (r1start, r2start) = r1r2(word);
    word.r1start = r1start;
    word.r2start = r2start;
}

/// `isShortWord`.
fn is_short_word(word: &SnowballWord) -> bool {
    if word.r1start < word.rs.len() {
        return false;
    }
    ends_short_syllable(word, word.rs.len())
}

/// `endsShortSyllable`.
fn ends_short_syllable(word: &SnowballWord, i: usize) -> bool {
    if i == 2 {
        return is_lower_vowel(word.rs[0]) && !is_lower_vowel(word.rs[1]);
    }
    if i >= 3 {
        let s1 = word.rs[i - 1];
        let s2 = word.rs[i - 2];
        let s3 = word.rs[i - 3];
        return !is_lower_vowel(s1)
            && s1 != 'w'
            && s1 != 'x'
            && s1 != 'Y'
            && is_lower_vowel(s2)
            && !is_lower_vowel(s3);
    }
    false
}

/// `step0`.
fn step0(word: &mut SnowballWord) -> bool {
    let Some(suffix) = word.first_suffix(&["'s'", "'s", "'"]) else {
        return false;
    };
    word.remove_last_n(suffix.chars().count());
    true
}

/// `step1a`.
fn step1a(word: &mut SnowballWord) -> bool {
    let Some(suffix) = word.first_suffix(&["sses", "ied", "ies", "us", "ss", "s"]) else {
        return false;
    };
    match suffix {
        "sses" => {
            word.replace_suffix_runes(4, &['s', 's']);
            true
        }
        "ies" | "ied" => {
            let replacement = if word.len() > 4 { "i" } else { "ie" };
            let replacement: Vec<char> = replacement.chars().collect();
            word.replace_suffix_runes(3, &replacement);
            true
        }
        "us" | "ss" => false,
        "s" => {
            for i in 0..word.len().saturating_sub(2) {
                if is_lower_vowel(word.rs[i]) {
                    word.remove_last_n(1);
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// `step1b`.
fn step1b(word: &mut SnowballWord) -> bool {
    let Some(suffix) = word.first_suffix(&["eedly", "ingly", "edly", "ing", "eed", "ed"]) else {
        return false;
    };
    match suffix {
        "eed" | "eedly" => {
            let suffix_len = suffix.chars().count();
            if suffix_len <= word.len() - word.r1start {
                word.replace_suffix_runes(suffix_len, &['e', 'e']);
            }
            true
        }
        "ed" | "edly" | "ing" | "ingly" => {
            let suffix_len = suffix.chars().count();
            let mut has_lower_vowel = false;
            for i in 0..word.len() - suffix_len {
                if is_lower_vowel(word.rs[i]) {
                    has_lower_vowel = true;
                    break;
                }
            }
            if !has_lower_vowel {
                return false;
            }

            let original_r1start = word.r1start;
            let original_r2start = word.r2start;
            word.remove_last_n(suffix_len);

            if let Some(new_suffix) = word.first_suffix(&[
                "at", "bl", "iz", "bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt",
            ]) {
                match new_suffix {
                    "at" | "bl" | "iz" => {
                        let replacement = format!("{new_suffix}e");
                        let replacement: Vec<char> = replacement.chars().collect();
                        word.replace_suffix_runes(2, &replacement);
                    }
                    _ => word.remove_last_n(1),
                }
            } else if is_short_word(word) {
                word.rs.push('e');
                word.r1start = word.len();
                word.r2start = word.len();
                return true;
            }

            let rs_len = word.len();
            word.r1start = if original_r1start < rs_len {
                original_r1start
            } else {
                rs_len
            };
            word.r2start = if original_r2start < rs_len {
                original_r2start
            } else {
                rs_len
            };
            true
        }
        _ => false,
    }
}

/// `step1c`.
fn step1c(word: &mut SnowballWord) -> bool {
    let rs_len = word.len();
    if rs_len > 2
        && matches!(word.rs[rs_len - 1], 'y' | 'Y')
        && !is_lower_vowel(word.rs[rs_len - 2])
    {
        word.rs[rs_len - 1] = 'i';
        return true;
    }
    false
}

/// `step2`.
fn step2(word: &mut SnowballWord) -> bool {
    let Some(suffix) = word.first_suffix(&[
        "ational", "fulness", "iveness", "ization", "ousness", "biliti", "lessli", "tional",
        "alism", "aliti", "ation", "entli", "fulli", "iviti", "ousli", "anci", "abli", "alli",
        "ator", "enci", "izer", "bli", "ogi", "li",
    ]) else {
        return false;
    };
    let suffix_len = suffix.chars().count();
    if suffix_len > word.len() - word.r1start {
        return false;
    }

    match suffix {
        "li" => {
            let rs_len = word.len();
            if rs_len >= 3 {
                match word.rs[rs_len - 3] {
                    'c' | 'd' | 'e' | 'g' | 'h' | 'k' | 'm' | 'n' | 'r' | 't' => {
                        word.remove_last_n(suffix_len);
                        return true;
                    }
                    _ => {}
                }
            }
            false
        }
        "ogi" => {
            let rs_len = word.len();
            if rs_len >= 4 && word.rs[rs_len - 4] == 'l' {
                word.replace_suffix_runes(suffix_len, &['o', 'g']);
            }
            true
        }
        _ => {
            let replacement = match suffix {
                "tional" => "tion",
                "enci" => "ence",
                "anci" => "ance",
                "abli" => "able",
                "entli" => "ent",
                "izer" | "ization" => "ize",
                "ational" | "ation" | "ator" => "ate",
                "alism" | "aliti" | "alli" => "al",
                "fulness" => "ful",
                "ousli" | "ousness" => "ous",
                "iveness" | "iviti" => "ive",
                "biliti" | "bli" => "ble",
                "fulli" => "ful",
                "lessli" => "less",
                _ => "",
            };
            let replacement: Vec<char> = replacement.chars().collect();
            word.replace_suffix_runes(suffix_len, &replacement);
            true
        }
    }
}

/// `step3`.
fn step3(word: &mut SnowballWord) -> bool {
    let Some(suffix) = word.first_suffix(&[
        "ational", "tional", "alize", "icate", "ative", "iciti", "ical", "ful", "ness",
    ]) else {
        return false;
    };
    let suffix_len = suffix.chars().count();
    if suffix_len > word.len() - word.r1start {
        return false;
    }
    if suffix == "ative" {
        if word.len() - word.r2start >= 5 {
            word.remove_last_n(suffix_len);
            return true;
        }
        return false;
    }
    let replacement = match suffix {
        "ational" => "ate",
        "tional" => "tion",
        "alize" => "al",
        "icate" | "iciti" | "ical" => "ic",
        "ful" | "ness" => "",
        _ => "",
    };
    let replacement: Vec<char> = replacement.chars().collect();
    word.replace_suffix_runes(suffix_len, &replacement);
    true
}

/// `step4`.
fn step4(word: &mut SnowballWord) -> bool {
    let Some(suffix) = word.first_suffix(&[
        "ement", "ance", "ence", "able", "ible", "ment", "ent", "ant", "ism", "ate", "iti", "ous",
        "ive", "ize", "ion", "al", "er", "ic",
    ]) else {
        return false;
    };
    let suffix_len = suffix.chars().count();
    if suffix_len > word.len() - word.r2start {
        return false;
    }
    match suffix {
        "ion" => {
            let rs_len = word.len();
            if rs_len >= 4 {
                match word.rs[rs_len - 4] {
                    's' | 't' => {
                        word.remove_last_n(suffix_len);
                        return true;
                    }
                    _ => {}
                }
            }
            false
        }
        _ => {
            word.remove_last_n(suffix_len);
            true
        }
    }
}

/// `step5`.
fn step5(word: &mut SnowballWord) -> bool {
    let lri = word.len() - 1;
    if word.r1start > lri {
        return false;
    }
    if word.rs[lri] == 'e' {
        if word.r2start <= lri || !ends_short_syllable(word, lri) {
            word.remove_last_n(1);
            return true;
        }
        return false;
    }
    if word.r2start <= lri && word.rs[lri] == 'l' && lri >= 1 && word.rs[lri - 1] == 'l' {
        word.remove_last_n(1);
        return true;
    }
    false
}

/// `stemSpecialWord`.
fn stem_special_word(word: &str) -> Option<&'static str> {
    Some(match word {
        "skis" => "ski",
        "skies" => "sky",
        "dying" => "die",
        "lying" => "lie",
        "tying" => "tie",
        "idly" => "idl",
        "gently" => "gentl",
        "ugly" => "ugli",
        "early" => "earli",
        "only" => "onli",
        "singly" => "singl",
        "sky" => "sky",
        "news" => "news",
        "howe" => "howe",
        "atlas" => "atlas",
        "cosmos" => "cosmos",
        "bias" => "bias",
        "andes" => "andes",
        "inning" => "inning",
        "innings" => "inning",
        "outing" => "outing",
        "outings" => "outing",
        "canning" => "canning",
        "cannings" => "canning",
        "herring" => "herring",
        "herrings" => "herring",
        "earring" => "earring",
        "earrings" => "earring",
        "proceed" => "proceed",
        "proceeds" => "proceed",
        "proceeded" => "proceed",
        "proceeding" => "proceed",
        "exceed" => "exceed",
        "exceeds" => "exceed",
        "exceeded" => "exceed",
        "exceeding" => "exceed",
        "succeed" => "succeed",
        "succeeds" => "succeed",
        "succeeded" => "succeed",
        "succeeding" => "succeed",
        _ => return None,
    })
}

/// `english.Stem(word, stemStopWords)`.
pub fn stem(word: &str, stem_stop_words: bool) -> String {
    let word = word.trim().to_lowercase();
    if word.len() <= 2 || (!stem_stop_words && is_english_stop_word(&word)) {
        return word;
    }
    if let Some(special) = stem_special_word(&word) {
        return special.to_owned();
    }

    let mut w = SnowballWord::new(&word);
    preprocess(&mut w);
    step0(&mut w);
    step1a(&mut w);
    step1b(&mut w);
    step1c(&mut w);
    step2(&mut w);
    step3(&mut w);
    step4(&mut w);
    step5(&mut w);
    uncapitalize_ys(&mut w);
    w.text()
}
