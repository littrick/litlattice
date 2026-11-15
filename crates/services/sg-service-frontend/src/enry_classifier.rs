#![forbid(unsafe_code)]

//! go-enry v2.8.8 `GetLanguagesByClassifier` — the Naive Bayes model and the
//! Linguist tokenizer it consumes (`classifier.go`, `internal/tokenizer`).
//!
//! This is the classifier that `lib/codeintel/languages.GetLanguages` runs as
//! the last strategy. Before this port, ambiguous extensions whose content
//! heuristics found nothing kept enry's raw candidate list; the baseline ranks
//! that list by the trained model. The model data is embedded from
//! [`enry_classifier_data`] (generated from `data/frequencies.go`).
//!
//! Data scope: the generator keeps only the languages that can reach the
//! classifier through `GetLanguages` (values of multi-valued extension,
//! filename, interpreter and content-heuristic entries — 233 languages). A
//! candidate outside that set has prior `0` and the unknown-token fallback,
//! which still matches Go for candidates it contains; the omitted rows would
//! only matter for direct `GetLanguagesByClassifier` calls.
//!
//! ## Deliberate deviations (documented errata)
//!
//! - Go iterates `map[string]float64` when scoring, so candidates with equal
//!   scores have a randomized relative order (the sort is stable). The port
//!   uses first-seen order instead, making ties deterministic. The
//!   differential corpus avoids tie cases (Go itself is non-deterministic
//!   there).
//! - The tokenizer's regexes are Go RE2; the `regex` crate is used in bytes
//!   mode. `\s`/`\S`/`\d` are spelled out as ASCII classes to match Go. For
//!   invalid UTF-8 the lossy decoding used by `extractRemainders` differs from
//!   Go's `RuneError`-per-byte splitting, but such content is not a language
//!   sample.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::enry_data::LANGUAGE_BY_ALIAS;

/// `tokenizer.ByteLimit`.
const BYTE_LIMIT: usize = 100_000;

/// The embedded model: `N <tokensTotal>`, `L <lang> <logprob>`,
/// `T <lang> <token> <logprob>`. Parsed as bytes because some Linguist tokens
/// are not valid UTF-8; they are decoded lossily (matching the tokenizer's
/// lossy remainder extraction).
const MODEL_DATA: &[u8] = include_bytes!("enry_classifier_data.txt");

struct Model {
    tokens_total: f64,
    priors: HashMap<String, f64>,
    tokens: HashMap<String, HashMap<String, f64>>,
}

fn model() -> &'static Model {
    static MODEL: OnceLock<Model> = OnceLock::new();
    MODEL.get_or_init(|| {
        let mut tokens_total = 1.0;
        let mut priors = HashMap::new();
        let mut tokens: HashMap<String, HashMap<String, f64>> = HashMap::new();
        for line in MODEL_DATA.split(|byte| *byte == b'\n') {
            let mut fields = line.split(|byte| *byte == b'\t');
            let kind = fields.next();
            match kind {
                Some(kind) if kind == b"N" => {
                    if let Some(value) = fields.next().and_then(parse_f64) {
                        tokens_total = value;
                    }
                }
                Some(kind) if kind == b"L" => {
                    let (Some(lang), Some(prob)) =
                        (fields.next(), fields.next().and_then(parse_f64))
                    else {
                        continue;
                    };
                    priors.insert(String::from_utf8_lossy(lang).into_owned(), prob);
                }
                Some(kind) if kind == b"T" => {
                    let (Some(lang), Some(token), Some(prob)) = (
                        fields.next(),
                        fields.next(),
                        fields.next().and_then(parse_f64),
                    ) else {
                        continue;
                    };
                    tokens
                        .entry(String::from_utf8_lossy(lang).into_owned())
                        .or_default()
                        .insert(String::from_utf8_lossy(token).into_owned(), prob);
                }
                _ => {}
            }
        }
        Model {
            tokens_total,
            priors,
            tokens,
        }
    })
}

fn parse_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// enry `GetLanguageByAlias` for classifier candidates: normalize the alias
/// (`convertToAliasKey`: first comma segment, spaces to `_`, lowercase) and
/// look it up in the aliases table.
fn language_by_alias(candidate: &str) -> Option<String> {
    let first = candidate.split(',').next().unwrap_or(candidate);
    let key = first.replace(' ', "_").to_lowercase();
    LANGUAGE_BY_ALIAS
        .binary_search_by(|(alias, _)| (*alias).cmp(key.as_str()))
        .ok()
        .map(|index| LANGUAGE_BY_ALIAS[index].1.to_owned())
}

/// go-enry `naiveBayes.classify`: rank `candidates` by decreasing probability.
///
/// Candidate weights are irrelevant to the score (Go only uses their
/// presence), and alias-equal candidates collapse, exactly like the map in
/// `getLanguagesBySpecificClassifier` + `classify`.
pub fn classify(content: &[u8], candidates: &[String]) -> Vec<String> {
    let mut languages: Vec<(String, f64)> = Vec::with_capacity(candidates.len());
    let mut seen: HashMap<String, usize> = HashMap::new();
    for candidate in candidates {
        let language = language_by_alias(candidate).unwrap_or_else(|| candidate.clone());
        if let Some(&index) = seen.get(&language) {
            languages[index].1 += 1.0;
        } else {
            seen.insert(language.clone(), languages.len());
            languages.push((language, 1.0));
        }
    }

    let empty = content.is_empty();
    let tokens = if empty {
        Vec::new()
    } else {
        tokenize(content)
    };

    let model = model();
    let mut scored: Vec<(String, f64)> = languages
        .into_iter()
        .map(|(language, _weight)| {
            let mut score = *model.priors.get(&language).unwrap_or(&0.0);
            if !empty {
                score += tokens_log_probability(model, &tokens, &language);
            }
            (language, score)
        })
        .collect();

    // `sort.Stable(byScore)`, decreasing.
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().map(|(language, _)| language).collect()
}

fn tokens_log_probability(model: &Model, tokens: &[String], language: &str) -> f64 {
    let fallback = (1.000_000 / model.tokens_total).ln();
    let table = model.tokens.get(language);
    tokens
        .iter()
        .map(|token| {
            table
                .and_then(|table| table.get(token))
                .copied()
                .unwrap_or(fallback)
        })
        .sum()
}

// ---------------------------------------------------------------------------
// `internal/tokenizer.Tokenize` (RE2 build)
// ---------------------------------------------------------------------------

struct TokenizerRegexes {
    literal_string_quotes: regex::bytes::Regex,
    single_line_comment: regex::bytes::Regex,
    multiline_comment: regex::bytes::Regex,
    literal_number: regex::bytes::Regex,
    shebang: regex::bytes::Regex,
    punctuation: regex::bytes::Regex,
    sgml: regex::bytes::Regex,
    sgml_comment: regex::bytes::Regex,
    sgml_attributes: regex::bytes::Regex,
    sgml_lone_attribute: regex::bytes::Regex,
    regular_token: regex::bytes::Regex,
    operators: regex::bytes::Regex,
}

fn regexes() -> &'static TokenizerRegexes {
    static REGEXES: OnceLock<TokenizerRegexes> = OnceLock::new();
    REGEXES.get_or_init(|| {
        let compile = |pattern: &str| {
            regex::bytes::Regex::new(pattern).unwrap_or_else(|error| panic!("{pattern}: {error}"))
        };
        TokenizerRegexes {
            literal_string_quotes: compile(r#"("(.|\n)*?"|'(.|\n)*?')"#),
            single_line_comment: compile(r#"(?m)(//|--|#|%|")[ \t\n\f\r]([^\n]*$)"#),
            multiline_comment: compile(
                r#"(/\*(.|\n)*?\*/|<!--(.|\n)*?-->|\{-(.|\n)*?-\}|\(\*(.|\n)*?\*\)|"""(.|\n)*?"""|'''(.|\n)*?''')"#,
            ),
            literal_number: compile(
                r"(0x[0-9A-Fa-f]([0-9A-Fa-f]|\.)*|[0-9]([0-9]|\.)*)([uU][lL]{0,2}|([eE][-+][0-9]*)?[fFlL]*)",
            ),
            shebang: compile(
                r"(?m)^#!(?:/[0-9A-Za-z_]+)*/(?:([0-9A-Za-z_]+)|[0-9A-Za-z_]+(?:[ \t\n\f\r]*[0-9A-Za-z_]+=[0-9A-Za-z_]+[ \t\n\f\r]*)*[ \t\n\f\r]*([0-9A-Za-z_]+))(?:[ \t\n\f\r]*-[0-9A-Za-z_]+[ \t\n\f\r]*)*$",
            ),
            punctuation: compile(r";|\{|\}|\(|\)|\[|\]"),
            sgml: compile(r#"(</?[^ \t\n\f\r<>=[0-9]"']+)(?:[ \t\n\f\r](.|\n)*?/?>|>)"#),
            sgml_comment: compile(r"(<!--(.|\n)*?-->)"),
            sgml_attributes: compile(r"[ \t\n\f\r]+([0-9A-Za-z_]+=)|[ \t\n\f\r]+([^ \t\n\f\r>]+)"),
            sgml_lone_attribute: compile(r"([0-9A-Za-z_]+)"),
            regular_token: compile(r"[0-9A-Za-z_\.@#\/\*]+"),
            operators: compile(r"<<?|\+|\-|\*|\/|%|&&?|\|\|?"),
        }
    })
}

/// enry `tokenizer.Tokenize`: the exact seven-step extraction pipeline.
pub fn tokenize(content: &[u8]) -> Vec<String> {
    let content = &content[..content.len().min(BYTE_LIMIT)];
    let regexes = regexes();
    let mut tokens: Vec<String> = Vec::new();

    let content = {
        let (content, mut extracted) = extract_and_replace_shebang(content, regexes);
        tokens.append(&mut extracted);
        content
    };
    let content = {
        let (content, mut extracted) = extract_and_replace_sgml(&content, regexes);
        tokens.append(&mut extracted);
        content
    };
    let content = skip_comments_and_literals(&content, regexes);
    let content = {
        let (content, mut extracted) = extract_and_replace(&content, &regexes.punctuation);
        tokens.append(&mut extracted);
        content
    };
    let content = {
        let (content, mut extracted) = extract_and_replace(&content, &regexes.regular_token);
        tokens.append(&mut extracted);
        content
    };
    let content = {
        let (content, mut extracted) = extract_and_replace(&content, &regexes.operators);
        tokens.append(&mut extracted);
        content
    };
    tokens.append(&mut extract_remainders(&content));
    tokens
}

fn to_strings(tokens: Vec<Vec<u8>>) -> Vec<String> {
    tokens
        .into_iter()
        .map(|token| String::from_utf8_lossy(&token).into_owned())
        .collect()
}

fn find_all(regex: &regex::bytes::Regex, content: &[u8]) -> Vec<Vec<u8>> {
    regex
        .find_iter(content)
        .map(|matched| matched.as_bytes().to_vec())
        .collect()
}

fn replace_all(regex: &regex::bytes::Regex, content: &[u8]) -> Vec<u8> {
    regex.replace_all(content, &b" "[..]).into_owned()
}

/// `extractAndReplaceShebang`. Go discards the `ReplaceAll` result (the
/// vendored code calls it without assigning), so the content is unchanged.
fn extract_and_replace_shebang(
    content: &[u8],
    regexes: &TokenizerRegexes,
) -> (Vec<u8>, Vec<String>) {
    let mut tokens = Vec::new();
    for captures in regexes.shebang.captures_iter(content) {
        let mut token = Vec::new();
        for index in 1..captures.len() {
            if let Some(group) = captures.get(index) {
                if !group.as_bytes().is_empty() {
                    token = group.as_bytes().to_vec();
                    break;
                }
            }
        }
        let mut shebang = b"SHEBANG#!".to_vec();
        shebang.extend_from_slice(&token);
        tokens.push(shebang);
    }
    (content.to_vec(), to_strings(tokens))
}

fn extract_and_replace(
    content: &[u8],
    regex: &regex::bytes::Regex,
) -> (Vec<u8>, Vec<String>) {
    let tokens = find_all(regex, content);
    (replace_all(regex, content), to_strings(tokens))
}

fn extract_and_replace_sgml(
    content: &[u8],
    regexes: &TokenizerRegexes,
) -> (Vec<u8>, Vec<String>) {
    let mut tokens: Vec<Vec<u8>> = Vec::new();
    for captures in regexes.sgml.captures_iter(content) {
        let Some(whole) = captures.get(0) else {
            continue;
        };
        if regexes.sgml_comment.is_match(whole.as_bytes()) {
            continue;
        }
        let Some(name) = captures.get(1) else {
            continue;
        };
        let mut token = name.as_bytes().to_vec();
        token.push(b'>');
        tokens.push(token);
        tokens.extend(sgml_attributes(whole.as_bytes(), regexes));
    }
    (replace_all(&regexes.sgml, content), to_strings(tokens))
}

fn find_all_submatch(regex: &regex::bytes::Regex, content: &[u8]) -> Vec<Vec<Vec<u8>>> {
    regex
        .captures_iter(content)
        .map(|captures| {
            (0..captures.len())
                .map(|index| {
                    captures
                        .get(index)
                        .map(|group| group.as_bytes().to_vec())
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect()
}

fn sgml_attributes(content: &[u8], regexes: &TokenizerRegexes) -> Vec<Vec<u8>> {
    let mut attributes = Vec::new();
    for groups in find_all_submatch(&regexes.sgml_attributes, content) {
        if groups.len() > 1 && !groups[1].is_empty() {
            attributes.push(groups[1].clone());
        }
        if groups.len() > 2 && !groups[2].is_empty() {
            attributes.extend(find_all(&regexes.sgml_lone_attribute, &groups[2]));
        }
    }
    attributes
}

fn skip_comments_and_literals(content: &[u8], regexes: &TokenizerRegexes) -> Vec<u8> {
    let mut content = content.to_vec();
    for regex in [
        &regexes.literal_string_quotes,
        &regexes.multiline_comment,
        &regexes.single_line_comment,
        &regexes.literal_number,
    ] {
        content = replace_all(regex, &content);
    }
    content
}

/// `extractRemainders`: `bytes.Fields`, then `bytes.Split(field, nil)` (one
/// token per UTF-8 rune).
fn extract_remainders(content: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(content);
    let mut tokens = Vec::new();
    for field in text.split_whitespace() {
        for character in field.chars() {
            tokens.push(character.to_string());
        }
    }
    tokens
}
