//! M9.8ih differential harness: convert the full `protocol.CommitMatch` into
//! the enriched `convert::CommitMatch` model and encode it with
//! `search_stream::events::encode_protocol_commit_match`, then compare the
//! bytes with Go's `encoding/json` oracle
//! (`internal/gitserver/protocol/zz_m9ih_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9IH_DIFF=1` enable the harness
//! * `M9IH_CORPUS` corpus JSON
//! * `M9IH_OUT` output JSON

use prost_types::Timestamp;
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_proto::gitserver as pb;
use sg_service_frontend::search_commit_job::protocol_match_to_commit_match;
use sg_service_frontend::search_jobutil_filters::{DiffFile, Hunk};
use sg_service_frontend::search_stream::convert::MinimalRepo;
use sg_service_frontend::search_stream::events::encode_protocol_commit_match;
use sg_service_frontend::search_stream::json::push_quoted;

#[derive(Debug, Deserialize)]
struct Case {
    /// Deserialized only so malformed corpora keep the Go-compatible shape.
    #[allow(dead_code)]
    name: String,
    #[serde(default)]
    diff: bool,
    repo: RepoSpec,
    #[serde(rename = "match")]
    protocol_match: MatchSpec,
}

#[derive(Debug, Deserialize)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
struct MatchSpec {
    #[serde(rename = "Oid", default)]
    oid: String,
    #[serde(rename = "Author")]
    author: SignatureSpec,
    #[serde(rename = "Committer")]
    committer: SignatureSpec,
    #[serde(rename = "Parents", default)]
    parents: Vec<String>,
    #[serde(rename = "Refs", default)]
    refs: Vec<String>,
    #[serde(rename = "SourceRefs", default)]
    source_refs: Vec<String>,
    #[serde(rename = "Message")]
    message: MatchedStringSpec,
    #[serde(rename = "Diff")]
    diff: MatchedStringSpec,
    #[serde(rename = "ModifiedFiles", default)]
    modified_files: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SignatureSpec {
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "Email", default)]
    email: String,
    #[serde(rename = "Date")]
    date: String,
}

#[derive(Debug, Deserialize)]
struct MatchedStringSpec {
    #[serde(default)]
    content: String,
    #[serde(rename = "matchedRanges", default)]
    ranges: Vec<RangeSpec>,
}

#[derive(Debug, Deserialize)]
struct RangeSpec {
    start: [i64; 3],
    end: [i64; 3],
}

fn timestamp(value: &str) -> Timestamp {
    let parsed = OffsetDateTime::parse(value, &Rfc3339)
        .unwrap_or_else(|error| panic!("bad date {value}: {error}"));
    Timestamp {
        seconds: parsed.unix_timestamp(),
        nanos: parsed.nanosecond() as i32,
    }
}

fn pb_signature(spec: &SignatureSpec) -> pb::commit_match::Signature {
    pb::commit_match::Signature {
        name: spec.name.clone(),
        email: spec.email.clone(),
        date: Some(timestamp(&spec.date)),
    }
}

fn pb_location(location: [i64; 3]) -> pb::commit_match::Location {
    pb::commit_match::Location {
        offset: location[0] as u32,
        line: location[1] as u32,
        column: location[2] as u32,
    }
}

fn pb_matched_string(spec: &MatchedStringSpec) -> pb::commit_match::MatchedString {
    pb::commit_match::MatchedString {
        content: spec.content.clone(),
        ranges: spec
            .ranges
            .iter()
            .map(|range| pb::commit_match::Range {
                start: Some(pb_location(range.start)),
                end: Some(pb_location(range.end)),
            })
            .collect(),
    }
}

fn pb_match(spec: &MatchSpec) -> pb::CommitMatch {
    pb::CommitMatch {
        oid: spec.oid.clone(),
        author: Some(pb_signature(&spec.author)),
        committer: Some(pb_signature(&spec.committer)),
        parents: spec.parents.clone(),
        refs: spec.refs.clone(),
        source_refs: spec.source_refs.clone(),
        message: Some(pb_matched_string(&spec.message)),
        diff: Some(pb_matched_string(&spec.diff)),
        modified_files: spec.modified_files.clone(),
    }
}

fn field_str(out: &mut String, key: &str, value: &str) {
    if !out.ends_with('{') {
        out.push(',');
    }
    push_quoted(out, key);
    out.push(':');
    push_quoted(out, value);
}

fn field_int(out: &mut String, key: &str, value: i64) {
    if !out.ends_with('{') {
        out.push(',');
    }
    push_quoted(out, key);
    out.push(':');
    out.push_str(&value.to_string());
}

fn encode_hunk(out: &mut String, hunk: &Hunk) {
    out.push('{');
    // Go declaration order: OldStart, NewStart, OldCount, NewCount, Header,
    // Lines (`internal/search/result/commit_diff.go:242-247`).
    field_int(out, "OldStart", hunk.old_start);
    field_int(out, "NewStart", hunk.new_start);
    field_int(out, "OldCount", hunk.old_count);
    field_int(out, "NewCount", hunk.new_count);
    field_str(out, "Header", &hunk.header);
    if !out.ends_with('{') {
        out.push(',');
    }
    push_quoted(out, "Lines");
    out.push(':');
    if hunk.lines.is_empty() {
        // Go's parser leaves `Lines` nil when a hunk has no lines.
        out.push_str("null");
    } else {
        out.push('[');
        for (i, line) in hunk.lines.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            push_quoted(out, line);
        }
        out.push(']');
    }
    out.push('}');
}

fn encode_diff_files(files: &[DiffFile]) -> String {
    let mut out = String::from("[");
    for (i, file) in files.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        field_str(&mut out, "OrigName", &file.orig_name);
        field_str(&mut out, "NewName", &file.new_name);
        if !out.ends_with('{') {
            out.push(',');
        }
        push_quoted(&mut out, "Hunks");
        out.push_str(":[");
        for (j, hunk) in file.hunks.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            encode_hunk(&mut out, hunk);
        }
        out.push(']');
        out.push('}');
    }
    out.push(']');
    out
}

struct EncodedCase {
    protocol: String,
    structured: String,
}

fn run_case(case: &Case) -> EncodedCase {
    let repo = MinimalRepo {
        id: case.repo.id,
        name: case.repo.name.clone(),
    };
    let model = protocol_match_to_commit_match(&repo, case.diff, &pb_match(&case.protocol_match));
    EncodedCase {
        protocol: encode_protocol_commit_match(&model),
        structured: encode_diff_files(&model.diff),
    }
}

#[test]
fn commit_match_model_diff() {
    if std::env::var("SG_M9IH_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IH_CORPUS").expect("M9IH_CORPUS");
    let out_path = std::env::var("M9IH_OUT").expect("M9IH_OUT");
    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let encoded: Vec<EncodedCase> = cases.iter().map(run_case).collect();

    let mut out = String::from("{\"protocol\":[");
    for (i, case) in encoded.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&case.protocol);
    }
    out.push_str("],\"structured\":[");
    for (i, case) in encoded.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&case.structured);
    }
    out.push_str("]}");

    std::fs::write(&out_path, out).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
