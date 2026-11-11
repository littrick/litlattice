//! `internal/embeddings/embed/client/modeltransformations`.
//!
//! Two model-specific rewrites applied before embedding:
//!
//! * E5-like models (`scout*` / `e5*` in the last `/` segment) get a
//!   `query: ` / `passage: ` prefix for queries/documents;
//! * `openai/text-embedding-ada-002` replaces newlines with spaces because the
//!   model performs poorly on multi-line input.

/// `E5_QUERY_PREFIX`.
pub const E5_QUERY_PREFIX: &str = "query: ";
/// `E5_DOCUMENT_PREFIX`.
pub const E5_DOCUMENT_PREFIX: &str = "passage: ";
/// `modelsWithoutNewlines`.
pub const MODELS_WITHOUT_NEWLINES: [&str; 1] = ["openai/text-embedding-ada-002"];

/// `isE5LikeModel`.
pub fn is_e5_like_model(model: &str) -> bool {
    let model_name = model.rsplit('/').next().unwrap_or(model);
    model_name.starts_with("scout") || model_name.starts_with("e5")
}

fn replaces_newlines(model: &str) -> bool {
    MODELS_WITHOUT_NEWLINES.contains(&model)
}

/// `ApplyToQuery`.
pub fn apply_to_query(query: &str, model: &str) -> String {
    let mut transformed = query.to_owned();
    if is_e5_like_model(model) {
        transformed = format!("{E5_QUERY_PREFIX}{transformed}");
    }
    if replaces_newlines(model) {
        transformed = transformed.replace('\n', " ");
    }
    transformed
}

/// `ApplyToDocuments`.
pub fn apply_to_documents(documents: &[String], model: &str) -> Vec<String> {
    let replace_newlines = replaces_newlines(model);
    let has_e5_prefix = is_e5_like_model(model);
    documents
        .iter()
        .map(|document| {
            let mut transformed = document.clone();
            if has_e5_prefix {
                transformed = format!("{E5_DOCUMENT_PREFIX}{transformed}");
            }
            if replace_newlines {
                transformed = transformed.replace('\n', " ");
            }
            transformed
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_model_strips_newlines() {
        assert_eq!(
            apply_to_query("query1\nquery2\nquery3", "openai/text-embedding-ada-002"),
            "query1 query2 query3"
        );
        assert_eq!(
            apply_to_documents(
                &[
                    "line1\nline2\nline3".to_owned(),
                    "line4\nline5\nline6".to_owned()
                ],
                "openai/text-embedding-ada-002"
            ),
            vec!["line1 line2 line3", "line4 line5 line6"]
        );
    }

    #[test]
    fn e5_like_models_prepend_the_prefix() {
        assert_eq!(
            apply_to_query("query1\nquery2\nquery3", "sourcegraph/scout-base-v2"),
            "query: query1\nquery2\nquery3"
        );
        assert_eq!(
            apply_to_documents(
                &[
                    "line1\nline2\nline3".to_owned(),
                    "line4\nline5\nline6".to_owned()
                ],
                "sourcegraph/scout-base-v2"
            ),
            vec![
                "passage: line1\nline2\nline3",
                "passage: line4\nline5\nline6"
            ]
        );
        assert!(is_e5_like_model("sourcegraph/e5-large"));
        assert!(is_e5_like_model("e5"));
        assert!(!is_e5_like_model("sourcegraph/not-e5"));
    }

    #[test]
    fn other_models_are_untouched() {
        assert_eq!(apply_to_query("q\nq", "no-transform"), "q\nq");
        assert_eq!(
            apply_to_documents(&["a\nb".to_owned()], "no-transform"),
            vec!["a\nb"]
        );
    }
}
