#![forbid(unsafe_code)]

//! The on-disk repo embedding index (`internal/embeddings/types.go`) and the
//! similarity search the legacy `POST /search` handler runs over it
//! (`internal/embeddings/similarity_search.go`).
//!
//! The baseline stores gob. Rust stores the same logical structure as
//! [`INDEX_MAGIC`] followed by JSON (the `repo-embedding-job` worker writes
//! this format, see the M9.8io overview); gob indexes written before the
//! switch are not decodable and are reported as a format error, which the
//! cache treats like any other download failure.

use serde::{Deserialize, Serialize};

use crate::{EmbeddingSearchResult, SearchScoreDetails};

/// `embeddings.RepoEmbeddingRowMetadata`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowMetadata {
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "startLine")]
    pub start_line: i64,
    #[serde(rename = "endLine")]
    pub end_line: i64,
}

/// `embeddings.EmbeddingIndex`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingIndex {
    #[serde(default)]
    pub embeddings: Vec<i8>,
    #[serde(rename = "column_dimension", default)]
    pub column_dimension: i64,
    #[serde(rename = "row_metadata", default)]
    pub row_metadata: Vec<RowMetadata>,
    #[serde(default)]
    pub ranks: Vec<f32>,
}

impl EmbeddingIndex {
    /// `EmbeddingIndex.EstimateSize`.
    pub fn estimate_size(&self) -> u64 {
        (self.embeddings.len() + self.row_metadata.len() * (16 + 8 + 8) + self.ranks.len() * 4)
            as u64
    }

    fn row(&self, index: usize) -> &[i8] {
        let dimension = self.column_dimension.max(0) as usize;
        &self.embeddings[index * dimension..(index + 1) * dimension]
    }

    fn score(&self, query: &[i8], index: usize, use_document_ranks: bool) -> SearchScoreDetails {
        let similarity_score = 2 * dot(self.row(index), query);
        let mut rank_score = 0;
        if use_document_ranks {
            if let Some(&rank) = self.ranks.get(index) {
                let mut normalized_rank = rank / 32.0;
                if normalized_rank > 1.0 {
                    normalized_rank = 1.0;
                }
                rank_score = (1.0 * normalized_rank) as i32;
            }
        }
        SearchScoreDetails {
            score: similarity_score + rank_score,
            similarity_score,
            rank_score,
        }
    }

    /// `EmbeddingIndex.SimilaritySearch`. The baseline splits rows across
    /// workers, which does not change the selected rows or their scores; the
    /// final result is sorted by descending score.
    pub fn similarity_search(
        &self,
        query: &[i8],
        num_results: i64,
        use_document_ranks: bool,
        repo_name: &str,
        revision: &str,
    ) -> Vec<EmbeddingSearchResult> {
        if num_results <= 0 || self.embeddings.is_empty() {
            return Vec::new();
        }
        let dimension = self.column_dimension.max(0) as usize;
        if dimension == 0 || query.len() != dimension {
            return Vec::new();
        }
        let num_rows = self.row_metadata.len();
        if num_rows == 0 || self.embeddings.len() < num_rows * dimension {
            return Vec::new();
        }
        let num_results = (num_rows as i64).min(num_results) as usize;

        let mut scored: Vec<(usize, SearchScoreDetails)> = (0..num_rows)
            .map(|index| (index, self.score(query, index, use_document_ranks)))
            .collect();
        // Descending score; ties keep the lower row index first.
        scored.sort_by(|left, right| {
            right
                .1
                .score
                .cmp(&left.1.score)
                .then_with(|| left.0.cmp(&right.0))
        });
        scored.truncate(num_results);

        scored
            .into_iter()
            .map(|(index, score_details)| {
                let metadata = &self.row_metadata[index];
                EmbeddingSearchResult {
                    repo_name: repo_name.to_owned(),
                    revision: revision.to_owned(),
                    file_name: metadata.file_name.clone(),
                    start_line: metadata.start_line,
                    end_line: metadata.end_line,
                    score_details,
                }
            })
            .collect()
    }
}

/// `embeddings.RepoEmbeddingIndex`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StoredRepoEmbeddingIndex {
    #[serde(default)]
    pub repo_name: String,
    #[serde(default)]
    pub revision: String,
    #[serde(default)]
    pub embeddings_model: String,
    #[serde(default)]
    pub code_index: EmbeddingIndex,
    #[serde(default)]
    pub text_index: EmbeddingIndex,
}

impl StoredRepoEmbeddingIndex {
    /// `RepoEmbeddingIndex.EstimateSize`.
    pub fn estimate_size(&self) -> u64 {
        self.code_index.estimate_size() + self.text_index.estimate_size()
    }

    /// `RepoEmbeddingIndex.IsModelCompatible`.
    pub fn is_model_compatible(&self, model: &str) -> bool {
        self.embeddings_model.is_empty() || self.embeddings_model == model
    }
}

/// The Rust index header (see the module docs).
pub const INDEX_MAGIC: &[u8] = b"SGEMB1\n";

/// Decode an object-storage index body.
pub fn decode_index(bytes: &[u8]) -> Result<StoredRepoEmbeddingIndex, String> {
    let payload = bytes
        .strip_prefix(INDEX_MAGIC)
        .ok_or_else(|| "unrecognized embedding index format (expected SGEMB1)".to_owned())?;
    serde_json::from_slice(payload).map_err(|err| err.to_string())
}

/// `embeddings.Quantize`.
pub fn quantize(input: &[f32]) -> Vec<i8> {
    input
        .iter()
        .map(|&value| {
            let value = if value > 1.0 {
                1.0
            } else if value < -1.0 {
                -1.0
            } else {
                value
            };
            (f64::from(value) * 127.0).round() as i8
        })
        .collect()
}

/// `embeddings.Dot`: the sum of the element-wise products. The baseline has
/// architecture-specific SIMD paths; the arithmetic result is the same.
pub fn dot(left: &[i8], right: &[i8]) -> i32 {
    left.iter()
        .zip(right)
        .map(|(&left, &right)| i32::from(left) * i32::from(right))
        .sum()
}

/// `embeddings.CONTEXT_DETECTION_INDEX_NAME`.
pub const CONTEXT_DETECTION_INDEX_NAME: &str = "context_detection.embeddingindex";

/// `embeddings.GetRepoEmbeddingIndexName`.
pub fn repo_embedding_index_name(repo_id: i32) -> String {
    format!("{repo_id}.embeddingindex")
}

/// `embeddings.GetRepoEmbeddingIndexNameDeprecated`.
pub fn repo_embedding_index_name_deprecated(repo_name: &str) -> String {
    let fs_safe: String = repo_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect();
    let hash = md5::compute(repo_name.as_bytes());
    format!("{fs_safe}_{hash:x}.embeddingindex")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> EmbeddingIndex {
        EmbeddingIndex {
            embeddings: vec![127, 0, 0, 127],
            column_dimension: 2,
            row_metadata: vec![
                RowMetadata {
                    file_name: "b.go".to_owned(),
                    start_line: 0,
                    end_line: 1,
                },
                RowMetadata {
                    file_name: "a.go".to_owned(),
                    start_line: 0,
                    end_line: 1,
                },
            ],
            ranks: vec![32.0, 0.0],
        }
    }

    #[test]
    fn quantize_matches_the_baseline() {
        assert_eq!(
            quantize(&[1.5, -1.5, 0.5, 0.0, 0.01]),
            vec![127, -127, 64, 0, 1]
        );
    }

    #[test]
    fn similarity_search_orders_by_similarity_and_rank() {
        let results = index().similarity_search(&[127, 0], 10, true, "repo", "rev");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].file_name, "b.go");
        assert_eq!(
            results[0].score_details,
            SearchScoreDetails {
                score: 2 * 127 * 127 + 1,
                similarity_score: 2 * 127 * 127,
                rank_score: 1,
            }
        );
        assert_eq!(results[1].file_name, "a.go");
        assert_eq!(results[1].score_details.rank_score, 0);
        assert_eq!(results[0].repo_name, "repo");
        assert_eq!(results[0].revision, "rev");
    }

    #[test]
    fn similarity_search_handles_zero_results_and_dimension_mismatch() {
        assert!(index().similarity_search(&[127, 0], 0, false, "r", "v").is_empty());
        assert!(index().similarity_search(&[127], 10, false, "r", "v").is_empty());
    }

    #[test]
    fn size_and_names_match_the_baseline() {
        assert_eq!(index().estimate_size(), 4 + 2 * 32 + 2 * 4);
        assert_eq!(repo_embedding_index_name(42), "42.embeddingindex");
        assert_eq!(
            repo_embedding_index_name_deprecated("github.com/acme/repo"),
            format!(
                "github_com_acme_repo_{:x}.embeddingindex",
                md5::compute(b"github.com/acme/repo")
            )
        );
    }

    #[test]
    fn decode_requires_the_rust_magic() {
        let encoded = {
            let mut bytes = INDEX_MAGIC.to_vec();
            bytes.extend_from_slice(br#"{"repo_name":"r","embeddings_model":"m"}"#);
            bytes
        };
        let decoded = decode_index(&encoded).expect("decode");
        assert_eq!(decoded.repo_name, "r");
        assert_eq!(decoded.embeddings_model, "m");
        assert!(decode_index(b"gob-ish").is_err());
    }
}
