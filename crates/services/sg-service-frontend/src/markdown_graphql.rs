#![forbid(unsafe_code)]

//! `Query.renderMarkdown` / `Query.highlightCode` renderer seam
//! (`cmd/frontend/graphqlbackend/{markdown,highlight}.go`).
//!
//! The baseline delegates to the Go markdown (goldmark + Sourcegraph
//! extensions) and syntax-highlighting (syntect/Sourcegraph highlight)
//! libraries. Byte-identical output is the contract, so this slice defines the
//! injectable renderer seam and leaves both fields unported until a renderer
//! implementation is available. [`MarkdownRenderer::render`] is never called by
//! the current resolver.

/// `markdown.Renderer`/`highlight.Code`.
pub trait MarkdownRenderer: Send + Sync {
    fn render_markdown(&self, markdown: &str, options: MarkdownOptions) -> String;
    fn highlight_code(&self, code: &str, language: &str) -> String;
}

/// `MarkdownOptions` from the GraphQL arguments.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MarkdownOptions {
    pub gfm: bool,
    pub breaks: bool,
    pub render_raw_html: bool,
}

/// No renderer is wired in this slice; a `None` renderer keeps both root
/// fields unported.
pub fn renderer() -> Option<&'static dyn MarkdownRenderer> {
    None
}
