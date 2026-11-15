#![forbid(unsafe_code)]

//! `internal/ipynb.Render` — the `.ipynb` branch of `richHTML`
//! (`cmd/frontend/graphqlbackend/file.go:34-43`).
//!
//! The Go baseline converts a notebook with
//! `bevzzz/nb` + `goldmark-jupyter` + `nb-synth` + the ANSI adapter and then
//! sanitizes the result with `htmlutil.SanitizeReader`. This module mirrors
//! that stack for the cell/output shapes below, reusing the existing ports:
//!
//! - markdown cells: [`crate::markdown_graphql::render_markdown`] (the
//!   byte-compatible goldmark + bluemonday pipeline; nb uses the same
//!   `markdown.Goldmark()` instance);
//! - code cells: [`crate::chroma`] with no line numbers, which already emits
//!   the sanitized `<pre class="chroma">` form;
//! - stream outputs: ansihtml's plain-text escaping (`<pre>…</pre>`).
//!
//! The nb HTML wrapper tags/classes are reproduced verbatim; every fixed
//! fragment below was checked against the Go oracle's post-sanitize output.
//!
//! ## Coverage / errata
//!
//! - `raw` cells render the wrapper with no content, exactly like nb (no raw
//!   render function is registered by the Go configuration).
//! - Code-cell `display_data` / `execute_result` / `error` outputs are not
//!   rendered; the function returns an error instead of inventing HTML. Only
//!   `stream` outputs (and no outputs) are byte-compatible.
//! - Code-cell languages outside the five ported chroma lexers fall back to
//!   raw `<pre>` text (nb's own fallback when its lexer lookup is nil); the Go
//!   baseline would highlight other known languages.
//! - Notebooks other than nbformat v4 are rejected. Decode error strings are
//!   the port's own (serde), not Go's `encoding/json` text; the *semantics*
//!   (invalid JSON / unsupported version / unknown cell type) match.

use serde_json::Value;

use crate::chroma;

/// `ipynb.Render`: decode, render, sanitize. The markdown and chroma pieces
/// are already sanitized, so no extra pass is needed.
pub fn render(content: &str) -> Result<String, String> {
    let notebook: Value = serde_json::from_str(content)
        .map_err(|error| format!("ipynb.Render: decode: {error}"))?;

    let major = notebook.get("nbformat").and_then(Value::as_u64).unwrap_or(0);
    let minor = notebook
        .get("nbformat_minor")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if major != 4 {
        return Err(format!(
            "ipynb.Render: decode: schema v{major}.{minor} is not supported"
        ));
    }

    let language = notebook
        .get("metadata")
        .and_then(|metadata| metadata.get("language_info"))
        .and_then(|info| info.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    let cells = notebook
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| "ipynb.Render: decode: v4.0: missing cells".to_owned())?;

    let mut out = String::new();
    for cell in cells {
        match cell.get("cell_type").and_then(Value::as_str) {
            Some("markdown") => {
                let source = multiline(cell.get("source"));
                out.push_str(&markdown_cell(&source)?);
            }
            Some("code") => out.push_str(&code_cell(cell, &language)?),
            Some("raw") => {
                let source = multiline(cell.get("source"));
                out.push_str(&raw_cell(&source));
            }
            Some(other) => {
                return Err(format!(
                    "ipynb.Render: decode: v4.0: cell content: unknown cell type {other:?}"
                ));
            }
            None => {
                return Err("ipynb.Render: decode: v4.0: cell content: missing cell_type".to_owned())
            }
        }
    }
    Ok(out)
}

/// nb `common.MultilineString.Text`: a string or an array of strings,
/// concatenated.
fn multiline(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(lines)) => lines
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .concat(),
        _ => String::new(),
    }
}

fn markdown_cell(source: &str) -> Result<String, String> {
    let body = crate::markdown_graphql::render_markdown(source)?;
    Ok(format!(
        "<div class=\"jp-Cell jp-MarkdownCell jp-Notebook-cell\">\n\
<div class=\"jp-Cell-inputWrapper\">\n\
<div class=\"jp-Collapser jp-InputCollapser jp-Cell-inputCollapser\">\n \
</div>\n\
<div class=\"jp-InputArea jp-Cell-inputArea\">\n\
<div class=\"jp-InputPrompt jp-InputArea-prompt\"></div>\n\
<div class=\"jp-RenderedMarkdown jp-MarkdownOutput jp-RenderedHTMLCommon\">\n\
{body}</div>\n\
</div>\n\
</div>\n\
</div>\n"
    ))
}

fn raw_cell(source: &str) -> String {
    // nb registers no raw-cell renderer, but a raw cell's mime type is
    // `text/plain`, so the `text/*` `renderRaw` preference matches and writes
    // `<pre>escaped</pre>` into the input area.
    format!(
        "<div class=\"jp-Cell jp-RawCell jp-Notebook-cell\">\n\
<div class=\"jp-Cell-inputWrapper\">\n\
<div class=\"jp-Collapser jp-InputCollapser jp-Cell-inputCollapser\">\n \
</div>\n\
<div class=\"jp-InputArea jp-Cell-inputArea\">\n\
<div class=\"jp-InputPrompt jp-InputArea-prompt\"></div>\n\
<pre>{}</pre></div>\n\
</div>\n\
</div>\n",
        escape_html(source)
    )
}

fn code_cell(cell: &Value, language: &str) -> Result<String, String> {
    let source = multiline(cell.get("source"));
    let execution_count = cell
        .get("execution_count")
        .and_then(Value::as_i64)
        .unwrap_or(0);

    // nb: empty language -> `lexers.MatchMimeType("application/x-python")` =>
    // Python. Use the same default; an unknown non-empty language falls back
    // to nb's raw `<pre>` rendering (see the module errata).
    let lexer = if language.is_empty() { "python" } else { language };
    let code_html = chroma::highlight(lexer, chroma::LineNumbers::Off, 1, &source)
        .unwrap_or_else(|| format!("<pre>{}</pre>", escape_html(&source)));

    let outputs = cell
        .get("outputs")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let outputs_html = outputs_section(outputs)?;

    Ok(format!(
        "<div class=\"jp-Cell jp-CodeCell jp-Notebook-cell\">\n\
<div class=\"jp-Cell-inputWrapper\">\n\
<div class=\"jp-Collapser jp-InputCollapser jp-Cell-inputCollapser\">\n \
</div>\n\
<div class=\"jp-InputArea jp-Cell-inputArea\">\n\
<div class=\"jp-InputPrompt jp-InputArea-prompt\">In\u{a0}[{execution_count}]:</div>\n\
<div class=\"jp-CodeMirrorEditor jp-Editor jp-InputArea-editor\">\n\
<div>\n\
<div>\n\
{code_html}</div>\n\
</div>\n\
</div>\n\
</div>\n\
</div>\n\
{outputs_html}</div>\n"
    ))
}

fn outputs_section(outputs: &[Value]) -> Result<String, String> {
    if outputs.is_empty() {
        return Ok("<div class=\"jp-Cell-outputWrapper\">\n\
<div class=\"jp-Collapser jp-OutputCollapser jp-Cell-outputCollapser\"></div>\n\
<div class=\"jp-OutputArea jp-Cell-outputArea\">\n\
<div class=\"jp-OutputPrompt jp-OutputArea-prompt\"></div>\n\
<div>\n\
</div>\n\
</div>\n\
</div>\n"
            .to_owned());
    }

    // Only stream outputs are ported. nb picks the wrapper classes from the
    // first output: a stream is `text/plain`, which yields child +
    // `jp-OutputArea-executeResult` and `jp-RenderedText`.
    let mut rendered = String::new();
    for output in outputs {
        match output.get("output_type").and_then(Value::as_str) {
            Some("stream") => {
                let text = multiline(output.get("text"));
                rendered.push_str(&format!("<pre>{}</pre>", escape_html(&text)));
            }
            Some(other) => {
                return Err(format!(
                    "ipynb.Render: rendering {other:?} outputs is not ported"
                ))
            }
            None => return Err("ipynb.Render: output missing output_type".to_owned()),
        }
    }

    Ok(format!(
        "<div class=\"jp-Cell-outputWrapper\">\n\
<div class=\"jp-Collapser jp-OutputCollapser jp-Cell-outputCollapser\"></div>\n\
<div class=\"jp-OutputArea jp-Cell-outputArea\">\n\
<div class=\"jp-OutputArea-child jp-OutputArea-executeResult\">\n\
<div class=\"jp-OutputPrompt jp-OutputArea-prompt\"></div>\n\
<div class=\"jp-RenderedText jp-OutputArea-output \">\n\
{rendered}</div>\n\
</div>\n\
</div>\n\
</div>\n"
    ))
}

/// Go `html.EscapeString`.
fn escape_html(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            _ => out.push(character),
        }
    }
    out
}
