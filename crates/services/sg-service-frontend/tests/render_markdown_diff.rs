//! M9.23c differential: `Query.renderMarkdown`.
//!
//! Reads the corpus (`M9_23C_CORPUS`), renders every case with the production
//! renderer, and writes `<out>/<name>.html` for byte comparison with the Go
//! goldmark + bluemonday oracle (`internal/markdown.Render`).

use std::path::Path;

use serde::Deserialize;

use sg_service_frontend::markdown_graphql::render_markdown;

#[derive(Deserialize)]
struct Case {
    name: String,
    content: String,
}

#[test]
fn render_markdown_diff() {
    let corpus = match std::env::var("M9_23C_CORPUS") {
        Ok(path) => path,
        Err(_) => return,
    };
    let out = std::env::var("M9_23C_OUT").expect("M9_23C_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    for case in &cases {
        let html = render_markdown(&case.content).expect("render");
        std::fs::write(Path::new(&out).join(format!("{}.html", case.name)), html)
            .expect("write output");
    }
    println!("RUST {} cases", cases.len());
}
