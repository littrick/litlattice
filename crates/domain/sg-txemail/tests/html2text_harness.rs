//! M9.8cr differential harness: the html2text fallback of
//! `txemail.ParseTemplate`.
//!
//! Mirrors `internal/txemail/txemail_html2text_harness_test.go`. Every case
//! emits one `txhtml ` JSON line:
//!
//! `{"case": ..., "input": <html>, "text": <html2text(input)>,
//!   "subject": "", "rendered_html": ""}`
//!
//! Two integration cases run the real `render` with an empty `Templates::text`
//! and report the rendered subject/text/HTML instead, so the fallback is also
//! verified end to end.
//!
//! Gated by `SG_TXEMAIL_HTML2TEXT_HARNESS_ON=1`.

use sg_txemail::html2text::html2text;
use sg_txemail::{render, Message, Templates};

fn emit(case: &str, input: &str, subject: &str, text: &str, rendered_html: &str) {
    let line = serde_json::json!({
        "case": case,
        "input": input,
        "rendered_html": rendered_html,
        "subject": subject,
        "text": text,
    })
    .to_string();
    // Go's encoding/json escapes U+2028/U+2029 even with SetEscapeHTML(false);
    // serde_json emits them raw. Escape them so the JSON text is identical.
    let line = line
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    println!("txhtml {line}");
}

fn emit_case(case: &str, input: &str) {
    emit(case, input, "", &html2text(input), "");
}

fn emit_render_case(case: &str, subject: &str, html: &str, data: serde_json::Value) {
    let message = Message {
        to: vec!["dest@example.test".to_owned()],
        reply_to: None,
        message_id: None,
        references: Vec::new(),
        template: Templates {
            subject: subject.to_owned(),
            text: String::new(),
            html: html.to_owned(),
        },
        data,
    };
    let rendered = render("noreply@example.com", "Sourcegraph", &message).expect("render");
    emit(
        case,
        html,
        &rendered.subject,
        &rendered.text,
        &rendered.html,
    );
}

#[test]
fn html2text_harness() {
    if std::env::var("SG_TXEMAIL_HTML2TEXT_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }

    // Keep this table byte-identical to the Go harness.
    let cases: &[(&str, &str)] = &[
        ("empty", ""),
        ("plain", "plain text"),
        ("div", "<div>simple text</div>"),
        ("link-double", "click <a href=\"test\">here</a>"),
        ("link-single", "click <a href='test'>here</a>"),
        ("link-attrs", "click <a class=\"x\" href=\"test\">here</a>"),
        (
            "link-entity-href",
            "click <a href=\"ents/&apos;x&apos;\">here</a>",
        ),
        (
            "link-javascript",
            "click <a href=\"javascript:void(0)\">here</a>",
        ),
        (
            "link-nested-span",
            "click <a href=\"test\"><span>here</span> or here</a>",
        ),
        (
            "link-two",
            "<a rel=\"mw:WikiLink\" href=\"/wiki/yet#English\" title=\"yet\">yet</a>, <a rel=\"mw:WikiLink\" href=\"/wiki/not_yet#English\" title=\"not yet\">not yet</a>",
        ),
        ("link-uppercase", "click <A HREF=\"test\">here</A>"),
        ("link-no-href", "click <a>here</a>"),
        ("link-empty-href", "click <a href=\"\">here</a>"),
        ("link-newline-href", "click <a\nhref=\"test\">here</a>"),
        ("inline-strong", "strong <strong>text</strong>"),
        ("spaces", "should    ignore more spaces"),
        ("newlines", "should \nignore \r\nnew lines"),
        ("br", "two<br>line<br/>breaks"),
        ("br-space", "two<br >line"),
        ("p", "<p>two</p><p>paragraphs</p>"),
        (
            "headings",
            "<h1>First</h1>main text<h2>Second</h2>next<h6>Sixth</h6>end",
        ),
        ("h7", "<h7>Not Header</h7>next section"),
        (
            "ul-li",
            "list of items<ul><li>One</li><li>Two</li><li>Three</li></ul>",
        ),
        ("li-selfclose", "two<li/>three"),
        (
            "head-title",
            "<html><head><title>Good</title></head><body>x</body>",
        ),
        (
            "script",
            "we are not <script type=\"javascript\"></script>interested in scripts",
        ),
        ("style", "x<style>.a{color:red}</style>y"),
        ("nested-bad", "<head><script>x</script>y</head>z"),
        ("entity-named", "two&nbsp;&nbsp;spaces"),
        ("entity-copy", "&copy; 2017 K3A"),
        (
            "entity-money",
            "pay in &cent;, &pound;, &yen; or &euro;?",
        ),
        ("entity-escaped-tags", "&lt;printtag&gt;"),
        ("entity-unknown", "this &neither; as you see"),
        ("entity-no-semicolon", "fish &amp chips"),
        ("entity-long-name", "&abcdefghij;"),
        ("entity-amp-attr", "x <a href=\"a&amp;b\">l</a>"),
        (
            "numeric",
            "&#8268; decimal and hex entities supported &#x204D;",
        ),
        ("numeric-controls", "a&#9;b&#10;c&#13;d&#0;e&#31;f"),
        ("numeric-invalid", "&#xD800;&#x110000;&#x10FFFF;"),
        ("numeric-plus", "&#43; plus"),
        ("unicode", "héllo — 𝔄 world"),
        ("astral-entity", "&Afr; and &Zfr;"),
        ("whitespace-unicode", "a\u{85}b\u{2028}c\u{2029}d\u{2008}e\u{200b}f"),
        ("custom-tags", "<aa x=\"1\">hello</aa><bb>bye</bb>"),
        ("tags-attrs", "<div id=\"a\" class=\"b\">div</div>"),
        ("multibyte-tag", "<ä>text</ä>"),
        ("empty-tag", "a<>b"),
        ("trailing-space", "text   "),
    ];
    for (case, input) in cases {
        emit_case(case, input);
    }

    emit_render_case(
        "render-empty-text",
        "Hi {{.Name}}",
        "<p>Hello {{.Name}}</p><p>Bye</p>",
        serde_json::json!({"Name": "A <B>"}),
    );
    emit_render_case(
        "render-empty-text-rich",
        "Report",
        "<h1>Report</h1><p>Count: {{.Name}}</p><ul><li>one</li><li>two</li></ul>",
        serde_json::json!({"Name": "42"}),
    );
}
