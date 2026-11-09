//! M9.8co differential harness: `txemail.render` output.
//!
//! Mirrors `internal/txemail/txemail_render_harness_test.go`. Each case runs the
//! real Rust `render` and prints one canonical `txm ` JSON line. The Go
//! harness prints the same lines from the baseline's unexported `render`.
//!
//! Gated by `SG_TXEMAIL_HARNESS_ON=1`. The code-monitor template cases also
//! require `SG_TXEMAIL_TEMPLATES_DIR` to point at
//! `internal/codemonitors/background` so both implementations read the exact
//! same template files.

use serde_json::{json, Value};
use sg_txemail::{render, Message, Templates};

const CM_SUBJECT: &str = "{{ if .IsTest }}Test: {{ end }}{{.Priority}}Sourcegraph code monitor {{.Description}} detected {{.TotalCount}} new {{.ResultPluralized}}";

fn print_case(name: &str, from_name: &str, from_address: &str, message: &Message) {
    let rendered = render(from_address, from_name, message).unwrap_or_else(|error| {
        panic!("case {name}: render failed: {error}");
    });
    let text = rendered.text;
    let html = rendered.html;
    println!(
        "txm {{\"case\":{},\"from\":{},\"to\":{},\"reply_to\":{},\"headers\":{},\"subject\":{},\"text\":{},\"html\":{}}}",
        jstr(name),
        jstr(&rendered.from),
        serde_json::to_string(&rendered.to).expect("to"),
        serde_json::to_string(&rendered.reply_to).expect("reply_to"),
        serde_json::to_string(&rendered.headers).expect("headers"),
        jstr(&rendered.subject),
        jstr(&text),
        jstr(&html),
    );
}

fn jstr(value: &str) -> String {
    serde_json::to_string(value).expect("json string")
}

fn message(subject: &str, text: &str, html: &str, data: Value) -> Message {
    Message {
        to: vec!["dest@example.com".to_owned()],
        reply_to: None,
        message_id: None,
        references: Vec::new(),
        template: Templates {
            subject: subject.to_owned(),
            text: text.to_owned(),
            html: html.to_owned(),
        },
        data,
    }
}

fn plain() -> Message {
    message("{{.X}}", "text", "<p>html</p>", json!({"X": "y"}))
}

fn cm_data(test: bool, include_results: bool, display_more: bool, results: Value) -> Value {
    json!({
        "Priority": if test { "" } else { "[Critical] " },
        "CodeMonitorURL": "https://sourcegraph.example.com/code-monitoring/Q29kZU1vbml0b3I6MQ==?utm_source=code-monitoring-email",
        "SearchURL": "https://sourcegraph.example.com/search?q=type%3Acommit+after%3A2020-01-01&utm_source=code-monitoring-email",
        "Description": if test { "監視 <beta> & co" } else { "Fix <bugs> & \"features\" + more" },
        "IncludeResults": include_results,
        "TruncatedResults": results,
        "TotalCount": if test { 1 } else { 7 },
        "TruncatedCount": if display_more { 2 } else { 0 },
        "ResultPluralized": if test { "result" } else { "results" },
        "TruncatedResultPluralized": "results",
        "DisplayMoreLink": display_more,
        "IsTest": test,
    })
}

fn cm_result(result_type: &str, repo: &str, commit: &str, content: &str) -> Value {
    json!({
        "ResultType": result_type,
        "CommitURL": format!("https://sourcegraph.example.com/{repo}/-/commit/{commit}?utm_source=code-monitoring-email"),
        "RepoName": repo,
        "CommitID": commit,
        "Content": content,
    })
}

fn cm_templates(templates_dir: Option<&str>) -> Option<Templates> {
    let templates_dir = templates_dir?;
    let html = std::fs::read_to_string(format!("{templates_dir}/email_template.html.tmpl"))
        .expect("html template");
    let text = std::fs::read_to_string(format!("{templates_dir}/email_template.txt.tmpl"))
        .expect("text template");
    Some(Templates {
        subject: CM_SUBJECT.to_owned(),
        html,
        text,
    })
}

fn run_synthetic() {
    // Field pipelines, if/else and range with root references.
    print_case(
        "controls",
        "Sourcegraph",
        "noreply@example.com",
        &message(
            "S:{{if .A}}A{{else}}a{{end}}:{{.Name}}",
            "{{- range .Items }}[{{.Name}}={{.N}}]{{- end }}\nroot={{$.Name}};{{if .A}}yes{{else}}no{{end}}",
            "<ul>{{- range .Items }}<li class=\"i\">{{.Name}}: <b>{{.N}}</b></li>{{- end }}</ul><p>{{ if .A }}<a href=\"{{.URL}}\">x</a>{{ else }}none{{ end }}</p>",
            json!({
                "A": true,
                "Name": "root<&>",
                "URL": "https://e.test/s?q=a&b=c+d",
                "Items": [
                    {"Name": "a<b", "N": 1},
                    {"Name": "c&d", "N": 2},
                    {"Name": "e\"f'g+h", "N": 3},
                ],
            }),
        ),
    );

    // else-if chains in all three arms.
    for (name, a, b) in [
        ("else-if-a", true, false),
        ("else-if-b", false, true),
        ("else-if-c", false, false),
    ] {
        print_case(
            name,
            "Sourcegraph",
            "noreply@example.com",
            &message(
                "{{if .A}}A{{else if .B}}B{{else}}C{{end}}",
                "{{if .A}}A{{else if .B}}B{{else}}C{{end}}",
                "<p>{{if .A}}A{{else if .B}}B{{else}}C{{end}}</p>",
                json!({"A": a, "B": b}),
            ),
        );
    }

    // html/template URL and text escaping.
    print_case(
        "urls",
        "Sourcegraph",
        "noreply@example.com",
        &message(
            "urls",
            "{{.U1}}|{{.T}}",
            "<a href=\"{{.U1}}\">1</a><a href=\"{{.U2}}\">2</a><a href=\"{{.U3}}\">3</a><a href=\"{{.U4}}\">4</a><a href=\"{{.U5}}\">5</a><a href=\"{{.U6}}\">6</a><a href=\"{{.U7}}\">7</a><a href=\"{{.U8}}\">8</a><a href=\"{{.U9}}\">9</a><p>{{.T}}</p>",
            json!({
                "U1": "https://x.test/a?b=1&c=2",
                "U2": "javascript:alert(1)",
                "U3": "a b",
                "U4": "a+b",
                "U5": "a%zz",
                "U6": "a%2Fb",
                "U7": "a\"b",
                "U8": "<x>",
                "U9": "https://x.test/路径?q=あ",
                "T": "a<b>&c+d\"e'f\0g",
            }),
        ),
    );

    // Trim markers around control actions.
    print_case(
        "trim",
        "Sourcegraph",
        "noreply@example.com",
        &message(
            "  subj {{.V}}  ",
            "A \n\t {{- if .T -}} \n\t B {{- else -}} \n\t C {{- end -}} \n\t D",
            "X\n{{- range .Items -}}\n[{{.}}]\n{{- end -}}\nY",
            json!({"V": "<v&>", "T": true, "Items": ["a", "b<"]}),
        ),
    );

    // Printing the root value.
    print_case(
        "bare-root",
        "Sourcegraph",
        "noreply@example.com",
        &message(
            "{{.}}",
            "{{.}}|{{$}}",
            "<p>{{.}}</p>",
            json!({"K": "v", "N": 2}),
        ),
    );

    // From display-name formatting.
    for (name, from_name) in [
        ("from-empty", ""),
        ("from-plain", "Sourcegraph"),
        ("from-space", "John Doe"),
        ("from-special-ascii", "John, Doe"),
        ("from-unicode", "José"),
        ("from-unicode-special", "José, Jr."),
        ("from-long-unicode", "日本語日本語日本語日本語日本語"),
        (
            "from-long-unicode-special",
            "José, Jr. — 日本語日本語日本語日本語日本語",
        ),
        ("from-tab", "A\tB"),
    ] {
        print_case(name, from_name, "noreply@example.com", &plain());
    }

    // Address local-part quoting and missing '@'.
    for (name, from_address) in [
        ("addr-local-space", "weird local@example.com"),
        ("addr-local-dot", "first.last@example.com"),
        ("addr-no-at", "no-at"),
    ] {
        print_case(name, "", from_address, &plain());
    }

    // Header assembly.
    let mut headers = plain();
    headers.reply_to = Some("reply@example.com".to_owned());
    headers.message_id = Some("<mid@example.com>".to_owned());
    headers.references = vec!["r1@example.com".to_owned(), "r2@example.com".to_owned()];
    print_case("headers", "Sourcegraph", "noreply@example.com", &headers);
}

fn run_code_monitor(templates: &Templates) {
    let two_results = json!([
        cm_result(
            "Message",
            "github.com/acme/alpha",
            "a1b2c3d",
            "line1\nline2 <script> & \"x\" + y\nline3",
        ),
        cm_result(
            "Diff",
            "github.com/acme/beta",
            "b1b2c3d",
            "x.go x.go\n@@ -1,2 +1,2 @@ func main()\n-panic()\n+recover()\n",
        ),
    ]);

    let mut basic = Message {
        to: vec!["dest@example.com".to_owned()],
        reply_to: None,
        message_id: None,
        references: Vec::new(),
        template: templates.clone(),
        data: cm_data(false, true, true, two_results.clone()),
    };
    print_case("cm-basic", "Sourcegraph", "noreply@example.com", &basic);

    let test = Message {
        data: cm_data(
            true,
            true,
            false,
            json!([cm_result(
                "Message",
                "github.com/acme/gamma",
                "c1b2c3d",
                "tabs\there\r\nand CRLF",
            )]),
        ),
        ..basic.clone()
    };
    print_case("cm-test", "Sourcegraph", "noreply@example.com", &test);

    let test_results = Message {
        data: cm_data(true, true, true, two_results),
        ..basic.clone()
    };
    print_case(
        "cm-test-results",
        "Sourcegraph",
        "noreply@example.com",
        &test_results,
    );

    let no_results = Message {
        data: cm_data(false, false, false, json!([])),
        ..basic.clone()
    };
    print_case(
        "cm-no-results",
        "Sourcegraph",
        "noreply@example.com",
        &no_results,
    );

    basic.reply_to = Some("reply@example.com".to_owned());
    basic.message_id = Some("<mid@example.com>".to_owned());
    basic.references = vec!["cmref@example.com".to_owned()];
    print_case("cm-headers", "Sourcegraph", "noreply@example.com", &basic);
}

#[test]
fn txemail_render_harness() {
    if std::env::var("SG_TXEMAIL_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }
    run_synthetic();
    let templates_dir = std::env::var("SG_TXEMAIL_TEMPLATES_DIR").ok();
    if let Some(templates) = cm_templates(templates_dir.as_deref()) {
        run_code_monitor(&templates);
    }
}
