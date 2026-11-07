#![forbid(unsafe_code)]

//! Port of `github.com/sourcegraph/go-ctags` `ctagsProcess`.
//!
//! The interactive protocol is newline-delimited JSON: a `program` init reply,
//! then for each request a `tag`/`error` sequence terminated by `completed`.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde::Deserialize;

/// `ctags.d` arguments shipped with go-ctags (verbatim).
const CTAGS_ARGS: &str = include_str!("ctags_args.txt");

/// Language allowlist from go-ctags `SupportedLanguages`.
const SUPPORTED_LANGUAGES: &[&str] = &[
    "Basic",
    "C",
    "C#",
    "C++",
    "CSS",
    "CUDA",
    "Clojure",
    "Cobol",
    "D",
    "Elixir",
    "Erlang",
    "Go",
    "GraphQL",
    "Groovy",
    "Java",
    "JavaScript",
    "Jsonnet",
    "Lisp",
    "Lua",
    "Markdown",
    "MatLab",
    "OCaml",
    "ObjectiveC",
    "PHP",
    "Pascal",
    "Perl",
    "Perl6",
    "Powershell",
    "Protobuf",
    "Python",
    "R",
    "Ruby",
    "Rust",
    "Scheme",
    "Sh",
    "SystemVerilog",
    "Tcl",
    "Thrift",
    "VHDL",
    "Verilog",
    "Vim",
    "elm",
    "haskell",
    "kotlin",
    "scala",
    "swift",
    "tsx",
    "typescript",
];

/// universal-ctags line buffer size (a request longer than this is skipped).
const CTAGS_LINE_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub path: String,
    pub line: i32,
    pub kind: String,
    pub language: String,
    pub parent: String,
    pub parent_kind: String,
    pub pattern: String,
    pub signature: String,
    pub file_limited: bool,
}

#[derive(Debug)]
pub struct CtagsError(pub String);

impl std::fmt::Display for CtagsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CtagsError {}

pub struct CtagsProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl CtagsProcess {
    /// Spawn `bin` in interactive mode and consume the `program` init reply.
    pub fn spawn(bin: &str, pattern_length_limit: u32) -> Result<Self, CtagsError> {
        let mut args = vec![
            "--_interactive=default".to_owned(),
            "--fields=*".to_owned(),
            format!("--pattern-length-limit={pattern_length_limit}"),
        ];
        args.extend(CTAGS_ARGS.lines().map(str::to_owned));
        args.push(format!("--languages={}", SUPPORTED_LANGUAGES.join(",")));

        let mut child = Command::new(bin)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|err| CtagsError(format!("spawn {bin}: {err}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CtagsError("ctags stdin unavailable".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CtagsError("ctags stdout unavailable".to_owned()))?;

        let mut process = Self {
            child,
            stdin,
            stdout: BufReader::with_capacity(4096, stdout),
        };

        let init = process
            .read_reply()
            .map_err(|err| CtagsError(format!("read init reply: {err}")))?
            .ok_or_else(|| CtagsError("ctags closed stdout during init".to_owned()))?;
        if init.typ == "error" {
            process.close();
            return Err(CtagsError(format!(
                "starting {bin} failed: {}",
                init.message
            )));
        }
        Ok(process)
    }

    pub fn parse(&mut self, name: &str, content: &[u8]) -> Result<Vec<Entry>, CtagsError> {
        if std::str::from_utf8(content).is_err() {
            return Ok(Vec::new());
        }

        let filename = Path::new(name)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| name.to_owned());
        let request = serde_json::json!({
            "command": "generate-tags",
            "filename": filename,
            "size": content.len(),
        });
        let mut body = serde_json::to_vec(&request)
            .map_err(|err| CtagsError(format!("marshal ctags request: {err}")))?;
        body.push(b'\n');

        if body.len() > CTAGS_LINE_BUFFER_SIZE - 1 {
            return Ok(Vec::new());
        }

        self.stdin
            .write_all(&body)
            .and_then(|_| self.stdin.write_all(content))
            .map_err(|err| CtagsError(format!("write ctags request: {err}")))?;

        let mut entries = Vec::with_capacity(250);
        loop {
            let reply = self
                .read_reply()
                .map_err(|err| CtagsError(format!("read ctags reply: {err}")))?;
            let Some(reply) = reply else {
                return Err(CtagsError("ctags closed stdout".to_owned()));
            };

            match reply.typ.as_str() {
                "completed" => return Ok(entries),
                "error" => {
                    if reply.fatal {
                        return Err(CtagsError(format!(
                            "fatal ctags error for {name}: {}",
                            reply.message
                        )));
                    }
                }
                "tag" => {
                    let path = if reply.path == filename {
                        name.to_owned()
                    } else {
                        reply.path
                    };
                    entries.push(Entry {
                        name: reply.name,
                        path,
                        line: reply.line,
                        kind: reply.kind,
                        language: reply.language,
                        parent: reply.scope,
                        parent_kind: reply.scope_kind,
                        pattern: reply.pattern,
                        signature: reply.signature,
                        file_limited: false,
                    });
                }
                other => {
                    return Err(CtagsError(format!(
                        "ctags unexpected response {other} for {name}"
                    )))
                }
            }
        }
    }

    pub fn close(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn read_reply(&mut self) -> std::io::Result<Option<Reply>> {
        let Some(line) = self.read_line()? else {
            return Ok(None);
        };
        if line == b"(null)" {
            return Ok(Some(Reply::default()));
        }
        let reply = serde_json::from_slice(&line)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        Ok(Some(reply))
    }

    /// Skip empty lines and lines longer than the 4 KiB buffered reader,
    /// matching go-ctags' custom scanner.
    fn read_line(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        loop {
            let mut line = Vec::new();
            let mut too_long = false;
            loop {
                let available = match self.stdout.fill_buf() {
                    Ok(buffer) => buffer,
                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(err) => return Err(err),
                };
                if available.is_empty() {
                    // A partial trailing line is still a valid reply.
                    if too_long || line.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(line));
                }
                match available.iter().position(|byte| *byte == b'\n') {
                    Some(index) => {
                        if !too_long {
                            line.extend_from_slice(&available[..index]);
                        }
                        self.stdout.consume(index + 1);
                        break;
                    }
                    None => {
                        let len = available.len();
                        if !too_long {
                            line.extend_from_slice(available);
                            if line.len() > 4096 {
                                too_long = true;
                                line.clear();
                            }
                        }
                        self.stdout.consume(len);
                    }
                }
            }

            if !too_long {
                while matches!(line.last(), Some(b'\r')) {
                    line.pop();
                }
                if !line.is_empty() {
                    return Ok(Some(line));
                }
            }
        }
    }
}

impl Drop for CtagsProcess {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug, Default, Deserialize)]
struct Reply {
    #[serde(rename = "_type")]
    typ: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    fatal: bool,
    #[serde(default)]
    path: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    line: i32,
    #[serde(default)]
    kind: String,
    #[serde(default, rename = "scope")]
    scope: String,
    #[serde(default, rename = "scopeKind")]
    scope_kind: String,
    #[serde(default)]
    signature: String,
    #[serde(default)]
    pattern: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctags_binary() -> Option<String> {
        [
            std::env::var("CTAGS_COMMAND").ok(),
            Some("/usr/local/bin/universal-ctags-sg".to_owned()),
            Some("universal-ctags".to_owned()),
            Some("/usr/bin/ctags-universal".to_owned()),
        ]
        .into_iter()
        .flatten()
        .find(|candidate| {
            std::process::Command::new(candidate)
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
        })
    }

    #[test]
    fn parses_go_symbols() {
        let Some(bin) = ctags_binary() else {
            eprintln!("skipping: no universal-ctags binary");
            return;
        };
        let mut process = CtagsProcess::spawn(&bin, 250).expect("spawn ctags");
        let entries = process
            .parse(
                "example/main.go",
                b"package main\n\nfunc Hello() {}\n\ntype Thing struct{}\n",
            )
            .expect("parse");
        let hello = entries
            .iter()
            .find(|entry| entry.name == "Hello")
            .expect("Hello symbol");
        assert_eq!(hello.path, "example/main.go");
        assert_eq!(hello.language, "Go");
        assert_eq!(hello.line, 3);
    }
}
