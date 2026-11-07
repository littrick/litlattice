#![forbid(unsafe_code)]

//! Starlark definition resolution, port of `lang_starlark.go`.

use super::{Error, FoundDef, Node, RepoCommitPath, Service};

pub(crate) fn get_def(service: &Service<'_>, node: &Node) -> Result<Option<FoundDef>, Error> {
    match node.kind() {
        "identifier" => Ok(binding_named(&node.content_lossy(), &node.root()).map(FoundDef::Node)),
        "string" => get_def_string(service, node),
        _ => Ok(None),
    }
}

fn get_def_string(service: &Service<'_>, node: &Node) -> Result<Option<FoundDef>, Error> {
    // The baseline inspects only the first query match.
    let Some(captures) = node.root().query_captures(LOAD_QUERY).into_iter().next() else {
        return Ok(None);
    };
    if captures.len() < 3 {
        return Err(Error::Message(format!(
            "expected 3 captures in starlark query, got {}",
            captures.len()
        )));
    }
    let path = string_contents(&captures[1].1);
    let symbol = string_contents(&captures[2].1);
    // The baseline compares node ranges (`nodeId`), not node identity.
    if captures[2].1.to_range() != node.to_range() {
        return Ok(None);
    }

    let path_components: Vec<&str> = path.split(':').collect();
    if path_components.len() != 2 {
        return Ok(None);
    }
    let (directory, filename) = (path_components[0], path_components[1]);

    if !directory.starts_with("//") {
        return Err(Error::Message(format!(
            "expected starlark directory to be prefixed with \"//\", got {directory:?}"
        )));
    }

    let destination = RepoCommitPath {
        path: super::languages::join_path(&[
            directory.strip_prefix("//").unwrap_or(directory),
            filename,
        ]),
        repo: node.file().rc.repo.clone(),
        commit: node.file().rc.commit.clone(),
    };
    let destination_root = service.parse(destination)?;
    Ok(binding_named(&symbol, &destination_root).map(FoundDef::Node))
}

fn binding_named(name: &str, node: &Node) -> Option<Node> {
    node.root()
        .query_captures(STARLARK_EXPORT_QUERY)
        .concat()
        .into_iter()
        .map(|(_, capture)| capture)
        .find(|capture| capture.content_lossy() == name)
}

fn string_contents(node: &Node) -> String {
    let contents = node.content_lossy();
    let contents = contents.strip_prefix('"').unwrap_or(&contents);
    contents.strip_suffix('"').unwrap_or(contents).to_owned()
}

const STARLARK_EXPORT_QUERY: &str = r#"
;;; declaration
(module (function_definition name: (identifier) @name))
(module (expression_statement (assignment left: (identifier) @name)))
;;; load_statement
(
	(module
		(expression_statement
			(call
				function: (identifier) @_funcname
				arguments: (argument_list
                  (string) @path
                  (keyword_argument name: (identifier) @named) @symbol
                )
			)
		)
	)
	(#eq? @_funcname "load")
)
"#;

const LOAD_QUERY: &str = r#"
(
	(module
		(expression_statement
			(call
				function: (identifier) @_funcname
				arguments: (argument_list (string) @path (string) @symbol)
			)
		)
	)
	(#eq? @_funcname "load")
)
"#;
