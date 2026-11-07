#![forbid(unsafe_code)]

//! Python definition resolution, port of `lang_python.go`.

use super::languages::{join_path, path_base};
use super::{Error, FoundDef, Node, RepoCommitPath, Service};

pub(crate) fn get_def(service: &Service<'_>, node: &Node) -> Result<Option<FoundDef>, Error> {
    match node.kind() {
        "identifier" => get_def_identifier(service, node),
        _ => Ok(None),
    }
}

fn get_def_identifier(service: &Service<'_>, node: &Node) -> Result<Option<FoundDef>, Error> {
    let ident = node.content_lossy();
    let mut cur = node.clone();

    loop {
        let prev = cur.clone();
        let Some(parent) = prev.parent() else {
            return Ok(None);
        };
        cur = parent.clone();

        match parent.kind() {
            "module" => {
                if let Some(found) = find_node_in_scope(service, &parent, &ident)? {
                    return Ok(Some(FoundDef::Node(found)));
                }
                return get_def_in_imports(service, &parent, &ident);
            }
            "attribute" => {
                let Some(object) = parent.child_by_field_name("object") else {
                    return Ok(None);
                };
                let Some(attribute) = parent.child_by_field_name("attribute") else {
                    return Ok(None);
                };
                if object.idx() == prev.idx() {
                    continue;
                }
                return Ok(
                    get_field(service, &object, &attribute.content_lossy())?.map(FoundDef::Node)
                );
            }
            "for_statement" => {
                let Some(left) = parent.child_by_field_name("left") else {
                    continue;
                };
                if left.kind() == "identifier" && left.content_lossy() == ident {
                    return Ok(Some(FoundDef::Node(left)));
                }
                continue;
            }
            "with_statement" => {
                // The baseline iterates the with_statement's children twice;
                // reproduce that shape (the inner loop does not descend into
                // with_clause).
                for child in parent.named_children() {
                    if child.kind() != "with_clause" {
                        continue;
                    }
                    for inner in parent.named_children() {
                        if inner.kind() != "with_item" {
                            continue;
                        }
                        let Some(value) = inner.child_by_field_name("value") else {
                            continue;
                        };
                        if value.kind() == "identifier" && value.content_lossy() == ident {
                            return Ok(Some(FoundDef::Node(value)));
                        }
                    }
                }
                continue;
            }
            "except_clause" => {
                if parent.named_child_count() < 1 {
                    continue;
                }
                let Some(as_pattern) = parent.named_child(0) else {
                    continue;
                };
                if as_pattern.kind() != "as_pattern" {
                    continue;
                }
                let Some(target) = as_pattern.child_by_field_name("alias") else {
                    continue;
                };
                if target.kind() != "as_pattern_target" {
                    continue;
                }
                let Some(except_ident) = target.child(0) else {
                    continue;
                };
                if except_ident.kind() != "identifier" {
                    continue;
                }
                if except_ident.content_lossy() == ident {
                    return Ok(Some(FoundDef::Node(except_ident)));
                }
                continue;
            }
            "lambda" => {
                let query = r#"
                    (lambda parameters:
                        (lambda_parameters [
                            (identifier) @ident
                            (default_parameter name: (identifier) @ident)
                            (list_splat_pattern (identifier) @ident)
                            (dictionary_splat_pattern (identifier) @ident)
                        ])
                    )
                "#;
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue;
            }
            "function_definition" => {
                if let Some(name) = parent.child_by_field_name("name") {
                    if name.kind() == "identifier" && name.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(name)));
                    }
                }
                let Some(parameters) = parent.child_by_field_name("parameters") else {
                    continue;
                };
                let query = r#"
                    (parameters [
                        (identifier) @ident
                        (default_parameter name: (identifier) @ident)
                        (list_splat_pattern (identifier) @ident)
                        (dictionary_splat_pattern (identifier) @ident)

                        (typed_parameter [
                            (identifier) @ident
                            (list_splat_pattern (identifier) @ident)
                            (dictionary_splat_pattern (identifier) @ident)
                        ])
                        (typed_default_parameter name: (identifier) @ident)
                    ])
                "#;
                for capture in parameters.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }

                let Some(body) = parent.child_by_field_name("body") else {
                    continue;
                };
                if body.kind() != "block" {
                    continue;
                }
                if let Some(found) = find_node_in_scope(service, &body, &ident)? {
                    return Ok(Some(FoundDef::Node(found)));
                }
                continue;
            }
            "block" => continue,
            _ => continue,
        }
    }
}

fn find_node_in_scope(
    _service: &Service<'_>,
    block: &Node,
    ident: &str,
) -> Result<Option<Node>, Error> {
    for child in block.named_children() {
        match child.kind() {
            "function_definition" | "class_definition" => {
                if let Some(name) = child.child_by_field_name("name") {
                    if name.kind() == "identifier" && name.content_lossy() == ident {
                        return Ok(Some(name));
                    }
                }
                continue;
            }
            "expression_statement" => {
                let query = "(expression_statement (assignment left: (identifier) @ident))";
                for capture in child.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(capture));
                    }
                }
                continue;
            }
            "if_statement" => {
                let Some(consequence) = child.child_by_field_name("consequence") else {
                    return Ok(None);
                };
                if let Some(found) = find_node_in_scope(_service, &consequence, ident)? {
                    return Ok(Some(found));
                }
                let Some(alternative) = child.child_by_field_name("alternative") else {
                    continue;
                };
                let Some(body) = alternative.child_by_field_name("body") else {
                    return Ok(None);
                };
                if let Some(found) = find_node_in_scope(_service, &body, ident)? {
                    return Ok(Some(found));
                }
                continue;
            }
            "while_statement" | "for_statement" => {
                let Some(body) = child.child_by_field_name("body") else {
                    return Ok(None);
                };
                if let Some(found) = find_node_in_scope(_service, &body, ident)? {
                    return Ok(Some(found));
                }
                continue;
            }
            "try_statement" => {
                let Some(body) = child.child_by_field_name("body") else {
                    return Ok(None);
                };
                if let Some(found) = find_node_in_scope(_service, &body, ident)? {
                    return Ok(Some(found));
                }
                for try_child in child.named_children() {
                    if try_child.kind() != "except_clause" {
                        continue;
                    }
                    for except_child in try_child.named_children() {
                        if except_child.kind() == "block" {
                            if let Some(found) = find_node_in_scope(_service, &except_child, ident)?
                            {
                                return Ok(Some(found));
                            }
                        }
                    }
                }
                continue;
            }
            _ => continue,
        }
    }
    Ok(None)
}

fn get_field(service: &Service<'_>, object: &Node, field: &str) -> Result<Option<Node>, Error> {
    let Some(ty) = get_type_def(service, object)? else {
        return Ok(None);
    };
    lookup_field(service, &ty, field)
}

fn lookup_field(
    service: &Service<'_>,
    ty: &TypePython,
    field: &str,
) -> Result<Option<Node>, Error> {
    match ty {
        TypePython::Module(module) => find_node_in_scope(service, module, field),
        TypePython::Class(def) => {
            let Some(body) = def.child_by_field_name("body") else {
                return Ok(None);
            };
            for child in body.named_children() {
                match child.kind() {
                    "expression_statement" => {
                        let query = "(expression_statement (assignment left: (identifier) @ident))";
                        for capture in child.query_all(query) {
                            if capture.content_lossy() == field {
                                return Ok(Some(capture));
                            }
                        }
                    }
                    "function_definition" => {
                        let Some(name) = child.child_by_field_name("name") else {
                            continue;
                        };
                        if name.content_lossy() == field {
                            return Ok(Some(name));
                        }
                        if name.content_lossy() == "__init__" {
                            let query = r#"
                                (expression_statement
                                    (assignment
                                        left: (attribute
                                            object: (identifier) @object
                                            attribute: (identifier) @attribute
                                        )
                                    )
                                )
                            "#;
                            for captures in child.query_matches(query) {
                                let object = captures.get("object");
                                let attribute = captures.get("attribute");
                                if object.map(Node::content_lossy).as_deref() != Some("self") {
                                    continue;
                                }
                                if attribute.map(Node::content_lossy).as_deref() != Some(field) {
                                    continue;
                                }
                                return Ok(attribute.cloned());
                            }
                        }
                    }
                    "class_definition" => {
                        if let Some(name) = child.child_by_field_name("name") {
                            if name.content_lossy() == field {
                                return Ok(Some(name));
                            }
                        }
                    }
                    _ => {}
                }
            }
            for superclass in get_superclasses(def) {
                if let Some(found) = get_field(service, &superclass, field)? {
                    return Ok(Some(found));
                }
            }
            Ok(None)
        }
        TypePython::Fn { .. } => Ok(None),
    }
}

fn get_type_def(service: &Service<'_>, node: &Node) -> Result<Option<TypePython>, Error> {
    match node.kind() {
        "type" => match node.named_children().into_iter().next() {
            Some(child) => get_type_def(service, &child),
            None => Ok(None),
        },
        "identifier" => on_ident(service, node),
        "attribute" => {
            let Some(object) = node.child_by_field_name("object") else {
                return Ok(None);
            };
            let Some(attribute) = node.child_by_field_name("attribute") else {
                return Ok(None);
            };
            let Some(object_type) = get_type_def(service, &object)? else {
                return Ok(None);
            };
            let Some(found) = lookup_field(service, &object_type, &attribute.content_lossy())?
            else {
                return Ok(None);
            };
            def_to_type(service, &found)
        }
        "call" => {
            let Some(function) = node.child_by_field_name("function") else {
                return Ok(None);
            };
            let Some(ty) = get_type_def(service, &function)? else {
                return Ok(None);
            };
            match ty {
                TypePython::Fn { ret } => Ok(ret.map(|ret| *ret)),
                TypePython::Class(class) => Ok(Some(TypePython::Class(class))),
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

fn on_ident(service: &Service<'_>, node: &Node) -> Result<Option<TypePython>, Error> {
    let Some(found) = service.get_def(node)? else {
        return Ok(None);
    };
    let Some(found) = found.node() else {
        return Ok(None);
    };
    if is_recursive_definition(node, &found) {
        return Ok(None);
    }
    def_to_type(service, &found)
}

fn get_def_in_imports(
    service: &Service<'_>,
    program: &Node,
    ident: &str,
) -> Result<Option<FoundDef>, Error> {
    let find_module_or_pkg = |module_or_pkg: &Node| -> Option<Node> {
        let mut path = program.file().rc.path.clone();
        let base = path_base(&path).to_owned();
        if let Some(stripped) = path.strip_suffix(&base) {
            path = stripped.to_owned();
        }
        if let Some(stripped) = path.strip_suffix('/') {
            path = stripped.to_owned();
        }

        let dotted_name: Node;
        if module_or_pkg.kind() == "relative_import" {
            if module_or_pkg.named_child_count() < 1 {
                return None;
            }
            let import_prefix = module_or_pkg.named_child(0)?;
            if import_prefix.kind() != "import_prefix" {
                return None;
            }
            let dots = import_prefix.child_count();
            for _ in 0..dots.saturating_sub(1) {
                let base = path_base(&path).to_owned();
                if let Some(stripped) = path.strip_suffix(&base) {
                    path = stripped.to_owned();
                }
                if let Some(stripped) = path.strip_suffix('/') {
                    path = stripped.to_owned();
                }
            }
            if module_or_pkg.named_child_count() > 1 {
                dotted_name = module_or_pkg.named_child(1)?;
            } else {
                return None;
            }
        } else {
            dotted_name = module_or_pkg.clone();
        }

        if dotted_name.kind() != "dotted_name" {
            return None;
        }
        for component in dotted_name.named_children() {
            if component.kind() != "identifier" {
                return None;
            }
            let joined = join_path(&[path.as_str(), component.content_lossy().as_str()]);
            path = joined;
        }
        path.push_str(".py");
        service
            .parse(RepoCommitPath {
                repo: program.file().rc.repo.clone(),
                commit: program.file().rc.commit.clone(),
                path,
            })
            .ok()
    };

    let find_module_ident = |module: &Node, ident: &str| -> Result<Option<Node>, Error> {
        let Some(found_module) = find_module_or_pkg(module) else {
            return Ok(None);
        };
        find_node_in_scope(service, &found_module, ident)
    };

    let query = r#"[
        (import_statement) @import
        (import_from_statement) @import
    ]"#;
    for statement in program.query_all(query) {
        match statement.kind() {
            "import_statement" => {
                for import_child in statement.named_children() {
                    match import_child.kind() {
                        "dotted_name" => {
                            if import_child.named_child_count() == 0 {
                                continue;
                            }
                            let Some(last) =
                                import_child.named_child(import_child.named_child_count() - 1)
                            else {
                                continue;
                            };
                            if last.kind() != "identifier" || last.content_lossy() != ident {
                                continue;
                            }
                            return Ok(find_module_or_pkg(&import_child).map(FoundDef::Node));
                        }
                        "aliased_import" => {
                            let Some(alias) = import_child.child_by_field_name("alias") else {
                                continue;
                            };
                            if alias.kind() != "identifier" || alias.content_lossy() != ident {
                                continue;
                            }
                            let Some(name) = import_child.child_by_field_name("name") else {
                                continue;
                            };
                            return Ok(find_module_or_pkg(&name).map(FoundDef::Node));
                        }
                        _ => {}
                    }
                }
            }
            "import_from_statement" => {
                let Some(module_name) = statement.child_by_field_name("module_name") else {
                    continue;
                };

                let mut index = 0;
                while index < statement.child_count() {
                    let Some(child) = statement.child(index) else {
                        break;
                    };
                    if child.kind() == "import" {
                        index += 1;
                        break;
                    }
                    index += 1;
                }
                if index == 0 || index >= statement.child_count() {
                    continue;
                }

                if statement
                    .child(index)
                    .map(|child| child.kind() == "wildcard_import")
                    .unwrap_or(false)
                {
                    if let Some(found) = find_module_ident(&module_name, ident)? {
                        return Ok(Some(FoundDef::Node(found)));
                    }
                }

                while index < statement.child_count() {
                    let Some(child) = statement.child(index) else {
                        break;
                    };
                    index += 1;
                    if !child.is_named() {
                        continue;
                    }
                    match child.kind() {
                        "dotted_name" => {
                            if child.named_child_count() == 0 {
                                continue;
                            }
                            let Some(child_ident) = child.named_child(0) else {
                                continue;
                            };
                            if child_ident.kind() != "identifier"
                                || child_ident.content_lossy() != ident
                            {
                                continue;
                            }
                            if let Some(found) = find_module_ident(&module_name, ident)? {
                                return Ok(Some(FoundDef::Node(found)));
                            }
                        }
                        "aliased_import" => {
                            let Some(alias) = child.child_by_field_name("alias") else {
                                continue;
                            };
                            if alias.kind() != "identifier" || alias.content_lossy() != ident {
                                continue;
                            }
                            let Some(name) = child.child_by_field_name("name") else {
                                continue;
                            };
                            if name.kind() != "dotted_name" || name.named_child_count() == 0 {
                                continue;
                            }
                            let Some(name_ident) = name.named_child(0) else {
                                continue;
                            };
                            if name_ident.kind() != "identifier" {
                                continue;
                            }
                            if let Some(found) =
                                find_module_ident(&module_name, &name_ident.content_lossy())?
                            {
                                return Ok(Some(FoundDef::Node(found)));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(None)
}

fn def_to_type(service: &Service<'_>, def: &Node) -> Result<Option<TypePython>, Error> {
    if def.kind() == "module" {
        return Ok(Some(TypePython::Module(def.clone())));
    }

    let Some(parent) = def.parent() else {
        return Ok(None);
    };

    match parent.kind() {
        "parameters" => {
            if def.kind() == "identifier" && def.content_lossy() == "self" {
                let Some(function) = parent.parent() else {
                    return Ok(None);
                };
                if function.kind() != "function_definition" {
                    return Ok(None);
                }
                let Some(block) = function.parent() else {
                    return Ok(None);
                };
                if block.kind() != "block" {
                    return Ok(None);
                }
                let Some(class) = block.parent() else {
                    return Ok(None);
                };
                if class.kind() != "class_definition" {
                    return Ok(None);
                }
                let Some(name) = class.child_by_field_name("name") else {
                    return Ok(None);
                };
                return def_to_type(service, &name);
            }
            Ok(None)
        }
        "typed_parameter" => {
            let Some(ty) = parent.child_by_field_name("type") else {
                return Ok(None);
            };
            get_type_def(service, &ty)
        }
        "class_definition" => Ok(Some(TypePython::Class(parent))),
        "function_definition" => {
            let ret = match parent.child_by_field_name("return_type") {
                Some(ret_node) => get_type_def(service, &ret_node)?.map(Box::new),
                None => None,
            };
            Ok(Some(TypePython::Fn { ret }))
        }
        "assignment" => {
            let Some(ty) = parent.child_by_field_name("type") else {
                let Some(right) = parent.child_by_field_name("right") else {
                    return Ok(None);
                };
                return get_type_def(service, &right);
            };
            get_type_def(service, &ty)
        }
        _ => Ok(None),
    }
}

fn get_superclasses(definition: &Node) -> Vec<Node> {
    definition
        .child_by_field_name("superclasses")
        .map(|superclasses| superclasses.named_children())
        .unwrap_or_default()
}

fn is_recursive_definition(node: &Node, def: &Node) -> bool {
    if node.file().rc != def.file().rc {
        return false;
    }
    if def.kind() != "identifier" {
        return false;
    }
    let Some(parent) = def.parent() else {
        return false;
    };
    if parent.kind() != "assignment" {
        return false;
    }
    let mut ancestor = node.parent();
    while let Some(current) = ancestor {
        if current.idx() == parent.idx() {
            return true;
        }
        ancestor = current.parent();
    }
    false
}

enum TypePython {
    Module(Node),
    Class(Node),
    Fn { ret: Option<Box<TypePython>> },
}
