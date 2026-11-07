#![forbid(unsafe_code)]

//! Java definition resolution, port of `lang_java.go`.

use super::languages::{join_path, path_dir};
use super::{Error, FoundDef, Node, RepoCommitPath, Service};

pub(crate) fn get_def(service: &Service<'_>, node: &Node) -> Result<Option<FoundDef>, Error> {
    match node.kind() {
        "identifier" => get_def_identifier(service, node),
        "type_identifier" => get_def_type_identifier(service, node),
        "this" => Ok(get_enclosing_class_name(node).map(FoundDef::Node)),
        "super" => {
            let Some(class) = enclosing_class(node) else {
                return Ok(None);
            };
            let Some(superclass) = get_superclass(&class) else {
                return Ok(None);
            };
            service.get_def(&superclass)
        }
        _ => Ok(None),
    }
}

fn get_def_identifier(service: &Service<'_>, node: &Node) -> Result<Option<FoundDef>, Error> {
    let ident = node.content_lossy();
    let mut cur = node.clone();

    'outer: loop {
        let prev = cur.clone();
        let Some(parent) = prev.parent() else {
            return Ok(None);
        };
        cur = parent.clone();

        match parent.kind() {
            "program" => {
                return get_def_in_imports_or_current_package(service, &parent, &ident);
            }
            "import_declaration" => {
                let Some(program) = parent.parent() else {
                    return Ok(None);
                };
                let root = get_project_root(&program);
                let all_components = get_path(&parent);
                let components = get_path_up_to(&parent, node);
                if components.len() == all_components.len() {
                    if components.is_empty() {
                        return Ok(None);
                    }
                    let include = format!(
                        "^{}/{}",
                        join_path(&as_refs(&root)),
                        join_path(&as_refs(&components[..components.len() - 1]))
                    );
                    return Ok(service
                        .symbol_search_one(
                            &node.file().rc.repo,
                            &node.file().rc.commit,
                            vec![include],
                            &ident,
                        )?
                        .map(FoundDef::Node));
                }
                let mut dir_parts = root;
                dir_parts.extend(components);
                let dir = join_path(&as_refs(&dir_parts));
                return Ok(Some(FoundDef::Dir(RepoCommitPath {
                    repo: node.file().rc.repo.clone(),
                    commit: node.file().rc.commit.clone(),
                    path: dir,
                })));
            }
            "field_access" => {
                let Some(object) = parent.child_by_field_name("object") else {
                    continue 'outer;
                };
                if prev.idx() == object.idx() {
                    continue 'outer;
                }
                if let Some(field) = parent.child_by_field_name("field") {
                    if let Some(found) = get_field(service, &object, &field.content_lossy())? {
                        return Ok(Some(FoundDef::Node(found)));
                    }
                }
                continue 'outer;
            }
            "method_invocation" => {
                let Some(object) = parent.child_by_field_name("object") else {
                    continue 'outer;
                };
                if prev.idx() == object.idx() {
                    continue 'outer;
                }
                let Some(args) = parent.child_by_field_name("arguments") else {
                    continue 'outer;
                };
                if prev.idx() == args.idx() {
                    continue 'outer;
                }
                if let Some(name) = parent.child_by_field_name("name") {
                    if let Some(found) = get_field(service, &object, &name.content_lossy())? {
                        return Ok(Some(FoundDef::Node(found)));
                    }
                }
                continue 'outer;
            }
            "constructor_body" | "block" => {
                let mut block_child = prev.prev_named_sibling();
                while let Some(sibling) = block_child {
                    for capture in sibling.query_all(
                        "(local_variable_declaration declarator: (variable_declarator name: (identifier) @ident))",
                    ) {
                        if capture.content_lossy() == ident {
                            return Ok(Some(FoundDef::Node(capture)));
                        }
                    }
                    block_child = sibling.prev_named_sibling();
                }
                continue 'outer;
            }
            "constructor_declaration" => {
                let query = r#"[
                    (constructor_declaration parameters: (formal_parameters (formal_parameter name: (identifier) @ident)))
                    (constructor_declaration parameters: (formal_parameters (spread_parameter (variable_declarator name: (identifier) @ident))))
                ]"#;
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue 'outer;
            }
            "method_declaration" => {
                let query = r#"[
                    (method_declaration name: (identifier) @ident)
                    (method_declaration parameters: (formal_parameters (formal_parameter name: (identifier) @ident)))
                    (method_declaration parameters: (formal_parameters (spread_parameter (variable_declarator name: (identifier) @ident))))
                ]"#;
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue 'outer;
            }
            "class_declaration" => {
                if let Some(name) = parent.child_by_field_name("name") {
                    if name.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(name)));
                    }
                }
                if let Some(found) =
                    lookup_field(service, &TypeJava::Class(parent.clone()), &ident)?
                {
                    return Ok(Some(FoundDef::Node(found)));
                }
                if let Some(superclass) = get_superclass(&parent) {
                    if let Some(found) = get_field(service, &superclass, &ident)? {
                        return Ok(Some(FoundDef::Node(found)));
                    }
                }
                continue 'outer;
            }
            "lambda_expression" => {
                let query = r#"[
                    (lambda_expression parameters: (identifier) @ident)
                    (lambda_expression parameters: (formal_parameters (formal_parameter name: (identifier) @ident)))
                    (lambda_expression parameters: (formal_parameters (spread_parameter (variable_declarator name: (identifier) @ident))))
                    (lambda_expression parameters: (inferred_parameters (identifier) @ident))
                ]"#;
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue 'outer;
            }
            "catch_clause" => {
                let query = "(catch_clause (catch_formal_parameter name: (identifier) @ident))";
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue 'outer;
            }
            "for_statement" => {
                let query = "(for_statement init: (local_variable_declaration declarator: (variable_declarator name: (identifier) @ident)))";
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue 'outer;
            }
            "enhanced_for_statement" => {
                let query = "(enhanced_for_statement name: (identifier) @ident)";
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue 'outer;
            }
            "method_reference" => {
                if parent.named_child_count() == 0 {
                    return Ok(None);
                }
                let object = parent.named_child(0).expect("checked count");
                if object.idx() == prev.idx() {
                    continue 'outer;
                }
                if ident == "new" {
                    return service.get_def(&object);
                }
                return Ok(get_field(service, &object, &ident)?.map(FoundDef::Node));
            }
            _ => continue 'outer,
        }
    }
}

fn get_def_type_identifier(service: &Service<'_>, node: &Node) -> Result<Option<FoundDef>, Error> {
    let ident = node.content_lossy();
    let mut cur = node.clone();

    loop {
        let prev = cur.clone();
        let Some(parent) = prev.parent() else {
            return Ok(None);
        };
        cur = parent.clone();

        match parent.kind() {
            "program" => {
                let query = r#"[
                    (program (class_declaration name: (identifier) @ident))
                    (program (enum_declaration name: (identifier) @ident))
                    (program (interface_declaration name: (identifier) @ident))
                ]"#;
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                return get_def_in_imports_or_current_package(service, &parent, &ident);
            }
            "class_declaration" => {
                let query = r#"[
                    (class_declaration name: (identifier) @ident)
                    (class_declaration body: (class_body (class_declaration name: (identifier) @ident)))
                    (class_declaration body: (class_body (enum_declaration name: (identifier) @ident)))
                    (class_declaration body: (class_body (interface_declaration name: (identifier) @ident)))
                ]"#;
                for capture in parent.query_all(query) {
                    if capture.content_lossy() == ident {
                        return Ok(Some(FoundDef::Node(capture)));
                    }
                }
                continue;
            }
            "scoped_type_identifier" => {
                let Some(object) = parent.named_child(0) else {
                    continue;
                };
                if prev.idx() == object.idx() {
                    continue;
                }
                let Some(field) = parent.named_child(parent.named_child_count() - 1) else {
                    continue;
                };
                if let Some(found) = get_field(service, &object, &field.content_lossy())? {
                    return Ok(Some(FoundDef::Node(found)));
                }
                continue;
            }
            _ => continue,
        }
    }
}

fn get_field(service: &Service<'_>, object: &Node, field: &str) -> Result<Option<Node>, Error> {
    let Some(ty) = get_type_def(service, object)? else {
        return Ok(None);
    };
    lookup_field(service, &ty, field)
}

fn lookup_field(service: &Service<'_>, ty: &TypeJava, field: &str) -> Result<Option<Node>, Error> {
    match ty {
        TypeJava::Class(def) => {
            let Some(body) = def.child_by_field_name("body") else {
                return Ok(None);
            };
            for child in body.named_children() {
                match child.kind() {
                    "method_declaration" | "class_declaration" => {
                        let Some(name) = child.child_by_field_name("name") else {
                            continue;
                        };
                        if name.content_lossy() == field {
                            return Ok(Some(name));
                        }
                    }
                    "field_declaration" => {
                        let query = "(field_declaration declarator: (variable_declarator name: (identifier) @ident))";
                        for capture in child.query_all(query) {
                            if capture.content_lossy() == field {
                                return Ok(Some(capture));
                            }
                        }
                    }
                    _ => {}
                }
            }
            if let Some(superclass) = get_superclass(def) {
                return get_field(service, &superclass, field);
            }
            Ok(None)
        }
        TypeJava::Fn { .. } | TypeJava::Prim => Ok(None),
    }
}

fn get_type_def(service: &Service<'_>, node: &Node) -> Result<Option<TypeJava>, Error> {
    match node.kind() {
        "type_identifier" => {
            if node.content_lossy() == "var" {
                let Some(parent) = node.parent() else {
                    return Ok(None);
                };
                let query = "(local_variable_declaration declarator: (variable_declarator value: (_) @value))";
                if let Some(capture) = parent.query_all(query).into_iter().next() {
                    return get_type_def(service, &capture);
                }
                Ok(None)
            } else {
                on_ident(service, node)
            }
        }
        "this" | "super" | "identifier" => on_ident(service, node),
        "field_access" => {
            let Some(object) = node.child_by_field_name("object") else {
                return Ok(None);
            };
            let Some(field) = node.child_by_field_name("field") else {
                return Ok(None);
            };
            let Some(object_type) = get_type_def(service, &object)? else {
                return Ok(None);
            };
            let Some(found) = lookup_field(service, &object_type, &field.content_lossy())? else {
                return Ok(None);
            };
            def_to_type(service, &found)
        }
        "method_invocation" => {
            let Some(name) = node.child_by_field_name("name") else {
                return Ok(None);
            };
            let Some(ty) = get_type_def(service, &name)? else {
                return Ok(None);
            };
            match ty {
                TypeJava::Fn { ret } => Ok(ret.map(|ret| *ret)),
                _ => Ok(None),
            }
        }
        "generic_type" => {
            for child in node.named_children() {
                if child.kind() == "type_identifier" || child.kind() == "scoped_type_identifier" {
                    return get_type_def(service, &child);
                }
            }
            Ok(None)
        }
        "scoped_type_identifier" => {
            for child in node.named_children().into_iter().rev() {
                if child.kind() == "type_identifier" {
                    return get_type_def(service, &child);
                }
            }
            Ok(None)
        }
        "object_creation_expression" => {
            let Some(ty) = node.child_by_field_name("type") else {
                return Ok(None);
            };
            get_type_def(service, &ty)
        }
        "void_type" | "integral_type" | "floating_point_type" | "boolean_type" => {
            Ok(Some(TypeJava::Prim))
        }
        _ => Ok(None),
    }
}

fn on_ident(service: &Service<'_>, node: &Node) -> Result<Option<TypeJava>, Error> {
    let Some(found) = service.get_def(node)? else {
        return Ok(None);
    };
    let Some(found) = found.node() else {
        return Ok(None);
    };
    def_to_type(service, &found)
}

fn get_def_in_imports_or_current_package(
    service: &Service<'_>,
    program: &Node,
    ident: &str,
) -> Result<Option<FoundDef>, Error> {
    let root = get_project_root(program);

    let mut imports: Vec<Vec<String>> = Vec::new();
    for import_node in program.named_children() {
        if import_node.kind() != "import_declaration" {
            continue;
        }
        let mut path = get_path(&import_node);
        for child in import_node.named_children() {
            if child.kind() == "asterisk" {
                path.push("*".to_owned());
                break;
            }
        }
        if path.is_empty() {
            continue;
        }
        imports.push(path);
    }

    for import in &imports {
        let Some(last) = import.last() else {
            continue;
        };
        if last == "*" {
            continue;
        }
        if last == ident {
            let include = format!(
                "^{}/{}",
                join_path(&as_refs(&root)),
                join_path(&as_refs(&import[..import.len() - 1]))
            );
            let found = service.symbol_search_one(
                &program.file().rc.repo,
                &program.file().rc.commit,
                vec![include],
                ident,
            )?;
            return Ok(found.map(FoundDef::Node));
        }
    }

    let found = service.symbol_search_one(
        &program.file().rc.repo,
        &program.file().rc.commit,
        vec![path_dir(&program.file().rc.path)],
        ident,
    )?;
    if found.is_some() {
        return Ok(found.map(FoundDef::Node));
    }

    for import in &imports {
        if import.last().map(String::as_str) != Some("*") {
            continue;
        }
        let include = format!(
            "^{}/{}",
            join_path(&as_refs(&root)),
            join_path(&as_refs(&import[..import.len() - 1]))
        );
        let found = service.symbol_search_one(
            &program.file().rc.repo,
            &program.file().rc.commit,
            vec![include],
            ident,
        )?;
        if found.is_some() {
            return Ok(found.map(FoundDef::Node));
        }
    }

    Ok(None)
}

fn get_project_root(program: &Node) -> Vec<String> {
    let mut root: Vec<String> = path_dir(&program.file().rc.path)
        .split('/')
        .map(str::to_owned)
        .collect();
    for package in program.named_children() {
        if package.kind() != "package_declaration" {
            continue;
        }
        let pkg = get_path(&package);
        if root.len() > pkg.len() {
            let new_len = root.len() - pkg.len();
            root.truncate(new_len);
        }
    }
    root
}

fn get_path(node: &Node) -> Vec<String> {
    let mut captures = node.query_all("(identifier) @ident");
    captures.sort_by_key(|capture| capture.start_byte());
    captures
        .into_iter()
        .map(|capture| capture.content_lossy())
        .collect()
}

fn get_path_up_to(node: &Node, component: &Node) -> Vec<String> {
    let mut captures = node.query_all("(identifier) @ident");
    captures.sort_by_key(|capture| capture.start_byte());
    let mut components = Vec::new();
    for capture in captures {
        components.push(capture.content_lossy());
        if capture.idx() == component.idx() {
            break;
        }
    }
    components
}

fn get_superclass(declaration: &Node) -> Option<Node> {
    let superclass = declaration.child_by_field_name("superclass")?;
    superclass.named_child(0)
}

fn enclosing_class(node: &Node) -> Option<Node> {
    let mut cur = Some(node.clone());
    while let Some(current) = cur {
        if current.kind() == "class_declaration" || current.kind() == "interface_declaration" {
            return Some(current);
        }
        cur = current.parent();
    }
    None
}

fn get_enclosing_class_name(node: &Node) -> Option<Node> {
    let class = enclosing_class(node)?;
    class.child_by_field_name("name")
}

fn def_to_type(service: &Service<'_>, def: &Node) -> Result<Option<TypeJava>, Error> {
    let Some(parent) = def.parent() else {
        return Ok(None);
    };
    match parent.kind() {
        "class_declaration" => Ok(Some(TypeJava::Class(parent))),
        "method_declaration" => {
            let ret = match parent.child_by_field_name("type") {
                Some(ret_node) => get_type_def(service, &ret_node)?.map(Box::new),
                None => None,
            };
            Ok(Some(TypeJava::Fn { ret }))
        }
        "formal_parameter" | "enhanced_for_statement" => {
            let Some(ty) = parent.child_by_field_name("type") else {
                return Ok(None);
            };
            get_type_def(service, &ty)
        }
        "variable_declarator" => {
            let Some(grandparent) = parent.parent() else {
                return Ok(None);
            };
            let Some(ty) = grandparent.child_by_field_name("type") else {
                return Ok(None);
            };
            get_type_def(service, &ty)
        }
        _ => Ok(None),
    }
}

enum TypeJava {
    Class(Node),
    Fn { ret: Option<Box<TypeJava>> },
    Prim,
}

fn as_refs(parts: &[String]) -> Vec<&str> {
    parts.iter().map(String::as_str).collect()
}
