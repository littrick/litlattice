//! Unit tests for `sg-jsonx` (the differential harness lives in
//! `jsonx_diff.rs` and is gated by `SG_M9FR_DIFF=1`).

use sg_jsonx::{
    apply_edits, compute_property_edit, compute_property_removal, find_node_at_location,
    go_json_string, parse_tree, property_path, settings_format_options, Edit, EditValue, Literal,
    NodeType, ParseOptions, Segment,
};

const PARSE: ParseOptions = ParseOptions {
    comments: true,
    trailing_commas: true,
};

#[test]
fn parse_tree_finds_nested_property() {
    let (tree, errors) = parse_tree(
        r#"{"a": {"b": 1, // comment
    "c": [true, null]}}"#,
        PARSE,
    );
    assert!(errors.is_empty());
    assert_eq!(tree.node(tree.root.unwrap()).kind, NodeType::Object);
    let path = vec![
        Segment::Property("a".to_owned()),
        Segment::Property("c".to_owned()),
        Segment::Index(1),
    ];
    let node = find_node_at_location(&tree, tree.root, &path).expect("nested node");
    assert_eq!(tree.node(node).kind, NodeType::Null);
}

#[test]
fn edit_sets_existing_property() {
    let text = "{\n  \"a\": 1\n}\n";
    let edits = compute_property_edit(
        text,
        &property_path(&["a"]),
        EditValue::Json("2".to_owned()),
        &settings_format_options(),
    )
    .expect("edit");
    let output = apply_edits(text, &edits).expect("apply");
    assert_eq!(output, "{\n  \"a\": 2\n}\n");
}

#[test]
fn removal_writes_through_format_edit() {
    let text = "{\"a\":1,\"b\":2}";
    let edits = compute_property_removal(text, &property_path(&["b"]), &settings_format_options())
        .expect("remove");
    let output = apply_edits(text, &edits).expect("apply");
    assert_eq!(output, "{\n  \"a\": 1\n}");
}

#[test]
fn removal_missing_property_is_noop() {
    let edits = compute_property_removal(
        "{\"a\": 1}",
        &property_path(&["z"]),
        &settings_format_options(),
    )
    .expect("remove");
    assert!(edits.is_empty());
}

#[test]
fn apply_edits_rejects_out_of_order() {
    // Edits must be sorted ascending; Go's ApplyEdits iterates in reverse and
    // reports the first out-of-order end offset.
    let edits = vec![
        Edit {
            offset: 5,
            length: 0,
            content: "y".to_owned(),
        },
        Edit {
            offset: 2,
            length: 1,
            content: "x".to_owned(),
        },
    ];
    let err = apply_edits("{\"a\": 1}", &edits).expect_err("out of order");
    assert!(err.starts_with("edit out of order"), "{err}");
}

#[test]
fn property_names_use_go_json_escaping() {
    assert_eq!(go_json_string("a\"b<c>"), "\"a\\\"b\\u003cc\\u003e\"");
    assert_eq!(go_json_string("line\n"), "\"line\\n\"");
}

#[test]
fn literal_values_are_preserved_verbatim() {
    let (tree, _) = parse_tree("[1.50, 1e3]", PARSE);
    let root = tree.root.unwrap();
    let first = tree.node(root).children[0];
    match &tree.node(first).value {
        Some(Literal::Number(number)) => assert_eq!(number, "1.50"),
        other => panic!("unexpected value: {other:?}"),
    }
}
