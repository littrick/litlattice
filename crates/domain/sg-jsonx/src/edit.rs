//! `jsonx/edit.go`: property edits that preserve the document's formatting.

use crate::format::{format_range, FormatOptions};
use crate::tree::{find_node_at_location, parse_tree, NodeType, Path, Segment};
use crate::visitor::ParseOptions;

/// `jsonx.Edit`: a replacement of `length` characters at `offset` (both in
/// `char`s) with `content`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    pub offset: i64,
    pub length: i64,
    pub content: String,
}

/// The value to insert.
///
/// `Json` is the compact JSON encoding of the value to insert (the Go side
/// receives `interface{}` and runs `json.Marshal`); `Raw` is an opaque
/// fragment that may itself contain comments and trailing commas (Go
/// `json.RawMessage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditValue {
    Json(String),
    Raw(String),
}

impl EditValue {
    fn into_string(self) -> String {
        match self {
            EditValue::Json(text) | EditValue::Raw(text) => text,
        }
    }
}

const PARSE_OPTIONS: ParseOptions = ParseOptions {
    comments: true,
    trailing_commas: true,
};

/// `jsonx.ComputePropertyEdit`.
pub fn compute_property_edit(
    text: &str,
    path: &Path,
    value: EditValue,
    options: &FormatOptions,
) -> Result<Vec<Edit>, String> {
    compute_property_edit_inner(text, path, Some(value), options)
}

/// `jsonx.ComputePropertyRemoval`.
pub fn compute_property_removal(
    text: &str,
    path: &Path,
    options: &FormatOptions,
) -> Result<Vec<Edit>, String> {
    compute_property_edit_inner(text, path, None, options)
}

fn compute_property_edit_inner(
    text: &str,
    path: &Path,
    value_obj: Option<EditValue>,
    options: &FormatOptions,
) -> Result<Vec<Edit>, String> {
    let mut value = match &value_obj {
        Some(value) => value.clone().into_string(),
        None => String::new(),
    };

    let (tree, _parse_error_codes) = parse_tree(text, PARSE_OPTIONS);

    let mut parent: Option<usize> = None;
    let mut last_segment: Option<&Segment> = None;
    let mut path_len = path.len();
    while path_len > 0 {
        path_len -= 1;
        last_segment = Some(&path[path_len]);

        parent = find_node_at_location(&tree, tree.root, &path[..path_len]);
        if parent.is_none() && value_obj.is_some() {
            match last_segment.expect("segment") {
                Segment::Property(property) => {
                    value = format!("{{{}:{}}}", go_json_string(property), value);
                }
                Segment::Index(_) => {
                    value = format!("[{}]", value);
                }
            }
        } else {
            break;
        }
    }

    let Some(parent) = parent else {
        // Empty document.
        if value_obj.is_none() {
            return Err("can't delete in empty document".to_owned());
        }
        let (offset, length) = match tree.root {
            Some(root) => (tree.node(root).offset, tree.node(root).length),
            None => (0, 0),
        };
        return format_edit(
            text,
            &Edit {
                offset,
                length,
                content: value,
            },
            options,
        );
    };

    let parent_kind = tree.node(parent).kind;

    // An object property.
    if parent_kind == NodeType::Object && matches!(last_segment, Some(Segment::Property(_))) {
        let existing = if let Some(segment) = last_segment {
            find_node_at_location(&tree, Some(parent), std::slice::from_ref(segment))
        } else {
            None
        };

        if let Some(existing) = existing {
            if value_obj.is_none() {
                // Delete.
                let property = tree.node(existing).parent.unwrap_or(parent);
                let property_index = tree
                    .node(parent)
                    .children
                    .iter()
                    .position(|&c| c == property);
                let (remove_begin, remove_end) = match property_index {
                    Some(index) if index > 0 => {
                        let previous = tree.node(parent).children[index - 1];
                        let previous = tree.node(previous);
                        let remove_begin = previous.offset + previous.length;
                        let remove_end = tree.node(property).offset + tree.node(property).length;
                        (remove_begin, remove_end)
                    }
                    Some(_) => {
                        let remove_begin = tree.node(parent).offset + 1;
                        let children = &tree.node(parent).children;
                        let remove_end = if children.len() > 1 {
                            // Remove the comma of the next node.
                            tree.node(children[1]).offset
                        } else {
                            // Remove trailing comma after this node, if any.
                            tree.node(parent).offset + tree.node(parent).length - 1
                        };
                        (remove_begin, remove_end)
                    }
                    None => {
                        let node = tree.node(existing);
                        (node.offset, node.offset + node.length)
                    }
                };
                return format_edit(
                    text,
                    &Edit {
                        offset: remove_begin,
                        length: remove_end - remove_begin,
                        content: String::new(),
                    },
                    options,
                );
            }

            // Set the value of the existing property.
            let node = tree.node(existing);
            return format_edit(
                text,
                &Edit {
                    offset: node.offset,
                    length: node.length,
                    content: value,
                },
                options,
            );
        }

        if value_obj.is_none() {
            // Property does not exist, nothing to do.
            return Ok(Vec::new());
        }

        let new_property = match last_segment {
            Some(Segment::Property(property)) => {
                format!("{}: {}", go_json_string(property), value)
            }
            _ => unreachable!(),
        };

        let index = tree.node(parent).children.len();
        let children = &tree.node(parent).children;
        let edit = if index > 0 {
            let previous = tree.node(children[index - 1]);
            Edit {
                offset: previous.offset + previous.length,
                length: 0,
                content: format!(",{}", new_property),
            }
        } else if children.is_empty() {
            Edit {
                offset: tree.node(parent).offset + 1,
                length: 0,
                content: new_property,
            }
        } else {
            Edit {
                offset: tree.node(parent).offset + 1,
                length: 0,
                content: format!("{},", new_property),
            }
        };
        return format_edit(text, &edit, options);
    }

    // An array index.
    if parent_kind == NodeType::Array && matches!(last_segment, Some(Segment::Index(_))) {
        let Segment::Index(insert_index) = *last_segment.expect("segment") else {
            unreachable!();
        };

        if insert_index == -1 {
            // Insert.
            let children = &tree.node(parent).children;
            let edit = if children.is_empty() {
                Edit {
                    offset: tree.node(parent).offset + 1,
                    length: 0,
                    content: value,
                }
            } else {
                let previous = tree.node(*children.last().expect("child"));
                Edit {
                    offset: previous.offset + previous.length,
                    length: 0,
                    content: format!(",{}", value),
                }
            };
            return format_edit(text, &edit, options);
        }

        if value_obj.is_none() {
            // Removal.
            let children = &tree.node(parent).children;
            let removal_index = usize::try_from(insert_index).unwrap_or(0);
            let to_remove = children[removal_index];
            let edit = if children.len() == 1 {
                // Only item.
                Edit {
                    offset: tree.node(parent).offset + 1,
                    length: tree.node(parent).length - 2,
                    content: String::new(),
                }
            } else if children.len() - 1 == removal_index {
                // Last item.
                let previous = tree.node(children[removal_index - 1]);
                let offset = previous.offset + previous.length;
                let parent_end_offset = tree.node(parent).offset + tree.node(parent).length;
                Edit {
                    offset,
                    length: parent_end_offset - 2 - offset,
                    content: String::new(),
                }
            } else {
                let to_remove = tree.node(to_remove);
                let next = tree.node(children[removal_index + 1]);
                Edit {
                    offset: to_remove.offset,
                    length: next.offset - to_remove.offset,
                    content: String::new(),
                }
            };
            return format_edit(text, &edit, options);
        }

        // Modify.
        let children = &tree.node(parent).children;
        let edit_index = usize::try_from(insert_index).unwrap_or(0);
        let to_edit = tree.node(children[edit_index]);
        let edit = Edit {
            offset: to_edit.offset,
            length: to_edit.length,
            content: value,
        };
        return format_edit(text, &edit, options);
    }

    let noun = if matches!(last_segment, Some(Segment::Property(_))) {
        "property"
    } else {
        "index"
    };
    Err(format!(
        "can't add {} to parent of type {}",
        noun,
        parent_kind.as_str()
    ))
}

/// `jsonx.ApplyEdits`.
pub fn apply_edits(text: &str, edits: &[Edit]) -> Result<String, String> {
    let mut chars: Vec<char> = text.chars().collect();
    let mut last_edit_offset = chars.len() as i64;
    for edit in edits.iter().rev() {
        if edit.offset < 0 || edit.length < 0 && edit.offset + edit.length > chars.len() as i64 {
            return Err(format!(
                "edit out of bounds: offset {}, length {}, doc length {}",
                edit.offset,
                edit.length,
                chars.len()
            ));
        }
        if last_edit_offset < edit.offset + edit.length {
            return Err(format!(
                "edit out of order: edit end offset {} exceeds next edit offset {}",
                edit.offset + edit.length,
                last_edit_offset
            ));
        }
        last_edit_offset = edit.offset;

        let offset = usize::try_from(edit.offset).unwrap_or(0);
        let length = usize::try_from(edit.length).unwrap_or(0);
        let mut replaced: Vec<char> = chars[..offset].to_vec();
        replaced.extend(edit.content.chars());
        if offset + length < chars.len() {
            replaced.extend_from_slice(&chars[offset + length..]);
        }
        chars = replaced;
    }
    Ok(chars.into_iter().collect())
}

/// `jsonx.FormatEdit`.
pub fn format_edit(text: &str, edit: &Edit, options: &FormatOptions) -> Result<Vec<Edit>, String> {
    let new_text = apply_edits(text, std::slice::from_ref(edit))?;

    let mut begin = edit.offset;
    let mut end = edit.offset + edit.content.chars().count() as i64;
    let edits = format_range(
        &new_text,
        usize::try_from(begin).unwrap_or(0),
        usize::try_from(end - begin).unwrap_or(0),
        options,
    );

    let mut new_text = new_text;
    for format_edit in edits.iter().rev() {
        new_text = apply_edits(&new_text, std::slice::from_ref(format_edit))?;
        if format_edit.offset < begin {
            begin = format_edit.offset;
        }
        if format_edit.offset + format_edit.length > end {
            end = format_edit.offset + format_edit.length;
        }
        end += format_edit.content.chars().count() as i64 - format_edit.length;
    }

    let edit_length = text.chars().count() as i64 - (new_text.chars().count() as i64 - end) - begin;
    let new_chars: Vec<char> = new_text.chars().collect();
    let begin_usize = usize::try_from(begin).unwrap_or(0);
    let end_usize = usize::try_from(end).unwrap_or(0).min(new_chars.len());
    let content: String = new_chars[begin_usize..end_usize].iter().collect();
    Ok(vec![Edit {
        offset: begin,
        length: edit_length,
        content,
    }])
}

/// `encoding/json` string encoding (HTML escaping included), as used by
/// `json.Marshal` on property names and values.
pub fn go_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
