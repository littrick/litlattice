#![forbid(unsafe_code)]

//! Port of `cmd/symbols/internal/squirrel/hover.go`.

use super::Node;

/// Return the markdown hover message for the node, if any.
pub(crate) fn find_hover(node: &Node) -> String {
    let style = &node.file().lang.comment_style;
    let contents = String::from_utf8_lossy(&node.file().contents);
    let lines: Vec<&str> = contents.split('\n').collect();
    let row = node.start_point().0 as usize;

    let mut hover = String::new();
    hover.push_str("```");
    hover.push_str(style.code_fence_name);
    hover.push('\n');
    hover.push_str(lines.get(row).copied().unwrap_or(""));
    hover.push('\n');
    hover.push_str("```");

    let mut cur = Some(node.clone());
    while let Some(current) = cur {
        if current.start_point().0 != node.start_point().0 {
            break;
        }

        let mut prev = current.prev_named_sibling();
        while let Some(candidate) = prev.clone() {
            if !style.skip_node_types.contains(&candidate.kind()) {
                break;
            }
            prev = candidate.prev_named_sibling();
        }

        let mut comments: Vec<String> = Vec::new();
        let mut last_start_row: Option<u32> = None;
        while let Some(candidate) = prev.clone() {
            if !style.node_types.contains(&candidate.kind()) {
                break;
            }
            let candidate_start = candidate.start_point().0;
            let candidate_end = {
                let data = &candidate.file().nodes[candidate.idx()];
                data.end_row
            };
            match last_start_row {
                None => last_start_row = Some(candidate_start),
                Some(last) if last != candidate_end + 1 => break,
                Some(_) => last_start_row = Some(candidate_start),
            }

            let comment = candidate.content_lossy();
            let mut cleaned: Vec<String> = Vec::new();
            for line in comment.split('\n') {
                if let Some(ignore) = &style.ignore_regex {
                    if ignore.is_match(line) {
                        continue;
                    }
                }
                let line = match &style.strip_regex {
                    Some(strip) => strip.replace_all(line, "").into_owned(),
                    None => line.to_owned(),
                };
                cleaned.push(line);
            }

            // The baseline measures the minimum indentation but then trims all
            // leading spaces from every line.
            for line in &mut cleaned {
                *line = line.trim_start_matches(' ').to_owned();
            }

            comments.push(cleaned.join("\n"));
            prev = candidate.prev_named_sibling();
        }

        if comments.is_empty() {
            cur = current.parent();
            continue;
        }

        comments.reverse();
        hover.push_str("\n\n---\n\n");
        hover.push_str(&comments.join("\n"));
        hover.push('\n');

        cur = current.parent();
    }

    hover
}
