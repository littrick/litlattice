# tree-sitter-go

本 crate 为 [tree-sitter][] 解析库提供 Go 语法。要使用本 crate，请将其添加到
`Cargo.toml` 文件的 `[dependencies]` 段中。（注意：若要以任何有意义的方式使用
解析结果，你可能还需要依赖 [`tree-sitter`][tree-sitter crate] crate。）

```toml
[dependencies]
tree-sitter = "0.20.10"
tree-sitter-go = "0.20.0"
```

通常，你会使用 [language][language] 函数将此语法添加到 tree-sitter
[Parser][]，然后使用该解析器解析代码：

```rust
let code = r#"
    func double(x int) int {
        return x * 2
    }
"#;
let mut parser = Parser::new();
parser.set_language(tree_sitter_go::language()).expect("Error loading Go grammar");
let parsed = parser.parse(code, None);
```

如有任何问题，请通过 [tree-sitter discussions] 页面与我们联系。

[language]: https://docs.rs/tree-sitter-go/*/tree_sitter_go/fn.language.html
[Parser]: https://docs.rs/tree-sitter/*/tree_sitter/struct.Parser.html
[tree-sitter]: https://tree-sitter.github.io/
[tree-sitter crate]: https://crates.io/crates/tree-sitter
[tree-sitter discussions]: https://github.com/tree-sitter/tree-sitter/discussions
