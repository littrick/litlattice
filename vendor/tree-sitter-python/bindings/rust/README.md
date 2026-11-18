# tree-sitter-python

本 crate 为 [tree-sitter][] 解析库提供 Python 语法。要使用本 crate，请将其添加到
`Cargo.toml` 文件的 `[dependencies]` 段中。（注意：若要以任何有意义的方式使用
解析结果，你可能还需要依赖 [`tree-sitter`][tree-sitter crate] crate。）

``` toml
[dependencies]
tree-sitter = "0.17"
tree-sitter-python = "0.17"
```

通常，你会使用 [language][language func] 函数将此语法添加到 tree-sitter
[Parser][]，然后使用该解析器解析代码：

``` rust
let code = r#"
    def double(x):
        return x * 2
"#;
let mut parser = Parser::new();
parser.set_language(tree_sitter_python::language()).expect("Error loading Python grammar");
let parsed = parser.parse(code, None);
```

如有任何问题，请通过 [tree-sitter discussions] 页面与我们联系。

[Language]: https://docs.rs/tree-sitter/*/tree_sitter/struct.Language.html
[language func]: https://docs.rs/tree-sitter-python/*/tree_sitter_python/fn.language.html
[Parser]: https://docs.rs/tree-sitter/*/tree_sitter/struct.Parser.html
[tree-sitter]: https://tree-sitter.github.io/
[tree-sitter crate]: https://crates.io/crates/tree-sitter
[tree-sitter discussions]: https://github.com/tree-sitter/tree-sitter/discussions
