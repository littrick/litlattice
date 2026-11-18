# tree-sitter-javascript

本 crate 为 [tree-sitter][] 解析库提供 JavaScript 语法。要使用本 crate，请将其
添加到 `Cargo.toml` 文件的 `[dependencies]` 段中。（注意：若要以任何有意义的方式
使用解析结果，你可能还需要依赖 [`tree-sitter`][tree-sitter crate] crate。）

``` toml
[dependencies]
tree-sitter = "~0.20.10"
tree-sitter-javascript = "~0.20.1"
```

通常，你会使用 [language][language func] 函数将此语法添加到 tree-sitter
[Parser][]，然后使用该解析器解析代码。

下面的示例演示了一个简单程序：解析一个 JavaScript 函数并把结果打印到终端。

``` rust
use tree_sitter::Parser;

fn main() {
    let code = r#"
    function double(x) {
        return x * 2;
    }
"#;
    let mut parser = Parser::new();
    parser
        .set_language(tree_sitter_javascript::language())
        .expect("Error loading JavaScript grammar");
    let parsed = parser.parse(code, None);
    println!("{:#?}", parsed);
}
```

如有任何问题，请通过 [tree-sitter discussions] 页面与我们联系。

[language func]: https://docs.rs/tree-sitter-javascript/*/tree_sitter_javascript/fn.language.html
[Parser]: https://docs.rs/tree-sitter/*/tree_sitter/struct.Parser.html
[tree-sitter]: https://tree-sitter.github.io/
[tree-sitter crate]: https://crates.io/crates/tree-sitter
[tree-sitter discussions]: https://github.com/tree-sitter/tree-sitter/discussions
