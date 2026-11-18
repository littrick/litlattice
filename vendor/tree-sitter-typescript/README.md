tree-sitter-typescript
===========================

[![Build Status](https://github.com/tree-sitter/tree-sitter-typescript/workflows/build/badge.svg)](https://github.com/tree-sitter/tree-sitter-typescript/actions?query=workflow%3Abuild)
[![Build status](https://ci.appveyor.com/api/projects/status/rn11gs5y3tm7tuy0/branch/master?svg=true)](https://ci.appveyor.com/project/maxbrunsfeld/tree-sitter-typescript/branch/master)

用于 [tree-sitter][] 的 TypeScript 和 TSX 语法。

由于 TSX 和 TypeScript 实际上是两种不同的方言，本模块定义了两个语法。请按如下方式引入：

```js
require('tree-sitter-typescript').typescript; // TypeScript 语法
require('tree-sitter-typescript').tsx; // TSX 语法
```

对于带 [flow] 类型注解的 JavaScript 文件，可以使用 `tsx` 解析器。

[tree-sitter]: https://github.com/tree-sitter/tree-sitter
[flow]: https://flow.org/en/

参考资料

* [TypeScript 语言规范](https://github.com/microsoft/TypeScript/blob/main/doc/spec-ARCHIVED.md)
