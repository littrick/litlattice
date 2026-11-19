# Rust 版 Sourcegraph 进程与可执行文件文档

本目录描述 `dev/rs/repo` 这个 Rust 重写版本在「单容器 / `sourcegraph/server` 形态」下
的可执行文件、进程关系、通信方式与启动参数。所有内容以当前仓库代码与打包镜像
（`docker/package`）为准，基线为 `sourcegraph-public-snapshot@c864f15`。

| 文档 | 内容 |
| --- | --- |
| [01_executables_overview.md](01_executables_overview.md) | 项目内所有可执行文件总览：Rust `bins/`、镜像内置的 Go/系统组件、路径与角色 |
| [02_process_architecture.md](02_process_architecture.md) | 能跑起来所需的全部进程关系图、架构图、模块间通信方式与请求链路 |
| [03_process_reference.md](03_process_reference.md) | 每个进程的启动方式、监听地址、命令行、环境变量、依赖与注意事项 |

## 阅读顺序

1. 先看 01 了解「有哪些可执行文件、分别放在镜像哪里」；
2. 再看 02 的进程关系图与通信矩阵，建立整体架构；
3. 部署/排障时查 03 的逐进程参数与环境变量。

## 口径说明

- 「Rust 进程」指 `bins/` 下由本仓库 `cargo build` 产出的二进制；
- 「Go/外部进程」指打包镜像时从 Sourcegraph 源码或 Ubuntu 软件包安装的组件，
  它们仍然是 Go/原生二进制，不在本仓库重写范围内；
- 端口与路径默认值均取自代码常量与 `sg-entry`（`crates/base/sg-entry`）的
  Procfile 生成逻辑，实际值可能被同名环境变量覆盖。
