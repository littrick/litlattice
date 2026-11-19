# Sourcegraph Rust Rewrite

A Rust reimplementation of the Sourcegraph server main process. The goal is a
drop-in `sourcegraph/server` image: the Go processes are replaced by Rust
services, while the web UI and the database are reused.

## Layout

- `crates/` — shared libraries and service implementations
- `bins/` — service binaries
- `docker/` — packaging scripts, rootfs assets and image tests
- `docs/` — architecture and process documentation
- `vendor/` — vendored tree-sitter grammars (MIT)

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Third-party material, file-level provenance and the remaining release blockers
are documented in [THIRD_PARTY.md](THIRD_PARTY.md). In particular, files marked
`enterprise-only` in `third_party/provenance.tsv` are not covered by this
repository's license and must not be redistributed.

This project is not affiliated with or endorsed by Sourcegraph, Inc.
