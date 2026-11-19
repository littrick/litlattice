# Third-party material and provenance

This repository is licensed under the Apache License, Version 2.0 (see
[`LICENSE`](LICENSE)). This document records material that is not original to
the project, the license that applies to it, and the release blockers that must
be resolved before the repository is published.

## 1. Upstream baseline

The Go implementation being reimplemented is
`github.com/sourcegraph/sourcegraph-public-snapshot`.

| Commit | Date | License | Notes |
| --- | --- | --- | --- |
| `1cd36d2dbbd2a9ab638cc437d208d2717eaefb0b` | 2023-06-13 | Apache-2.0 (`LICENSE.apache`), except `enterprise/` and `client/web/src/enterprise/` | Last commit published under Apache-2.0 |
| `c864f15af264f0f456a6d8a83290b5c940715349` | 2024-08-22 | Sourcegraph Enterprise License (`LICENSE.enterprise`) | Repository snapshot; forbids copying, distribution and sublicensing |

Only material at or before `1cd36d2` may be redistributed under Apache-2.0.
Material that first appeared after `1cd36d2` has no Apache-licensed version.

## 2. Copied files

`third_party/provenance.tsv` is the file-level record. It was produced by
hashing every tracked file (`git hash-object`) and comparing the hashes with the
`git ls-tree -r` output of both upstream commits.

Columns: `local_path`, `upstream_path`, `upstream_commit`, `license_status`,
`apache_1cd36d2`.

### 2.1 Apache-2.0 copied files (34)

These files are byte-identical to `1cd36d2`. They are covered by this
repository's Apache-2.0 license; attribution is given in [`NOTICE`](NOTICE).

- `crates/base/sg-entry/assets/nginx/sourcegraph_backend.conf`
- `crates/base/sg-entry/assets/nginx/sourcegraph_http.conf`
- `crates/base/sg-entry/assets/nginx/sourcegraph_main.conf`
- `crates/base/sg-entry/assets/nginx/sourcegraph_server.conf`
- `crates/base/sg-entry/assets/redis-cache.conf.tmpl`
- `crates/base/sg-entry/assets/redis-store.conf.tmpl`
- `crates/domain/sg-graphql/src/schema/sdl/insights_aggregations.graphql`
- `crates/domain/sg-graphql/src/schema/sdl/outbound_webhooks.graphql`
- `crates/domain/sg-luasandbox/lua/fun.lua`
- `crates/domain/sg-luasandbox/lua/json.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/config.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/go.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/indexes.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/ruby.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/rust.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/shared.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/test.lua`
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/util.lua`
- `crates/services/sg-service-frontend/src/search_contexts.graphql`
- `crates/services/sg-service-repo-updater/src/state.html.tmpl`
- `crates/services/sg-service-symbols/tests/test_repos/java1/src/sub/Sample.java`
- `crates/services/sg-service-symbols/tests/test_repos/java1/src/sub/Sample2.java`
- `crates/services/sg-service-symbols/tests/test_repos/java1/src/sub/sub2/Sample3.java`
- `crates/services/sg-service-symbols/tests/test_repos/python/example.py`
- `crates/services/sg-service-symbols/tests/test_repos/python/example2.py`
- `crates/services/sg-service-symbols/tests/test_repos/python/sub/example3.py`
- `crates/services/sg-service-symbols/tests/test_repos/starlark/BUILD`
- `crates/services/sg-service-symbols/tests/test_repos/starlark/BUILD.bazel`
- `crates/services/sg-service-symbols/tests/test_repos/starlark/WORKSPACE`
- `crates/services/sg-service-symbols/tests/test_repos/starlark/bzl_library.bzl`
- `crates/services/sg-service-symbols/tests/test_repos/starlark/sample.bzl`
- `crates/services/sg-service-worker/src/jobs/code_monitor_email/search_results.txt.tmpl`
- `docker/server/image_tests/test_zoekt.sh`
- `docker/server/rootfs/postgres-optimize.sh`

### 2.2 Release blockers: enterprise-only material (44)

These files currently match `c864f15` and have **no Apache-licensed upstream
version**. They are **not covered by the Apache-2.0 grant** in this repository;
redistributing them would distribute Sourcegraph Enterprise-licensed material.

Each entry must be resolved before a public release:

- *added after `1cd36d2`* (17 files): rewrite from behaviour/specification, or
  drop the feature.
- *modified after `1cd36d2`* (27 files): take the `1cd36d2` version and adapt the
  Rust code, or re-derive the delta.

Added after `1cd36d2`:

- `crates/domain/sg-graphql/src/schema/sdl/search_jobs.graphql` (upstream `cmd/frontend/graphqlbackend/search_jobs.graphql`)
- `crates/domain/sg-graphql/src/schema/sdl/telemetry.graphql` (upstream `cmd/frontend/graphqlbackend/telemetry.graphql`)
- `crates/domain/sg-graphql/src/schema/sdl/viewer.graphql` (upstream `cmd/frontend/graphqlbackend/viewer.graphql`)
- `crates/net/sg-proto/proto/internalapi/v1/internalapi.proto` (upstream `internal/api/internalapi/v1/internalapi.proto`)
- `crates/net/sg-proto/proto/telemetrygateway/v1/telemetrygateway.proto` (upstream `lib/telemetrygateway/v1/telemetrygateway.proto`)
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/dotnet.lua` (upstream `internal/codeintel/autoindexing/internal/inference/lua/dotnet.lua`)
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/java.lua` (upstream `internal/codeintel/autoindexing/internal/inference/lua/java.lua`)
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/patterns.lua` (upstream `internal/codeintel/autoindexing/internal/inference/lua/patterns.lua`)
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/python.lua` (upstream `internal/codeintel/autoindexing/internal/inference/lua/python.lua`)
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/recognizer.lua` (upstream `internal/codeintel/autoindexing/internal/inference/lua/recognizer.lua`)
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/recognizers.lua` (upstream `internal/codeintel/autoindexing/internal/inference/lua/recognizers.lua`)
- `crates/services/sg-service-frontend/src/autoindexing_runtime/inference/lua/typescript.lua` (upstream `internal/codeintel/autoindexing/internal/inference/lua/typescript.lua`)
- `crates/services/sg-service-gitserver/src/sg_maintenance.sh` (upstream `cmd/gitserver/internal/sg_maintenance.sh`)
- `crates/services/sg-service-symbols/src/squirrel/language-file-extensions.json` (upstream `cmd/symbols/internal/squirrel/language-file-extensions.json`)
- `crates/services/sg-service-worker/src/jobs/code_monitor_email/search_results.html.tmpl` (upstream `internal/codemonitors/background/email_template.html.tmpl`)
- `docker/server/image_tests/test_packages.sh` (upstream `cmd/server/image_tests/test_packages.sh`)
- `docker/server/image_tests/test_services.sh` (upstream `cmd/server/image_tests/test_services.sh`)

Modified after `1cd36d2` (line counts are against the `1cd36d2` version):

- `crates/base/sg-entry/assets/nginx.conf` — upstream `cmd/server/shared/assets/nginx.conf`, modified(-1/+1)
- `crates/data/sg-migrator/assets/stitched-migration-graph.json` — upstream `internal/database/migration/shared/data/stitched-migration-graph.json`, modified(-17/+2173)
- `crates/domain/sg-graphql/src/schema/sdl/authz.graphql` — upstream `cmd/frontend/graphqlbackend/authz.graphql`, modified(-6/+18)
- `crates/domain/sg-graphql/src/schema/sdl/batches.graphql` — upstream `cmd/frontend/graphqlbackend/batches.graphql`, modified(-1/+82)
- `crates/domain/sg-graphql/src/schema/sdl/code_monitors.graphql` — upstream `cmd/frontend/graphqlbackend/code_monitors.graphql`, modified(-0/+16)
- `crates/domain/sg-graphql/src/schema/sdl/codeintel.autoindexing.graphql` — upstream `cmd/frontend/graphqlbackend/codeintel.autoindexing.graphql`, modified(-2/+20)
- `crates/domain/sg-graphql/src/schema/sdl/codeintel.codenav.graphql` — upstream `cmd/frontend/graphqlbackend/codeintel.codenav.graphql`, modified(-0/+700)
- `crates/domain/sg-graphql/src/schema/sdl/cody_context.graphql` — upstream `cmd/frontend/graphqlbackend/cody_context.graphql`, modified(-1/+302)
- `crates/domain/sg-graphql/src/schema/sdl/embeddings.graphql` — upstream `cmd/frontend/graphqlbackend/embeddings.graphql`, modified(-2/+34)
- `crates/domain/sg-graphql/src/schema/sdl/githubapps.graphql` — upstream `cmd/frontend/graphqlbackend/githubapps.graphql`, modified(-2/+48)
- `crates/domain/sg-graphql/src/schema/sdl/insights.graphql` — upstream `cmd/frontend/graphqlbackend/insights.graphql`, modified(-1/+16)
- `crates/domain/sg-graphql/src/schema/sdl/notebooks.graphql` — upstream `cmd/frontend/graphqlbackend/notebooks.graphql`, modified(-0/+4)
- `crates/domain/sg-graphql/src/schema/sdl/own.graphql` — upstream `cmd/frontend/graphqlbackend/own.graphql`, modified(-3/+14)
- `crates/domain/sg-luasandbox/lua/path.lua` — upstream `internal/luasandbox/lua/path.lua`, modified(-0/+5)
- `crates/domain/sg-settings-schema/assets/settings.schema.json` — upstream `schema/settings.schema.json`, modified(-62/+72)
- `crates/domain/sg-site-schema/assets/site.schema.json` — upstream `schema/site.schema.json`, modified(-142/+1725)
- `crates/net/sg-proto/proto/gitserver/v1/gitserver.proto` — upstream `internal/gitserver/v1/gitserver.proto`, modified(-194/+1081)
- `crates/net/sg-proto/proto/repoupdater/v1/repoupdater.proto` — upstream `internal/repoupdater/v1/repoupdater.proto`, modified(-79/+22)
- `crates/net/sg-proto/proto/searcher/v1/searcher.proto` — upstream `internal/searcher/v1/searcher.proto`, modified(-32/+68)
- `crates/net/sg-proto/proto/symbols/v1/symbols.proto` — upstream `internal/symbols/v1/symbols.proto`, modified(-36/+39)
- `crates/services/sg-service-frontend/src/codeintel.ranking.graphql` — upstream `cmd/frontend/graphqlbackend/codeintel.ranking.graphql`, modified(-0/+29)
- `docker/package/monitoring/prometheus_targets.yml` — upstream `dev/prometheus/linux/prometheus_targets.yml`, modified(-5/+14)
- `docker/server/rootfs/reindex.sh` — upstream `docker-images/postgres-12-alpine/rootfs/reindex.sh`, modified(-4/+15)
- `testdata/proto/gitserver/v1/gitserver.proto` — upstream `internal/gitserver/v1/gitserver.proto`, modified(-194/+1081)
- `testdata/proto/repoupdater/v1/repoupdater.proto` — upstream `internal/repoupdater/v1/repoupdater.proto`, modified(-79/+22)
- `testdata/proto/searcher/v1/searcher.proto` — upstream `internal/searcher/v1/searcher.proto`, modified(-32/+68)
- `testdata/proto/symbols/v1/symbols.proto` — upstream `internal/symbols/v1/symbols.proto`, modified(-36/+39)

## 3. Vendored tree-sitter grammars

`vendor/tree-sitter-*` contains parsers vendored from
`github.com/smacker/go-tree-sitter@233c2f923ac7` (MIT). Each directory contains
the upstream `LICENSE` (MIT, various copyright holders); the Cargo manifests pin
the grammar versions to the ones used by the Go baseline. The wrapper license is
in `third_party/licenses/smacker-go-tree-sitter-MIT.txt`.

## 4. Protobuf definitions from other repositories

- `crates/domain/sg-zoekt/proto/**` — `github.com/sourcegraph/zoekt`
  (Apache-2.0). License: `third_party/licenses/zoekt-APACHE-2.0.txt`.
- `crates/domain/sg-scip/proto/scip.proto` — `github.com/sourcegraph/scip`
  (Apache-2.0). License: `third_party/licenses/scip-APACHE-2.0.txt`.

The Sourcegraph-owned protobuf definitions in `crates/net/sg-proto/proto/` are
covered by section 2.

## 5. Rust dependencies

The workspace and all 506 external crates in `Cargo.lock` use permissive
licenses only: MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Zlib,
Unicode-3.0, Unlicense, 0BSD, CC0-1.0, CDLA-Permissive-2.0, BSL-1.0, or
compatible combinations of those. `deny.toml` encodes the allow-list and
`.github/workflows/licenses.yml` enforces it in CI.

## 6. Frontend policy

The packaged web UI is upstream material. Before publishing:

- Take `client/web` from `1cd36d2` only; it is Apache-2.0 except
  `client/web/src/enterprise/`.
- Do not copy from `c864f15` or any later commit.
- Do not include `enterprise/`, `client/web/src/enterprise/`, or any directory
  carrying its own `LICENSE` unless that license is compatible.
- Keep the upstream license/notice files in the distributed image.

## 7. Rust port and clean-room status

The Rust crates are functional reimplementations written after studying the Go
source. A port that follows the structure and expression of a specific upstream
file is a derivative work of that file. For an Apache-2.0 release, porting work
must reference `1cd36d2` (or an earlier Apache commit); work derived from later
commits is a blocker in the same way as section 2.2. Where a file was
independently re-derived from public behaviour or documentation only, record
that fact in the corresponding slice report.

The current porting baseline used by the project reports is `c864f15`. Until the
port is re-based on `1cd36d2` or shown to be independent, the repository as a
whole should not be published.

## 8. Release checklist

- [ ] Every `enterprise-only` row in `third_party/provenance.tsv` resolved.
- [ ] Porting baseline for each crate recorded as `<= 1cd36d2`, or a clean-room
      note added to the slice report.
- [ ] Frontend taken from `1cd36d2`, enterprise directories excluded.
- [ ] `cargo deny check licenses bans sources` passes.
- [ ] `LICENSE`, `NOTICE`, `THIRD_PARTY.md` and `third_party/` included in
      release artifacts and container images.
- [ ] Legal review completed.
