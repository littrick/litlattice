//! Vendored OSS GraphQL SDL documents.
//!
//! Byte-identical copies of `cmd/frontend/graphqlbackend/schema.graphql`,
//! `outbound_webhooks.graphql` and `viewer.graphql` from the baseline
//! (`sourcegraph-public-snapshot` @ `c864f15`). `NewSchema` joins exactly
//! these three documents (in this order) for an OSS build with nil enterprise
//! `OptionalResolver`s.

/// `//go:embed schema.graphql` — the main schema.
pub const MAIN_SCHEMA: &str = include_str!("sdl/schema.graphql");

/// `//go:embed outbound_webhooks.graphql` — outbound webhook types and
/// `extend type Query`/`Mutation` fields.
pub const OUTBOUND_WEBHOOKS_SCHEMA: &str = include_str!("sdl/outbound_webhooks.graphql");

/// `//go:embed viewer.graphql` — `extend type Query` viewer fields and the
/// `Viewer` interface.
pub const VIEWER_SCHEMA: &str = include_str!("sdl/viewer.graphql");

/// `//go:embed code_monitors.graphql` — `OptionalResolver.CodeMonitorsResolver`
/// SDL. `NewSchema` appends it before the saved-searches document when both
/// resolvers are present.
pub const CODE_MONITORS_SCHEMA: &str = include_str!("sdl/code_monitors.graphql");

/// `//go:embed saved_searches.graphql` — `OptionalResolver.SavedSearchesResolver`
/// SDL, appended directly after the code-monitors document.
pub const SAVED_SEARCHES_SCHEMA: &str = include_str!("sdl/saved_searches.graphql");

/// `//go:embed batches.graphql` — `OptionalResolver.BatchChangesResolver` SDL
/// (M9.24c). `NewSchema` appends it when the batches resolver is present.
pub const BATCHES_SCHEMA: &str = include_str!("sdl/batches.graphql");

/// `//go:embed githubapps.graphql` — `OptionalResolver.GitHubAppsResolver`
/// SDL. `batches.graphql` references `GitHubApp` (`BatchChangesCredential.
/// gitHubApp`, `CommitSigningConfiguration`), so `NewSchemaWithBatchChanges
/// Resolver` registers this document alongside it; the vendored copy is
/// byte-identical to the baseline.
pub const GITHUB_APPS_SCHEMA: &str = include_str!("sdl/githubapps.graphql");
