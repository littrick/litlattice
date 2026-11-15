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

/// `//go:embed codeintel*.graphql` — the Code Intel auto-indexing document
/// (`Query.preciseIndexes`, `PreciseIndex`, `IndexSteps`, ...). Appended only
/// when the code-intel resolver is explicitly enabled (M9.27d); `assemble_oss`
/// never includes it.
pub const CODEINTEL_AUTOINDEXING_SCHEMA: &str =
    include_str!("sdl/codeintel.autoindexing.graphql");

/// `//go:embed codeintel*.graphql` — the policies document. The auto-indexing
/// document references `CodeIntelligenceConfigurationPolicy`, so the two are
/// registered together, exactly like `NewSchema`'s `ReadDir` batch.
pub const CODEINTEL_POLICIES_SCHEMA: &str = include_str!("sdl/codeintel.policies.graphql");

/// `//go:embed codeintel*.graphql` — the code-navigation document (M9.48b):
/// `GitBlob.lsif`, `GitBlobLSIFData`, `GitTreeLSIFData`, `CodeGraphData`,
/// `Query.usagesForSymbol`, diagnostics and the input types. `assemble_oss`
/// never includes it; production appends it through
/// `with_codeintel_codenav` only when that surface is enabled.
pub const CODEINTEL_CODENAV_SCHEMA: &str = include_str!("sdl/codeintel.codenav.graphql");

/// `//go:embed notebooks.graphql` — `OptionalResolver.NotebooksResolver` SDL
/// (M9.29a): `Query.notebooks`, the `Notebook`/`NotebookBlock` types and the
/// notebook mutations. `NewSchema` appends it only when the notebooks
/// resolver is present, so `assemble_oss` never includes it.
pub const NOTEBOOKS_SCHEMA: &str = include_str!("sdl/notebooks.graphql");

/// `//go:embed insights.graphql` — `OptionalResolver.InsightsResolver` SDL
/// (`Query.insightViews`, `Query.insightsDashboards`, `InsightView`, ...).
/// Appended only when the insights resolver is explicitly enabled (M9.29b);
/// `assemble_oss` never includes it.
pub const INSIGHTS_SCHEMA: &str = include_str!("sdl/insights.graphql");

/// `//go:embed insights_aggregations.graphql` —
/// `OptionalResolver.InsightsAggregationResolver` SDL (M9.34a):
/// `Query.searchQueryAggregate`, `SearchQueryAggregate`, the aggregation
/// result union and its enums. `NewSchema` appends it only when the
/// aggregation resolver is present, so `assemble_oss` never includes it.
pub const INSIGHTS_AGGREGATIONS_SCHEMA: &str =
    include_str!("sdl/insights_aggregations.graphql");

/// `//go:embed cody_context.graphql` — `OptionalResolver.CodyContextResolver`
/// SDL (M9.48d): `Query.getCodyContext*`/`chatIntent`/`rankContext`/
/// `recordContext`/`chatContext`/`urlMentionContext`, the `CodyContextResult`
/// union and its result objects. `NewSchema` appends it only when the context
/// resolver is present, so `assemble_oss` never includes it.
pub const CODY_CONTEXT_SCHEMA: &str = include_str!("sdl/cody_context.graphql");

/// `//go:embed embeddings.graphql` — `OptionalResolver.EmbeddingsResolver` SDL
/// (M9.48d): `Query.embeddingsSearch`/`embeddingsMultiSearch`/
/// `isContextRequiredForChatQuery`/`repoEmbeddingJobs`, the scheduling
/// mutations, `Repository.embeddingJobs` and the `RepoEmbeddingJob` types.
/// Appended only when the embeddings resolver is present; `assemble_oss` never
/// includes it.
pub const EMBEDDINGS_SCHEMA: &str = include_str!("sdl/embeddings.graphql");

/// `//go:embed own.graphql` — `OptionalResolver.OwnResolver` SDL (M9.48c):
/// the `Ownable` interface, `GitBlob`/`GitTree`/`GitCommit.ownership`,
/// `OwnershipStats`/`OwnershipConnection`/`Owner`/`OwnershipReasonType`,
/// CODEOWNERS ingestion types and the `Query`/`Mutation` roots. `NewSchema`
/// appends it only when the own resolver is present, so `assemble_oss` never
/// includes it.
pub const OWN_SCHEMA: &str = include_str!("sdl/own.graphql");
