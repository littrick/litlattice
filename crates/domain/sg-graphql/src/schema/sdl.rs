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
