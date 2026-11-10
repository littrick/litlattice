#![forbid(unsafe_code)]

//! `Mutation.createFeatureFlag` / `updateFeatureFlag` / `deleteFeatureFlag`
//! (`cmd/frontend/graphqlbackend/feature_flags.go:236-288`).
//!
//! The executor resolves fields synchronously while the database is async, so
//! the writes run in [`prefetch_feature_flag_mutations`] before execution and
//! the [`MutationRoot`](crate::settings_mutation::MutationRoot) replays the
//! outcome. Each root field is site-admin gated before any database access;
//! `value` takes priority over `rolloutBasisPoints`, matching the baseline's
//! `if args.Value != nil { ... } else if args.RolloutBasisPoints != nil`
//! branch. The returned flag reuses the read surface's `FeatureFlagData` (with
//! its prefetched overrides) so the response object is identical to
//! `Query.featureFlag`.

use std::sync::Arc;

use sg_graphql::exec::{Actor, RootField};

use crate::graphql_resolvers::{
    check_current_user_is_site_admin, feature_flag_data, FeatureFlagData, FeatureFlagRegistry,
    FeatureFlagStore, OrgMembershipsStore,
};
use sg_db::feature_flags::FeatureFlagKind;

/// The baseline's `errors.Errorf("either 'value' or 'rolloutBasisPoints' must
/// be set")` (`feature_flags.go:254,283`).
pub const EITHER_VALUE_OR_ROLLOUT: &str = "either 'value' or 'rolloutBasisPoints' must be set";

/// One mutation root field's prefetched result.
#[derive(Debug, Clone)]
pub enum FeatureFlagMutationOutcome {
    /// `createFeatureFlag` / `updateFeatureFlag`: the RETURNING row plus the
    /// overrides/registry the response object resolves from.
    Flag(Arc<FeatureFlagData>, Arc<FeatureFlagRegistry>),
    /// `deleteFeatureFlag`'s non-nil `&EmptyResponse{}`.
    Empty,
    Error(String),
}

/// Execute every feature-flag mutation root field in document order.
pub async fn prefetch_feature_flag_mutations(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, FeatureFlagMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "createFeatureFlag" => {
                write_flag(
                    flags_store,
                    namespaces,
                    field,
                    actor,
                    viewer_site_admin,
                    false,
                )
                .await
            }
            "updateFeatureFlag" => {
                write_flag(
                    flags_store,
                    namespaces,
                    field,
                    actor,
                    viewer_site_admin,
                    true,
                )
                .await
            }
            "deleteFeatureFlag" => delete_flag(flags_store, field, actor, viewer_site_admin).await,
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

async fn write_flag(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    update: bool,
) -> FeatureFlagMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return FeatureFlagMutationOutcome::Error(message);
    }
    let Some(name) = field.arguments.string("name") else {
        return FeatureFlagMutationOutcome::Error("missing name argument".to_owned());
    };
    let kind = match flag_kind(field) {
        Ok(Some(kind)) => kind,
        Ok(None) => return FeatureFlagMutationOutcome::Error(EITHER_VALUE_OR_ROLLOUT.to_owned()),
        Err(message) => return FeatureFlagMutationOutcome::Error(message),
    };

    let flag = if update {
        flags_store.update_feature_flag(name, kind).await
    } else {
        flags_store.create_feature_flag(name, kind).await
    };
    match flag {
        Ok(flag) => {
            let data =
                feature_flag_data(flags_store, namespaces, flag, actor, viewer_site_admin).await;
            let mut registry = FeatureFlagRegistry::default();
            registry.flags.insert(data.name.clone(), data.clone());
            FeatureFlagMutationOutcome::Flag(Arc::new(data), Arc::new(registry))
        }
        Err(message) => FeatureFlagMutationOutcome::Error(message),
    }
}

async fn delete_flag(
    flags_store: &dyn FeatureFlagStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> FeatureFlagMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return FeatureFlagMutationOutcome::Error(message);
    }
    let Some(name) = field.arguments.string("name") else {
        return FeatureFlagMutationOutcome::Error("missing name argument".to_owned());
    };
    match flags_store.delete_feature_flag(name).await {
        Ok(()) => FeatureFlagMutationOutcome::Empty,
        Err(message) => FeatureFlagMutationOutcome::Error(message),
    }
}

/// `CreateFeatureFlag`/`UpdateFeatureFlag`'s argument priority: `value` wins
/// when both are supplied.
fn flag_kind(field: &RootField) -> Result<Option<FeatureFlagKind>, String> {
    kind_from_arguments(
        field.arguments.bool("value"),
        field.arguments.i64("rolloutBasisPoints"),
    )
}

fn kind_from_arguments(
    value: Option<bool>,
    rollout: Option<i64>,
) -> Result<Option<FeatureFlagKind>, String> {
    // graph-gophers coerces `Int` to int32 before the resolver runs, so an
    // out-of-range value is an argument error regardless of `value`.
    let rollout = match rollout {
        Some(rollout) => {
            Some(i32::try_from(rollout).map_err(|_| "not a 32-bit integer".to_owned())?)
        }
        None => None,
    };
    Ok(if let Some(value) = value {
        Some(FeatureFlagKind::Bool(value))
    } else {
        rollout.map(FeatureFlagKind::Rollout)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_takes_priority_over_rollout() {
        assert_eq!(
            kind_from_arguments(Some(false), Some(5000)),
            Ok(Some(FeatureFlagKind::Bool(false)))
        );
    }

    #[test]
    fn rollout_is_used_when_value_is_absent() {
        assert_eq!(
            kind_from_arguments(None, Some(10000)),
            Ok(Some(FeatureFlagKind::Rollout(10000)))
        );
    }

    #[test]
    fn neither_argument_is_an_error() {
        assert_eq!(kind_from_arguments(None, None), Ok(None));
    }

    #[test]
    fn out_of_range_rollout_is_rejected_even_with_value() {
        assert_eq!(
            kind_from_arguments(Some(true), Some(i64::from(i32::MAX) + 1)),
            Err("not a 32-bit integer".to_owned())
        );
    }
}
