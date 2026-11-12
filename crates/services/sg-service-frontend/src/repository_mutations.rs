#![forbid(unsafe_code)]

//! `Mutation.recloneRepository` / `deleteRepositoryFromDisk` /
//! `reindexRepository` (`cmd/frontend/graphqlbackend/graphqlbackend.go:868-920`,
//! `repository_reindex.go:13-38`).
//!
//! All three are site-admin only. The baseline's `DeleteRepositoryFromDisk`
//! returns a non-nil `EmptyResponse` together with an error; the wrapper
//! (`RecloneRepository`) then adds its own context. Note the baseline's
//! "busy cloning" branch calls `errors.Wrap(err, ...)` while `err` is nil,
//! which yields a successful empty response without deleting anything; that
//! behaviour is reproduced here.

use sg_graphql::exec::{Actor, RootField};

use crate::admin_store::{
    check_current_user_is_site_admin, AdminMutationOutcome, AdminStore, CloneStatus, RepoRow,
};
use crate::external_services::unmarshal_repository_id;

pub async fn prefetch_repository_mutations(
    store: &dyn AdminStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, AdminMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "recloneRepository" => {
                reclone_repository(store, field, actor, viewer_site_admin).await
            }
            "deleteRepositoryFromDisk" => {
                delete_repository_from_disk(store, field, actor, viewer_site_admin).await
            }
            "reindexRepository" => {
                reindex_repository(store, field, actor, viewer_site_admin).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

/// `relay.UnmarshalSpec(id, &repoID)`: the kind is not validated, the spec must
/// fit in an int32.
fn repo_id_from_spec(raw: &str) -> Result<i32, String> {
    unmarshal_repository_id(raw)
}

/// `schemaResolver.repositoryByID` (`graphqlbackend.go:925-935`).
async fn repository_by_id(
    store: &dyn AdminStore,
    raw: &str,
) -> Result<RepoRow, String> {
    let repo_id = repo_id_from_spec(raw)?;
    match store.get_repo_by_id(repo_id).await {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(format!("repo not found: id={repo_id}")),
        Err(error) => Err(error),
    }
}

async fn reclone_repository(
    store: &dyn AdminStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(raw) = field.arguments.string("repo") else {
        return AdminMutationOutcome::Error("missing repo argument".to_owned());
    };
    let repo = match repository_by_id(store, raw).await {
        Ok(repo) => repo,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    match delete_repository_from_disk(store, field, actor, viewer_site_admin).await {
        AdminMutationOutcome::Error(error) => AdminMutationOutcome::Error(format!(
            "could not delete repository with ID {}: {error}",
            repo.id
        )),
        // A successful inner delete returns `&EmptyResponse{}`.
        _ => AdminMutationOutcome::Empty,
    }
}

async fn delete_repository_from_disk(
    store: &dyn AdminStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let Some(raw) = field.arguments.string("repo") else {
        return AdminMutationOutcome::Error("missing repo argument".to_owned());
    };
    let repo_id = match repo_id_from_spec(raw) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let clone_status = match store.get_gitserver_repo(repo_id).await {
        Ok(Some(status)) => status,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!(
                "error while fetching repository with ID {repo_id}: gitserver repo not found"
            ))
        }
        Err(error) => {
            return AdminMutationOutcome::Error(format!(
                "error while fetching repository with ID {repo_id}: {error}"
            ))
        }
    };
    if clone_status == CloneStatus::Cloning {
        // `errors.Wrap(nil, ...) == nil`: the baseline returns success without
        // running the delete.
        return AdminMutationOutcome::Empty;
    }
    // `backend.Repos.RecloneRepository` fetches the repo first and wraps its
    // error: `error while fetching repo with ID %d`.
    let repo = match store.get_repo_by_id(repo_id).await {
        Ok(Some(repo)) => repo,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!(
                "error while deleting repository with ID {repo_id}: error while fetching repo with ID {repo_id}: repo not found: id={repo_id}"
            ))
        }
        Err(error) => {
            return AdminMutationOutcome::Error(format!(
                "error while deleting repository with ID {repo_id}: error while fetching repo with ID {repo_id}: {error}"
            ))
        }
    };
    match store.reclone_repository(repo.id, &repo.name).await {
        Ok(()) => AdminMutationOutcome::Empty,
        Err(error) => AdminMutationOutcome::Error(format!(
            "error while deleting repository with ID {repo_id}: {error}"
        )),
    }
}

async fn reindex_repository(
    store: &dyn AdminStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(raw) = field.arguments.string("repository") else {
        return AdminMutationOutcome::Error("missing repository argument".to_owned());
    };
    let repo = match repository_by_id(store, raw).await {
        Ok(repo) => repo,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    match store.reindex_repository(repo.id, &repo.name).await {
        Ok(()) => AdminMutationOutcome::Empty,
        Err(error) => AdminMutationOutcome::Error(error),
    }
}
