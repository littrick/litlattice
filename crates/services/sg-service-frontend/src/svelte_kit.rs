#![forbid(unsafe_code)]

//! `cmd/frontend/internal/app/ui/sveltekit`: route selection and the
//! `svelteKit` JS context.
//!
//! The image can opt into the SvelteKit web app at build time. At runtime the
//! server only exposes the toggle and enabled routes when `_sk/index.html` is
//! present in the asset bundle; per-user `web-next` / `web-next-rollout`
//! feature flags decide which routes render the Svelte app.

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::Regex;
use sqlx::PgPool;

use crate::jscontext::SvelteKitJsContext;

pub mod routes {
    include!("svelte_kit_routes.rs");
}

/// Top-level routes served by the React app. The client checks these before
/// treating a single-segment path as a repository root, mirroring the
/// `knownRoutes` list the Go server derives from the gorilla mux.
///
/// Kept in sync with the frontend route table; only repo-root disambiguation
/// depends on it, so this covers the non-Svelte top-level pages.
pub const KNOWN_ROUTES: &[&str] = &[
    r"^/search/?$",
    r"^/search/stream",
    r"^/settings",
    r"^/site-admin",
    r"^/sign-in",
    r"^/sign-up",
    r"^/login",
    r"^/logout",
    r"^/request-access",
    r"^/site-init",
    r"^/verify-email",
    r"^/unlock-account",
    r"^/reset-password",
    r"^/check-username-taken",
    r"^/user/",
    r"^/users/",
    r"^/organizations/",
    r"^/teams/",
    r"^/contexts",
    r"^/saved-searches",
    r"^/search-jobs",
    r"^/code-monitoring",
    r"^/insights",
    r"^/batch-changes",
    r"^/notebooks",
    r"^/api",
    r"^/\.api",
    r"^/\.assets",
    r"^/\.internal",
    r"^/graphql",
];

fn matchers() -> &'static Vec<Option<Regex>> {
    static MATCHERS: OnceLock<Vec<Option<Regex>>> = OnceLock::new();
    MATCHERS.get_or_init(|| {
        routes::ROUTES
            .iter()
            .map(|route| Regex::new(route.pattern).ok())
            .collect()
    })
}

/// `true` when `path` matches route `index`.
pub fn matches(index: usize, path: &str) -> bool {
    matchers()
        .get(index)
        .and_then(Option::as_ref)
        .is_some_and(|regex| regex.is_match(path))
}

/// Routes enabled by the given feature flags (`web-next`, `web-next-rollout`).
pub fn enabled_indices(web_next: bool, rollout: bool, dotcom: bool) -> Vec<i64> {
    routes::ROUTES
        .iter()
        .enumerate()
        .filter(|(_, route)| {
            if route.dotcom && !dotcom {
                return false;
            }
            route.always || (route.opt_in && web_next) || (route.rollout && rollout)
        })
        .map(|(index, _)| index as i64)
        .collect()
}

/// The server-side check for rendering the Svelte shell for this request.
///
/// `repo_root_request` is true only when the request resolved to the
/// repository root/landing route; the repo-root Svelte route is a catch-all
/// and must not swallow other single-segment pages.
pub fn enabled_for_path(path: &str, enabled: &[i64], repo_root_request: bool) -> bool {
    enabled.iter().any(|index| {
        let index = *index as usize;
        let Some(route) = routes::ROUTES.get(index) else {
            return false;
        };
        if route.repo_root && !repo_root_request {
            return false;
        }
        matches(index, path)
    })
}

/// Build the `svelteKit` JS context for one request.
pub async fn js_context(
    pool: &PgPool,
    user_id: Option<i32>,
    available: bool,
    dotcom: bool,
) -> SvelteKitJsContext {
    if !available {
        return SvelteKitJsContext {
            enabled_routes: Vec::new(),
            show_toggle: false,
            known_routes: Vec::new(),
        };
    }

    let mut flags: HashMap<String, bool> =
        sg_db::feature_flags::get_global_feature_flags(pool)
            .await
            .unwrap_or_default();
    if let Some(uid) = user_id.filter(|uid| *uid > 0) {
        if let Ok(user_flags) = sg_db::feature_flags::get_user_flags(pool, uid).await {
            flags.extend(user_flags);
        }
    }

    let web_next = flags.get("web-next").copied().unwrap_or(false);
    let rollout = flags.get("web-next-rollout").copied().unwrap_or(false);
    SvelteKitJsContext {
        enabled_routes: enabled_indices(web_next, rollout, dotcom),
        show_toggle: flags.get("web-next-toggle").copied().unwrap_or(true),
        known_routes: KNOWN_ROUTES.iter().map(|route| (*route).to_owned()).collect(),
    }
}

/// Head/body fragments from `_sk/index.html` (`SvelteInjections`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SvelteShell {
    pub head: String,
    pub body: String,
}

/// Load the SvelteKit injections when the build packaged `_sk/index.html`.
pub fn load_shell(assets: &dyn sg_ui::AssetsProvider) -> Option<SvelteShell> {
    let entry = assets.assets().open("_sk/index.html").ok()?;
    if entry.is_dir {
        return None;
    }
    let contents = String::from_utf8(entry.data).ok()?;
    let (head, body) = split_injections(&contents)?;
    Some(SvelteShell { head, body })
}

fn known_route_matchers() -> &'static Vec<Regex> {
    static MATCHERS: OnceLock<Vec<Regex>> = OnceLock::new();
    MATCHERS.get_or_init(|| {
        KNOWN_ROUTES
            .iter()
            .filter_map(|pattern| Regex::new(pattern).ok())
            .collect()
    })
}

/// Whether the server should render the Svelte shell for this path.
///
/// The repository-root Svelte route is a catch-all, so it is only selected
/// when the path does not match a known React top-level route.
pub fn enabled_for_request_path(path: &str, enabled: &[i64]) -> bool {
    let known = known_route_matchers()
        .iter()
        .any(|regex| regex.is_match(path));
    enabled_for_path(path, enabled, !known)
}

/// Split `_sk/index.html` into the head/body injections
/// (`loadSvelteKitInjections`).
pub fn split_injections(contents: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = contents.split("<!-- SPLIT -->").collect();
    if parts.len() != 3 {
        return None;
    }
    Some((parts[1].to_owned(), parts[2].to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_table_compiles_and_matches() {
        assert_eq!(routes::ROUTES.len(), 15);
        assert!(matches(0, "/github.com/foo/bar"));
        assert!(matches(1, "/github.com/foo/bar/-/blob/src/main.go"));
        assert!(!matches(1, "/search"));
        assert!(matches(14, "/search"));
    }

    #[test]
    fn flags_select_route_indexes() {
        assert!(enabled_indices(false, false, false).is_empty());
        let enabled = enabled_indices(true, true, false);
        assert!(enabled.contains(&0));
        assert!(enabled.contains(&14));
        // The dotcom-only community route (index 12) is excluded off dotcom.
        assert!(!enabled.contains(&12));
        assert!(enabled_indices(true, true, true).contains(&12));
    }

    #[test]
    fn repo_root_is_gated_by_request_kind() {
        let enabled = enabled_indices(true, true, false);
        assert!(enabled_for_path("/github.com/foo/bar", &enabled, true));
        assert!(!enabled_for_path("/github.com/foo/bar", &enabled, false));
        assert!(enabled_for_path(
            "/github.com/foo/bar/-/blob/main.go",
            &enabled,
            false
        ));
        assert!(enabled_for_path("/search", &enabled, false));
    }

    #[test]
    fn splits_svelte_injections() {
        let (head, body) = split_injections("x<!-- SPLIT -->HEAD<!-- SPLIT -->BODY")
            .expect("split");
        assert_eq!(head, "HEAD");
        assert_eq!(body, "BODY");
        assert!(split_injections("no markers").is_none());
    }
}
