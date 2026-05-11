use crate::handlers::admin::request::{AdminAppState, AdminRequestContext};
use crate::GatewayError;
use axum::{
    body::Body,
    http,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub(crate) async fn maybe_build_local_admin_plugins_response(
    state: &AdminAppState<'_>,
    request_context: &AdminRequestContext<'_>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.decision() else {
        return Ok(None);
    };
    if decision.route_family.as_deref() != Some("plugins_manage") {
        return Ok(None);
    }
    if request_context.method() != http::Method::GET {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("list_plugins") if is_admin_plugins_root(request_context.path()) => Ok(Some(
            Json(state.app().plugins.provider_plugins_payload()).into_response(),
        )),
        Some("plugin_detail") => {
            let Some(plugin_id) = plugin_id_from_path(request_context.path()) else {
                return Ok(Some(not_found_response(request_context.path())));
            };
            Ok(Some(
                state
                    .app()
                    .plugins
                    .provider_plugin_payload(plugin_id)
                    .map(Json)
                    .map(IntoResponse::into_response)
                    .unwrap_or_else(|| not_found_response(request_context.path())),
            ))
        }
        _ => Ok(None),
    }
}

fn is_admin_plugins_root(path: &str) -> bool {
    matches!(path.trim_end_matches('/'), "/api/admin/plugins")
}

fn plugin_id_from_path(path: &str) -> Option<&str> {
    let path = path.trim_end_matches('/');
    let plugin_id = path.strip_prefix("/api/admin/plugins/")?.trim();
    (!plugin_id.is_empty() && !plugin_id.contains('/')).then_some(plugin_id)
}

fn not_found_response(path: &str) -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({
            "detail": "plugin not found",
            "request_path": path,
        })),
    )
        .into_response()
}
