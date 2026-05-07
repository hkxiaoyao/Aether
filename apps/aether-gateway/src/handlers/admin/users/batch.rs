use super::{
    build_admin_users_bad_request_response, build_admin_users_read_only_response,
    normalize_admin_user_api_formats, normalize_admin_user_string_list,
};
use crate::handlers::admin::request::{AdminAppState, AdminRequestContext};
use crate::handlers::admin::shared::attach_admin_audit_response;
use crate::GatewayError;
use axum::{
    body::{Body, Bytes},
    http,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct AdminUserSelectionFilters {
    #[serde(default)]
    search: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    is_active: Option<bool>,
}

#[derive(Debug, Clone, Default)]
struct AdminUserSelectionRequest {
    user_ids: Vec<String>,
    filters: Option<AdminUserSelectionFilters>,
    filters_scope_present: bool,
}

#[derive(Debug)]
struct AdminUserBatchActionRequest {
    selection: AdminUserSelectionRequest,
    action: String,
    payload: Option<Value>,
}

#[derive(Debug, serde::Deserialize)]
struct RawAdminUserBatchActionRequest {
    selection: Value,
    action: String,
    #[serde(default)]
    payload: Option<Value>,
}

#[derive(Debug, Clone, Default)]
struct NormalizedAdminUserSelectionFilters {
    search: Option<String>,
    role: Option<String>,
    is_active: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct AdminUserSelectionItem {
    user_id: String,
    username: String,
    email: Option<String>,
    role: String,
    is_active: bool,
}

#[derive(Debug, Clone, Default)]
struct ResolvedAdminUserSelection {
    items: Vec<AdminUserSelectionItem>,
    missing_user_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct AdminUserBatchMutation {
    allowed_providers_present: bool,
    allowed_providers: Option<Vec<String>>,
    allowed_api_formats_present: bool,
    allowed_api_formats: Option<Vec<String>>,
    allowed_models_present: bool,
    allowed_models: Option<Vec<String>>,
    rate_limit_present: bool,
    rate_limit: Option<i32>,
    is_active: Option<bool>,
    modified_fields: Vec<&'static str>,
}

pub(in super::super) async fn build_admin_resolve_user_selection_response(
    state: &AdminAppState<'_>,
    _request_context: &AdminRequestContext<'_>,
    request_body: Option<&Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let selection = match parse_resolve_selection_request(request_body) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_user_batch_bad_request_response(detail)),
    };
    let resolved = match resolve_admin_user_selection(state, selection).await {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_user_batch_bad_request_response(detail)),
    };

    Ok(Json(json!({
        "total": resolved.items.len(),
        "items": resolved.items,
    }))
    .into_response())
}

pub(in super::super) async fn build_admin_user_batch_action_response(
    state: &AdminAppState<'_>,
    _request_context: &AdminRequestContext<'_>,
    request_body: Option<&Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_user_write_capability() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法批量更新用户",
        ));
    }

    let request = match parse_batch_action_request(request_body) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_user_batch_bad_request_response(detail)),
    };
    let mutation = match parse_batch_mutation(&request.action, request.payload) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_user_batch_bad_request_response(detail)),
    };
    let resolved = match resolve_admin_user_selection(state, request.selection).await {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_user_batch_bad_request_response(detail)),
    };

    let mut success = 0usize;
    let mut failures = resolved
        .missing_user_ids
        .iter()
        .map(|user_id| json!({ "user_id": user_id, "reason": "用户不存在或已删除" }))
        .collect::<Vec<_>>();

    for item in &resolved.items {
        let updated = state
            .update_local_auth_user_admin_fields(
                &item.user_id,
                None,
                mutation.allowed_providers_present,
                mutation.allowed_providers.clone(),
                mutation.allowed_api_formats_present,
                mutation.allowed_api_formats.clone(),
                mutation.allowed_models_present,
                mutation.allowed_models.clone(),
                mutation.rate_limit_present,
                mutation.rate_limit,
                mutation.is_active,
            )
            .await?;
        if updated.is_some() {
            success += 1;
        } else {
            failures.push(json!({
                "user_id": item.user_id,
                "reason": "用户不存在或已删除",
            }));
        }
    }

    let failed = failures.len();
    let total = success + failed;
    let response = Json(json!({
        "total": total,
        "success": success,
        "failed": failed,
        "failures": failures,
        "action": request.action.trim().to_ascii_lowercase(),
        "modified_fields": mutation.modified_fields,
    }))
    .into_response();

    Ok(attach_admin_audit_response(
        response,
        "admin_users_batch_action_executed",
        "batch_update_users",
        "user_batch",
        "users",
    ))
}

fn parse_resolve_selection_request(
    request_body: Option<&Bytes>,
) -> Result<AdminUserSelectionRequest, String> {
    match request_body {
        None => Ok(AdminUserSelectionRequest::default()),
        Some(body) if body.is_empty() => Ok(AdminUserSelectionRequest::default()),
        Some(body) => {
            let value = serde_json::from_slice::<Value>(body)
                .map_err(|_| "Invalid JSON request body".to_string())?;
            parse_selection_request_value(value)
        }
    }
}

fn parse_batch_action_request(
    request_body: Option<&Bytes>,
) -> Result<AdminUserBatchActionRequest, String> {
    match request_body {
        Some(body) if !body.is_empty() => {
            let raw = serde_json::from_slice::<RawAdminUserBatchActionRequest>(body)
                .map_err(|_| "Invalid JSON request body".to_string())?;
            Ok(AdminUserBatchActionRequest {
                selection: parse_selection_request_value(raw.selection)?,
                action: raw.action,
                payload: raw.payload,
            })
        }
        _ => Err("Invalid JSON request body".to_string()),
    }
}

fn parse_selection_request_value(value: Value) -> Result<AdminUserSelectionRequest, String> {
    let Value::Object(map) = value else {
        return Err("selection 必须是对象".to_string());
    };

    let user_ids = match map.get("user_ids") {
        None | Some(Value::Null) => Vec::new(),
        Some(value) => serde_json::from_value::<Vec<String>>(value.clone())
            .map_err(|_| "user_ids 必须是字符串数组".to_string())?,
    };

    let (filters_scope_present, filters) = match map.get("filters") {
        Some(Value::Object(_)) => {
            let filters = serde_json::from_value::<AdminUserSelectionFilters>(
                map.get("filters").cloned().unwrap_or(Value::Null),
            )
            .map_err(|_| "filters 参数不合法".to_string())?;
            (true, Some(filters))
        }
        None | Some(Value::Null) => (false, None),
        Some(_) => return Err("filters 必须是对象".to_string()),
    };

    Ok(AdminUserSelectionRequest {
        user_ids,
        filters,
        filters_scope_present,
    })
}

async fn resolve_admin_user_selection(
    state: &AdminAppState<'_>,
    selection: AdminUserSelectionRequest,
) -> Result<ResolvedAdminUserSelection, String> {
    let filters = normalize_selection_filters(selection.filters)?;
    let explicit_user_ids = normalize_user_ids(selection.user_ids);
    if explicit_user_ids.is_empty() && !selection.filters_scope_present {
        return Err("至少需要选择一个用户或明确提供筛选条件".to_string());
    }
    let should_resolve_filters = selection.filters_scope_present;
    let mut items_by_id = BTreeMap::new();
    let mut missing_user_ids = Vec::new();

    if !explicit_user_ids.is_empty() {
        let users = state
            .resolve_auth_user_summaries_by_ids(&explicit_user_ids)
            .await
            .map_err(|_| "用户数据不可用".to_string())?;
        for user_id in explicit_user_ids {
            match users.get(&user_id).filter(|user| !user.is_deleted) {
                Some(user) => {
                    items_by_id.insert(
                        user.id.clone(),
                        AdminUserSelectionItem {
                            user_id: user.id.clone(),
                            username: user.username.clone(),
                            email: user.email.clone(),
                            role: user.role.clone(),
                            is_active: user.is_active,
                        },
                    );
                }
                None => missing_user_ids.push(user_id),
            }
        }
    }

    if should_resolve_filters {
        let users = state
            .list_export_users()
            .await
            .map_err(|_| "用户数据不可用".to_string())?;
        for user in users
            .into_iter()
            .filter(|user| admin_user_matches_filters(user, filters.as_ref()))
        {
            items_by_id.insert(
                user.id.clone(),
                AdminUserSelectionItem {
                    user_id: user.id,
                    username: user.username,
                    email: user.email,
                    role: user.role,
                    is_active: user.is_active,
                },
            );
        }
    }

    let mut items = items_by_id.into_values().collect::<Vec<_>>();
    items.sort_by(|left, right| {
        left.username
            .to_ascii_lowercase()
            .cmp(&right.username.to_ascii_lowercase())
            .then_with(|| left.user_id.cmp(&right.user_id))
    });

    Ok(ResolvedAdminUserSelection {
        items,
        missing_user_ids,
    })
}

fn normalize_selection_filters(
    filters: Option<AdminUserSelectionFilters>,
) -> Result<Option<NormalizedAdminUserSelectionFilters>, String> {
    let Some(filters) = filters else {
        return Ok(None);
    };
    let search = filters
        .search
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let role = match filters
        .role
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && value != "all")
    {
        Some(role) if matches!(role.as_str(), "user" | "admin") => Some(role),
        Some(_) => return Err("role 参数不合法".to_string()),
        None => None,
    };

    Ok(Some(NormalizedAdminUserSelectionFilters {
        search,
        role,
        is_active: filters.is_active,
    }))
}

fn admin_user_matches_filters(
    user: &aether_data::repository::users::StoredUserExportRow,
    filters: Option<&NormalizedAdminUserSelectionFilters>,
) -> bool {
    let Some(filters) = filters else {
        return true;
    };
    if filters
        .role
        .as_deref()
        .is_some_and(|role| !user.role.eq_ignore_ascii_case(role))
    {
        return false;
    }
    if filters
        .is_active
        .is_some_and(|is_active| user.is_active != is_active)
    {
        return false;
    }
    if let Some(search) = filters.search.as_deref() {
        let searchable_text = format!(
            "{} {}",
            user.username,
            user.email.as_deref().unwrap_or_default()
        )
        .to_ascii_lowercase();
        let keywords = search
            .to_ascii_lowercase()
            .split_whitespace()
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if !keywords
            .iter()
            .all(|keyword| searchable_text.contains(keyword))
        {
            return false;
        }
    }
    true
}

fn normalize_user_ids(user_ids: Vec<String>) -> Vec<String> {
    user_ids
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn parse_batch_mutation(
    action: &str,
    payload: Option<Value>,
) -> Result<AdminUserBatchMutation, String> {
    match action.trim().to_ascii_lowercase().as_str() {
        "enable" => Ok(AdminUserBatchMutation {
            is_active: Some(true),
            modified_fields: vec!["is_active"],
            ..AdminUserBatchMutation::default()
        }),
        "disable" => Ok(AdminUserBatchMutation {
            is_active: Some(false),
            modified_fields: vec!["is_active"],
            ..AdminUserBatchMutation::default()
        }),
        "update_access_control" => parse_access_control_mutation(payload),
        _ => Err("不支持的批量操作".to_string()),
    }
}

fn parse_access_control_mutation(payload: Option<Value>) -> Result<AdminUserBatchMutation, String> {
    let Some(Value::Object(payload)) = payload else {
        return Err("payload 必须是对象".to_string());
    };
    let mut mutation = AdminUserBatchMutation::default();

    if let Some(value) = payload.get("allowed_providers") {
        mutation.allowed_providers_present = true;
        mutation.allowed_providers = parse_optional_string_list(value, "allowed_providers")?;
        mutation.modified_fields.push("allowed_providers");
    }
    if let Some(value) = payload.get("allowed_api_formats") {
        mutation.allowed_api_formats_present = true;
        mutation.allowed_api_formats = parse_optional_api_formats(value)?;
        mutation.modified_fields.push("allowed_api_formats");
    }
    if let Some(value) = payload.get("allowed_models") {
        mutation.allowed_models_present = true;
        mutation.allowed_models = parse_optional_string_list(value, "allowed_models")?;
        mutation.modified_fields.push("allowed_models");
    }
    if let Some(value) = payload.get("rate_limit") {
        mutation.rate_limit_present = true;
        mutation.rate_limit = parse_optional_rate_limit(value)?;
        mutation.modified_fields.push("rate_limit");
    }

    if mutation.modified_fields.is_empty() {
        return Err("至少需要选择一个要修改的访问控制字段".to_string());
    }

    Ok(mutation)
}

fn parse_optional_string_list(
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<String>>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let values = serde_json::from_value::<Vec<String>>(value.clone())
        .map_err(|_| format!("{field_name} 必须是字符串数组或 null"))?;
    normalize_admin_user_string_list(Some(values), field_name)
}

fn parse_optional_api_formats(value: &Value) -> Result<Option<Vec<String>>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let values = serde_json::from_value::<Vec<String>>(value.clone())
        .map_err(|_| "allowed_api_formats 必须是字符串数组或 null".to_string())?;
    normalize_admin_user_api_formats(Some(values))
}

fn parse_optional_rate_limit(value: &Value) -> Result<Option<i32>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let rate_limit = serde_json::from_value::<i32>(value.clone())
        .map_err(|_| "rate_limit 必须是整数或 null".to_string())?;
    if rate_limit < 0 {
        return Err("rate_limit 必须大于等于 0".to_string());
    }
    Ok(Some(rate_limit))
}

fn build_admin_user_batch_bad_request_response(detail: String) -> Response<Body> {
    if detail.as_str() == "缺少 user_id" {
        return build_admin_users_bad_request_response("缺少 user_id");
    }
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}
