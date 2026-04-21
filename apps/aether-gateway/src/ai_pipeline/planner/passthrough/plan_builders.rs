use aether_contracts::{ExecutionPlan, RequestBody};

use super::{
    augment_sync_report_context, take_non_empty_string, LocalStreamPlanAndReport,
    LocalSyncPlanAndReport,
};
use crate::{GatewayControlSyncDecisionResponse, GatewayError};

pub(crate) fn build_passthrough_sync_plan_from_decision(
    parts: &http::request::Parts,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<LocalSyncPlanAndReport>, GatewayError> {
    let mut payload = payload;
    let Some(request_id) = take_non_empty_string(&mut payload.request_id) else {
        return Ok(None);
    };
    let Some(provider_id) = take_non_empty_string(&mut payload.provider_id) else {
        return Ok(None);
    };
    let Some(endpoint_id) = take_non_empty_string(&mut payload.endpoint_id) else {
        return Ok(None);
    };
    let Some(key_id) = take_non_empty_string(&mut payload.key_id) else {
        return Ok(None);
    };
    let Some(provider_api_format) = take_non_empty_string(&mut payload.provider_api_format) else {
        return Ok(None);
    };
    let Some(client_api_format) = take_non_empty_string(&mut payload.client_api_format) else {
        return Ok(None);
    };
    let Some(upstream_url) = take_non_empty_string(&mut payload.upstream_url) else {
        return Ok(None);
    };
    let provider_request_headers = std::mem::take(&mut payload.provider_request_headers);
    let ignored_provider_request_body = serde_json::Value::Null;
    let report_context = augment_sync_report_context(
        payload.report_context.take(),
        &provider_request_headers,
        &ignored_provider_request_body,
    )?;
    let request_body = resolve_passthrough_sync_request_body(
        payload.provider_request_body.take(),
        payload.provider_request_body_base64.take(),
    );
    let provider_request_method = take_non_empty_string(&mut payload.provider_request_method);
    let content_type = payload
        .content_type
        .take()
        .or_else(|| provider_request_headers.get("content-type").cloned());

    let plan = ExecutionPlan {
        request_id,
        candidate_id: payload.candidate_id.take(),
        provider_name: payload.provider_name.take(),
        provider_id,
        endpoint_id,
        key_id,
        method: provider_request_method.unwrap_or_else(|| parts.method.to_string()),
        url: upstream_url,
        headers: provider_request_headers,
        content_type,
        content_encoding: None,
        body: request_body,
        stream: false,
        client_api_format,
        provider_api_format,
        model_name: payload.model_name.take(),
        proxy: payload.proxy.take(),
        tls_profile: payload.tls_profile.take(),
        timeouts: payload.timeouts.take(),
    };

    Ok(Some(LocalSyncPlanAndReport {
        plan,
        report_kind: payload.report_kind,
        report_context,
    }))
}

pub(crate) fn build_passthrough_stream_plan_from_decision(
    parts: &http::request::Parts,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<LocalStreamPlanAndReport>, GatewayError> {
    let mut payload = payload;
    let Some(request_id) = take_non_empty_string(&mut payload.request_id) else {
        return Ok(None);
    };
    let Some(provider_id) = take_non_empty_string(&mut payload.provider_id) else {
        return Ok(None);
    };
    let Some(endpoint_id) = take_non_empty_string(&mut payload.endpoint_id) else {
        return Ok(None);
    };
    let Some(key_id) = take_non_empty_string(&mut payload.key_id) else {
        return Ok(None);
    };
    let Some(provider_api_format) = take_non_empty_string(&mut payload.provider_api_format) else {
        return Ok(None);
    };
    let Some(client_api_format) = take_non_empty_string(&mut payload.client_api_format) else {
        return Ok(None);
    };
    let Some(upstream_url) = take_non_empty_string(&mut payload.upstream_url) else {
        return Ok(None);
    };
    let provider_request_headers = std::mem::take(&mut payload.provider_request_headers);
    let content_type = payload
        .content_type
        .take()
        .or_else(|| provider_request_headers.get("content-type").cloned());
    let plan = ExecutionPlan {
        request_id,
        candidate_id: payload.candidate_id.take(),
        provider_name: payload.provider_name.take(),
        provider_id,
        endpoint_id,
        key_id,
        method: parts.method.to_string(),
        url: upstream_url,
        headers: provider_request_headers,
        content_type,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format,
        provider_api_format,
        model_name: payload.model_name.take(),
        proxy: payload.proxy.take(),
        tls_profile: payload.tls_profile.take(),
        timeouts: payload.timeouts.take(),
    };

    Ok(Some(LocalStreamPlanAndReport {
        plan,
        report_kind: payload.report_kind,
        report_context: payload.report_context,
    }))
}

fn resolve_passthrough_sync_request_body(
    provider_request_body: Option<serde_json::Value>,
    provider_request_body_base64: Option<String>,
) -> RequestBody {
    if let Some(body_bytes_b64) = provider_request_body_base64.and_then(trim_owned_non_empty_string)
    {
        return RequestBody {
            json_body: None,
            body_bytes_b64: Some(body_bytes_b64),
            body_ref: None,
        };
    }

    match provider_request_body.unwrap_or(serde_json::Value::Null) {
        serde_json::Value::Null => RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        other => RequestBody::from_json(other),
    }
}

fn trim_owned_non_empty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.len() == value.len() {
        return Some(value);
    }
    Some(trimmed.to_owned())
}
