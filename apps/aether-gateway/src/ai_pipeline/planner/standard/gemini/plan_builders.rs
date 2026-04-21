use aether_contracts::{ExecutionPlan, RequestBody};

use super::{
    augment_sync_report_context, generic_decision_missing_exact_provider_request,
    take_non_empty_string, LocalStreamPlanAndReport, LocalSyncPlanAndReport,
};
use crate::ai_pipeline::transport::ensure_upstream_auth_header;
use crate::{GatewayControlSyncDecisionResponse, GatewayError};

pub(crate) fn build_gemini_sync_plan_from_decision(
    _parts: &http::request::Parts,
    _body_json: &serde_json::Value,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<LocalSyncPlanAndReport>, GatewayError> {
    let mut payload = payload;
    if generic_decision_missing_exact_provider_request(&payload) {
        return Ok(None);
    }
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
    let Some(url) = take_non_empty_string(&mut payload.upstream_url) else {
        return Ok(None);
    };
    let auth_header = take_non_empty_string(&mut payload.auth_header);
    let auth_value = take_non_empty_string(&mut payload.auth_value);
    if auth_header.is_some() != auth_value.is_some() {
        return Ok(None);
    }
    let Some(provider_api_format) = take_non_empty_string(&mut payload.provider_api_format) else {
        return Ok(None);
    };
    let Some(client_api_format) = take_non_empty_string(&mut payload.client_api_format) else {
        return Ok(None);
    };
    let Some(provider_request_body_value) = payload.provider_request_body.take() else {
        return Ok(None);
    };

    let mut provider_request_headers = std::mem::take(&mut payload.provider_request_headers);
    if let (Some(auth_header), Some(auth_value)) = (auth_header.as_deref(), auth_value.as_deref()) {
        ensure_upstream_auth_header(&mut provider_request_headers, auth_header, auth_value);
    }
    if payload.upstream_is_stream {
        provider_request_headers
            .entry("accept".to_string())
            .or_insert_with(|| "text/event-stream".to_string());
    }
    let content_type = payload
        .content_type
        .take()
        .or_else(|| Some("application/json".to_string()));
    let report_context = augment_sync_report_context(
        payload.report_context.take(),
        &provider_request_headers,
        &provider_request_body_value,
    )?;
    let plan = ExecutionPlan {
        request_id,
        candidate_id: payload.candidate_id.take(),
        provider_name: payload.provider_name.take(),
        provider_id,
        endpoint_id,
        key_id,
        method: "POST".to_string(),
        url,
        headers: std::mem::take(&mut provider_request_headers),
        content_type,
        content_encoding: None,
        body: RequestBody::from_json(provider_request_body_value),
        stream: payload.upstream_is_stream,
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

pub(crate) fn build_gemini_stream_plan_from_decision(
    _parts: &http::request::Parts,
    _body_json: &serde_json::Value,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<LocalStreamPlanAndReport>, GatewayError> {
    let mut payload = payload;
    if generic_decision_missing_exact_provider_request(&payload) {
        return Ok(None);
    }
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
    let Some(url) = take_non_empty_string(&mut payload.upstream_url) else {
        return Ok(None);
    };
    let auth_header = take_non_empty_string(&mut payload.auth_header);
    let auth_value = take_non_empty_string(&mut payload.auth_value);
    if auth_header.is_some() != auth_value.is_some() {
        return Ok(None);
    }
    let Some(provider_api_format) = take_non_empty_string(&mut payload.provider_api_format) else {
        return Ok(None);
    };
    let Some(client_api_format) = take_non_empty_string(&mut payload.client_api_format) else {
        return Ok(None);
    };
    let Some(provider_request_body_value) = payload.provider_request_body.take() else {
        return Ok(None);
    };

    let mut provider_request_headers = std::mem::take(&mut payload.provider_request_headers);
    if let (Some(auth_header), Some(auth_value)) = (auth_header.as_deref(), auth_value.as_deref()) {
        ensure_upstream_auth_header(&mut provider_request_headers, auth_header, auth_value);
    }
    provider_request_headers.insert("accept".to_string(), "text/event-stream".to_string());
    let content_type = payload
        .content_type
        .take()
        .or_else(|| Some("application/json".to_string()));
    let report_context = augment_sync_report_context(
        payload.report_context.take(),
        &provider_request_headers,
        &provider_request_body_value,
    )?;
    let plan = ExecutionPlan {
        request_id,
        candidate_id: payload.candidate_id.take(),
        provider_name: payload.provider_name.take(),
        provider_id,
        endpoint_id,
        key_id,
        method: "POST".to_string(),
        url,
        headers: std::mem::take(&mut provider_request_headers),
        content_type,
        content_encoding: None,
        body: RequestBody::from_json(provider_request_body_value),
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
        report_context,
    }))
}
