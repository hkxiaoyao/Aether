use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::Response;
use serde_json::Value;

use crate::ai_pipeline::{
    build_generated_tool_call_id,
    build_local_success_background_report as build_local_success_background_report_impl,
    build_local_success_conversion_background_report as build_local_success_conversion_background_report_impl,
    canonicalize_tool_arguments,
    prepare_local_success_response_parts as prepare_local_success_response_parts_impl,
    GatewayControlDecision,
};
pub(crate) use crate::ai_pipeline_api::{
    normalize_provider_private_response_value as unwrap_local_finalize_response_value,
    provider_private_response_allows_sync_finalize as local_finalize_allows_envelope,
};
use crate::api::response::build_client_response_from_parts;
use crate::{usage::GatewaySyncReportRequest, GatewayError};

pub(crate) struct LocalCoreSyncFinalizeOutcome {
    pub(crate) response: Response<Body>,
    pub(crate) background_report: Option<GatewaySyncReportRequest>,
}

fn build_local_success_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    status_code: u16,
    body_bytes: Vec<u8>,
    headers: BTreeMap<String, String>,
) -> Result<Response<Body>, GatewayError> {
    build_client_response_from_parts(
        status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )
}

pub(crate) fn build_local_success_outcome(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    body_json: Value,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let report_headers = payload.headers.clone();
    let (body_bytes, response_headers) =
        prepare_local_success_response_parts_impl(&payload.headers, &body_json)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let background_report =
        build_local_success_background_report_impl(payload, body_json, report_headers);
    build_local_success_outcome_with_report(
        trace_id,
        decision,
        payload.status_code,
        body_bytes,
        response_headers,
        background_report,
    )
}

pub(crate) fn build_local_success_outcome_with_report(
    trace_id: &str,
    decision: &GatewayControlDecision,
    status_code: u16,
    body_bytes: Vec<u8>,
    headers: BTreeMap<String, String>,
    background_report: Option<GatewaySyncReportRequest>,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let response =
        build_local_success_response(trace_id, decision, status_code, body_bytes, headers)?;
    Ok(LocalCoreSyncFinalizeOutcome {
        response,
        background_report,
    })
}

pub(crate) fn build_local_success_outcome_with_conversion_report(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    client_body_json: Value,
    provider_body_json: Value,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let (body_bytes, response_headers) =
        prepare_local_success_response_parts_impl(&payload.headers, &client_body_json)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let report_payload = build_local_success_conversion_background_report_impl(
        payload,
        client_body_json,
        provider_body_json,
    );

    build_local_success_outcome_with_report(
        trace_id,
        decision,
        payload.status_code,
        body_bytes,
        response_headers,
        report_payload,
    )
}
