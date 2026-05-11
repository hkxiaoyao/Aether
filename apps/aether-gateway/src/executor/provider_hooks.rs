use aether_contracts::ExecutionPlan;
use aether_provider_plugin::{
    ProviderRequestRewriteInput, ProviderResponseRewriteInput, ProviderStreamRewriteInput,
};
use axum::body::{to_bytes, Body, Bytes};
use base64::Engine as _;
use http::header::{HeaderName, HeaderValue};
use http::Response;
use serde_json::Value;
use std::collections::BTreeMap;
use tracing::{info, warn};

use crate::control::GatewayControlDecision;
use crate::{AppState, GatewayError};

#[derive(Debug, Clone)]
pub(crate) struct ProviderRequestForLocalExecution {
    pub(crate) parts: http::request::Parts,
    pub(crate) body_bytes: Bytes,
    pub(crate) body_json: Value,
    pub(crate) body_base64: Option<String>,
}

pub(crate) async fn rewrite_provider_request_for_local_execution(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    body_json: &Value,
    body_base64: Option<&str>,
    trace_id: &str,
    decision: &GatewayControlDecision,
) -> Result<ProviderRequestForLocalExecution, GatewayError> {
    let original = ProviderRequestForLocalExecution {
        parts: parts.clone(),
        body_bytes: body_bytes.clone(),
        body_json: body_json.clone(),
        body_base64: body_base64.map(ToOwned::to_owned),
    };
    let input = ProviderRequestRewriteInput {
        trace_id,
        method: parts.method.as_str(),
        path: parts.uri.path(),
        query: parts.uri.query().map(ToOwned::to_owned),
        headers: collect_provider_plugin_request_headers(&parts.headers),
        body_json: Some(body_json.clone()),
        body_base64: body_base64.map(ToOwned::to_owned),
        route_family: decision.route_family.clone(),
        route_kind: decision.route_kind.clone(),
        auth_endpoint_signature: decision.auth_endpoint_signature.clone(),
    };

    let output =
        match aether_provider_plugin::rewrite_provider_request(state.plugins.registry(), input)
            .await
        {
            Ok(Some(output)) => output,
            Ok(None) => return Ok(original),
            Err(err) => {
                warn!(
                    event_name = "provider_plugin_request_rewrite_failed",
                    log_type = "ops",
                    trace_id,
                    error = %err,
                    "gateway ignored failed provider plugin request rewrite"
                );
                return Ok(original);
            }
        };

    let Some(rewritten) = apply_provider_request_rewrite_output(
        original.clone(),
        output,
        trace_id,
        decision.route_family.as_deref().unwrap_or("-"),
        decision.route_kind.as_deref().unwrap_or("-"),
    ) else {
        return Ok(original);
    };
    Ok(rewritten)
}

pub(crate) async fn rewrite_provider_response_for_local_execution(
    state: &AppState,
    request_parts: &http::request::Parts,
    response: Response<Body>,
    trace_id: &str,
    decision: &GatewayControlDecision,
) -> Result<Response<Body>, GatewayError> {
    let response_capability = aether_provider_plugin::provider_capability(
        aether_provider_plugin::CAP_PROVIDER_RESPONSE_REWRITE,
    );
    if !decision.is_execution_runtime_candidate()
        || response_is_sse(response.headers())
        || !response_may_be_json(response.headers())
        || state
            .plugins
            .registry()
            .enabled_with_capability(&response_capability)
            .next()
            .is_none()
    {
        return Ok(response);
    }

    let (mut parts, body) = response.into_parts();
    let body_bytes = match to_bytes(body, usize::MAX).await {
        Ok(body_bytes) => body_bytes,
        Err(err) => {
            warn!(
                event_name = "provider_plugin_response_body_read_failed",
                log_type = "ops",
                trace_id,
                error = %err,
                "gateway could not read response body for provider plugin response rewrite"
            );
            return Ok(Response::from_parts(parts, Body::empty()));
        }
    };
    let body_json = match serde_json::from_slice::<Value>(&body_bytes) {
        Ok(body_json) => body_json,
        Err(_) => {
            return Ok(Response::from_parts(parts, Body::from(body_bytes)));
        }
    };

    let input = ProviderResponseRewriteInput {
        trace_id,
        method: request_parts.method.as_str(),
        path: request_parts.uri.path(),
        query: request_parts.uri.query().map(ToOwned::to_owned),
        status: parts.status.as_u16(),
        headers: collect_provider_plugin_response_headers(&parts.headers),
        body_json: Some(body_json),
        body_base64: None,
        route_family: decision.route_family.clone(),
        route_kind: decision.route_kind.clone(),
        auth_endpoint_signature: decision.auth_endpoint_signature.clone(),
    };

    let original_status = parts.status;
    let output =
        match aether_provider_plugin::rewrite_provider_response(state.plugins.registry(), input)
            .await
        {
            Ok(Some(output)) => output,
            Ok(None) => return Ok(Response::from_parts(parts, Body::from(body_bytes))),
            Err(err) => {
                warn!(
                    event_name = "provider_plugin_response_rewrite_failed",
                    log_type = "ops",
                    trace_id,
                    error = %err,
                    "gateway ignored failed provider plugin response rewrite"
                );
                return Ok(Response::from_parts(parts, Body::from(body_bytes)));
            }
        };

    let original_body_bytes = body_bytes.clone();
    let output_body_is_json = output.body_json.is_some();
    let rewritten_body = if let Some(body_json) = output.body_json {
        serde_json::to_vec(&body_json).map(Bytes::from).ok()
    } else if let Some(body_base64) = output.body_base64 {
        base64::engine::general_purpose::STANDARD
            .decode(body_base64.as_bytes())
            .ok()
            .map(Bytes::from)
    } else {
        Some(body_bytes)
    };
    let Some(rewritten_body) = rewritten_body else {
        return Ok(Response::from_parts(parts, Body::from(original_body_bytes)));
    };

    let status = http::StatusCode::from_u16(output.status)
        .ok()
        .filter(|status| status.as_u16() > 0)
        .unwrap_or(original_status);
    parts.status = status;
    for (name, value) in output.headers {
        if is_sensitive_plugin_header(name.as_str()) {
            continue;
        }
        let Some(header_name) = HeaderName::from_bytes(name.trim().as_bytes()).ok() else {
            continue;
        };
        let Some(header_value) = HeaderValue::from_str(value.trim()).ok() else {
            continue;
        };
        parts.headers.insert(header_name, header_value);
    }
    parts.headers.remove(http::header::CONTENT_LENGTH);
    parts.headers.insert(
        http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&rewritten_body.len().to_string())
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    if output_body_is_json {
        parts.headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }

    info!(
        event_name = "provider_plugin_response_rewrite_applied",
        log_type = "event",
        trace_id,
        route_family = decision.route_family.as_deref().unwrap_or("-"),
        route_kind = decision.route_kind.as_deref().unwrap_or("-"),
        "gateway applied provider plugin response rewrite after local execution"
    );
    Ok(Response::from_parts(parts, Body::from(rewritten_body)))
}

pub(crate) async fn rewrite_provider_stream_chunk_for_local_execution(
    state: &AppState,
    plan: &ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    headers: &BTreeMap<String, String>,
    chunk: Bytes,
) -> Bytes {
    let stream_capability = aether_provider_plugin::provider_capability(
        aether_provider_plugin::CAP_PROVIDER_STREAM_REWRITE,
    );
    if !decision.is_execution_runtime_candidate()
        || chunk.is_empty()
        || !provider_stream_rewrite_runtime_available(state, &stream_capability)
    {
        return chunk;
    }

    let original = chunk.clone();
    let chunk_text = std::str::from_utf8(chunk.as_ref())
        .ok()
        .map(ToOwned::to_owned);
    let chunk_base64 = if chunk_text.is_some() {
        None
    } else {
        Some(base64::engine::general_purpose::STANDARD.encode(chunk.as_ref()))
    };
    let (path, query) = provider_stream_plan_path_and_query(plan);
    let input = ProviderStreamRewriteInput {
        trace_id,
        method: plan.method.as_str(),
        path: path.as_str(),
        query,
        headers: collect_provider_plugin_btreemap_headers(headers),
        chunk_text,
        chunk_base64,
        route_family: decision.route_family.clone(),
        route_kind: decision.route_kind.clone(),
        auth_endpoint_signature: decision.auth_endpoint_signature.clone(),
    };

    let output = match aether_provider_plugin::rewrite_provider_stream(
        state.plugins.registry(),
        input,
    )
    .await
    {
        Ok(Some(output)) => output,
        Ok(None) => return original,
        Err(err) => {
            warn!(
                event_name = "provider_plugin_stream_rewrite_failed",
                log_type = "ops",
                trace_id,
                error = %err,
                "gateway ignored failed provider plugin stream rewrite"
            );
            return original;
        }
    };

    let rewritten = if let Some(text) = output.chunk_text {
        Bytes::from(text)
    } else if let Some(body_base64) = output.chunk_base64 {
        match base64::engine::general_purpose::STANDARD.decode(body_base64.as_bytes()) {
            Ok(bytes) => Bytes::from(bytes),
            Err(err) => {
                warn!(
                    event_name = "provider_plugin_stream_rewrite_decode_failed",
                    log_type = "ops",
                    trace_id,
                    error = %err,
                    "gateway ignored provider plugin stream rewrite with invalid base64"
                );
                return original;
            }
        }
    } else {
        return original;
    };

    info!(
        event_name = "provider_plugin_stream_rewrite_applied",
        log_type = "event",
        trace_id,
        route_family = decision.route_family.as_deref().unwrap_or("-"),
        route_kind = decision.route_kind.as_deref().unwrap_or("-"),
        "gateway applied provider plugin stream rewrite during local execution"
    );
    rewritten
}

fn apply_provider_request_rewrite_output(
    mut request: ProviderRequestForLocalExecution,
    output: aether_provider_plugin::ProviderRequestRewriteOutput,
    trace_id: &str,
    route_family: &str,
    route_kind: &str,
) -> Option<ProviderRequestForLocalExecution> {
    if let Some(path) = output
        .path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let uri = rewrite_uri_path(&request.parts.uri, path)?;
        request.parts.uri = uri;
    }

    for (name, value) in output.headers {
        if is_sensitive_plugin_header(name.as_str()) {
            continue;
        }
        let header_name = HeaderName::from_bytes(name.trim().as_bytes()).ok()?;
        let header_value = HeaderValue::from_str(value.trim()).ok()?;
        request.parts.headers.insert(header_name, header_value);
    }

    if let Some(body_json) = output.body_json {
        let bytes = serde_json::to_vec(&body_json).ok()?;
        request.body_bytes = Bytes::from(bytes);
        request.body_json = body_json;
        request.body_base64 = None;
        request.parts.headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    } else if let Some(body_base64) = output.body_base64 {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(body_base64.as_bytes())
            .ok()?;
        request.body_bytes = Bytes::from(bytes);
        request.body_json = serde_json::json!({});
        request.body_base64 = Some(body_base64);
    }

    info!(
        event_name = "provider_plugin_request_rewrite_applied",
        log_type = "event",
        trace_id,
        route_family,
        route_kind,
        "gateway applied provider plugin request rewrite before local execution"
    );
    Some(request)
}

fn rewrite_uri_path(original: &http::Uri, replacement: &str) -> Option<http::Uri> {
    let replacement = if replacement.starts_with('/') {
        replacement.to_string()
    } else {
        format!("/{replacement}")
    };
    let path_and_query = if replacement.contains('?') {
        replacement
    } else if let Some(query) = original.query().filter(|value| !value.is_empty()) {
        format!("{replacement}?{query}")
    } else {
        replacement
    };
    let mut parts = original.clone().into_parts();
    parts.path_and_query = Some(path_and_query.parse().ok()?);
    http::Uri::from_parts(parts).ok()
}

fn collect_provider_plugin_request_headers(headers: &http::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter(|(name, _)| !is_sensitive_plugin_header(name.as_str()))
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect()
}

fn collect_provider_plugin_response_headers(headers: &http::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter(|(name, _)| !is_sensitive_plugin_header(name.as_str()))
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect()
}

fn collect_provider_plugin_btreemap_headers(
    headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter(|(name, _)| !is_sensitive_plugin_header(name.as_str()))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect()
}

fn provider_stream_rewrite_runtime_available(
    state: &AppState,
    capability: &aether_plugin_core::PluginCapability,
) -> bool {
    state
        .plugins
        .registry()
        .enabled_with_capability(capability)
        .any(|entry| match entry.manifest.runtime.kind {
            aether_plugin_core::PluginRuntimeKind::Builtin => false,
            aether_plugin_core::PluginRuntimeKind::Manifest => true,
            aether_plugin_core::PluginRuntimeKind::Sidecar
            | aether_plugin_core::PluginRuntimeKind::Wasm => true,
        })
}

fn provider_stream_plan_path_and_query(plan: &ExecutionPlan) -> (String, Option<String>) {
    if let Ok(url) = url::Url::parse(&plan.url) {
        return (url.path().to_string(), url.query().map(ToOwned::to_owned));
    }
    (plan.url.clone(), None)
}

fn is_sensitive_plugin_header(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_lowercase().as_str(),
        "authorization"
            | "proxy-authorization"
            | "x-api-key"
            | "api-key"
            | "x-goog-api-key"
            | "cookie"
            | "set-cookie"
    )
}

fn response_is_sse(headers: &http::HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"))
}

fn response_may_be_json(headers: &http::HeaderMap) -> bool {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            let value = value.to_ascii_lowercase();
            value.contains("application/json") || value.contains("+json")
        })
}

#[cfg(test)]
mod tests {
    use super::{
        rewrite_provider_response_for_local_execution,
        rewrite_provider_stream_chunk_for_local_execution,
    };
    use crate::control::GatewayControlDecision;
    use crate::plugins::GatewayPluginRegistry;
    use crate::AppState;
    use aether_contracts::{ExecutionPlan, RequestBody};
    use aether_plugin_core::{PluginManifest, PluginRegistry, PluginRuntimeKind};
    use axum::body::{to_bytes, Body, Bytes};
    use http::Response;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    #[tokio::test]
    async fn rewrites_json_response_and_filters_sensitive_headers() {
        let manifest = PluginManifest {
            id: "local.provider.response.gateway".to_string(),
            name: "Gateway Response Rewrite Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([aether_provider_plugin::provider_capability(
                aether_provider_plugin::CAP_PROVIDER_RESPONSE_REWRITE,
            )]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                aether_provider_plugin::PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "response_rewrite": {
                        "rules": [{
                            "route_family": "test",
                            "route_kind": "chat",
                            "set_status": 202,
                            "set_headers": {
                                "x-provider-plugin": "response",
                                "set-cookie": "blocked=true"
                            },
                            "set_body_fields": {
                                "provider": "plugin"
                            },
                            "remove_body_fields": ["debug"]
                        }]
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(aether_provider_plugin::manifest_provider_runtime(manifest)),
            None,
        );
        let state = AppState::new()
            .expect("state should build")
            .with_plugin_registry_for_tests(GatewayPluginRegistry::from_registry(registry));
        let (request_parts, _) = http::Request::builder()
            .method(http::Method::POST)
            .uri("/v1/chat")
            .body(())
            .expect("request should build")
            .into_parts();
        let decision = GatewayControlDecision::synthetic(
            "/v1/chat",
            Some("ai_public".to_string()),
            Some("test".to_string()),
            Some("chat".to_string()),
            Some("test:chat".to_string()),
        )
        .with_execution_runtime_candidate(true);
        let response = Response::builder()
            .status(200)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"id":"1","debug":true}"#))
            .expect("response should build");

        let response = rewrite_provider_response_for_local_execution(
            &state,
            &request_parts,
            response,
            "trace",
            &decision,
        )
        .await
        .expect("rewrite should not fail");

        assert_eq!(response.status(), http::StatusCode::ACCEPTED);
        assert_eq!(
            response
                .headers()
                .get("x-provider-plugin")
                .and_then(|value| value.to_str().ok()),
            Some("response")
        );
        assert!(response.headers().get(http::header::SET_COOKIE).is_none());
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let body_json: serde_json::Value =
            serde_json::from_slice(&body).expect("body should be json");
        assert_eq!(body_json["provider"], "plugin");
        assert_eq!(body_json["id"], "1");
        assert!(body_json.get("debug").is_none());
    }

    #[tokio::test]
    async fn rewrites_stream_chunk_via_provider_plugin() {
        let manifest = PluginManifest {
            id: "local.provider.stream.gateway".to_string(),
            name: "Gateway Stream Rewrite Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([aether_provider_plugin::provider_capability(
                aether_provider_plugin::CAP_PROVIDER_STREAM_REWRITE,
            )]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                aether_provider_plugin::PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "stream_rewrite": {
                        "rules": [{
                            "route_family": "test",
                            "route_kind": "chat",
                            "contains": "upstream",
                            "replace_with": "plugin"
                        }]
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(aether_provider_plugin::manifest_provider_runtime(manifest)),
            None,
        );
        let state = AppState::new()
            .expect("state should build")
            .with_plugin_registry_for_tests(GatewayPluginRegistry::from_registry(registry));
        let decision = GatewayControlDecision::synthetic(
            "/v1/chat",
            Some("ai_public".to_string()),
            Some("test".to_string()),
            Some("chat".to_string()),
            Some("test:chat".to_string()),
        )
        .with_execution_runtime_candidate(true);
        let plan = ExecutionPlan {
            request_id: "req-test".to_string(),
            candidate_id: None,
            provider_name: Some("test".to_string()),
            provider_id: "provider-test".to_string(),
            endpoint_id: "endpoint-test".to_string(),
            key_id: "key-test".to_string(),
            method: "POST".to_string(),
            url: "https://provider.example.test/v1/chat?stream=true".to_string(),
            headers: BTreeMap::new(),
            content_type: Some("application/json".to_string()),
            content_encoding: None,
            body: RequestBody::from_json(json!({})),
            stream: true,
            client_api_format: "test:chat".to_string(),
            provider_api_format: "test:chat".to_string(),
            model_name: Some("test-model".to_string()),
            proxy: None,
            transport_profile: None,
            timeouts: None,
        };
        let headers = BTreeMap::from([
            ("content-type".to_string(), "text/event-stream".to_string()),
            ("set-cookie".to_string(), "blocked=true".to_string()),
        ]);

        let chunk = rewrite_provider_stream_chunk_for_local_execution(
            &state,
            &plan,
            "trace",
            &decision,
            &headers,
            Bytes::from_static(b"data: upstream\n\n"),
        )
        .await;

        assert_eq!(chunk.as_ref(), b"data: plugin\n\n");
    }
}
