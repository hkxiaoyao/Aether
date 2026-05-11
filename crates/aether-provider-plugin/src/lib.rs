use aether_plugin_core::{
    PluginCapability, PluginHookEnvelope, PluginHookResponse, PluginManifest, PluginRegistry,
    PluginRuntime, PluginRuntimeKind, PluginRuntimeManifest,
};
use aether_provider_transport::provider_types::{
    fixed_provider_template, provider_runtime_policy, provider_type_admin_oauth_template,
    FixedProviderEndpointConfigValue, FixedProviderTemplate, ProviderApiFormatInheritance,
    ProviderLocalEmbeddingSupport, ProviderRuntimePolicy,
};
use async_trait::async_trait;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const PROVIDER_DOMAIN: &str = "provider";
pub const CAP_PROVIDER_ROUTE_ALIAS: &str = "provider.route_alias";
pub const CAP_PROVIDER_REQUEST_REWRITE: &str = "provider.request_rewrite";
pub const CAP_PROVIDER_RESPONSE_REWRITE: &str = "provider.response_rewrite";
pub const CAP_PROVIDER_STREAM_REWRITE: &str = "provider.stream_rewrite";
pub const CAP_PROVIDER_MODEL_FETCH: &str = "provider.model_fetch";
pub const CAP_PROVIDER_HEALTH_CHECK: &str = "provider.health_check";
pub const CAP_PROVIDER_OAUTH_FLOW: &str = "provider.oauth_flow";
pub const CAP_PROVIDER_MANAGEMENT_PROXY: &str = "provider.management_proxy";
pub const CAP_PROVIDER_WEBSOCKET_PROXY: &str = "provider.websocket_proxy";

const FIXED_PROVIDER_TYPES: &[&str] = &[
    "claude_code",
    "codex",
    "chatgpt_web",
    "kiro",
    "gemini_cli",
    "vertex_ai",
    "antigravity",
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginConfig {
    #[serde(default)]
    pub provider_types: Vec<String>,
    #[serde(default)]
    pub api_formats: Vec<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub endpoints: Vec<ProviderPluginEndpointConfig>,
    #[serde(default)]
    pub route_aliases: Vec<ProviderPluginRouteAlias>,
    #[serde(default)]
    pub request_rewrite: Option<ProviderPluginRequestRewriteConfig>,
    #[serde(default)]
    pub response_rewrite: Option<ProviderPluginResponseRewriteConfig>,
    #[serde(default)]
    pub stream_rewrite: Option<ProviderPluginStreamRewriteConfig>,
    #[serde(default)]
    pub runtime_policy: Option<ProviderPluginRuntimePolicy>,
    #[serde(default)]
    pub auth: Option<ProviderPluginAuthConfig>,
    #[serde(default)]
    pub model_fetch: Option<ProviderPluginModelFetchConfig>,
    #[serde(default)]
    pub health_check: Option<ProviderPluginHealthCheckConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPluginRouteAlias {
    pub method: String,
    pub path: String,
    pub route_family: String,
    pub route_kind: String,
    pub auth_endpoint_signature: String,
    #[serde(default)]
    pub request_auth_channel: Option<String>,
    #[serde(default = "default_true")]
    pub execution_runtime_candidate: bool,
}

fn default_true() -> bool {
    true
}

impl ProviderPluginRouteAlias {
    pub fn matches(&self, method: &str, path: &str) -> bool {
        self.method.eq_ignore_ascii_case(method)
            && normalize_path(&self.path) == normalize_path(path)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProviderPluginRequestRewriteConfig {
    #[serde(default)]
    pub rules: Vec<ProviderPluginRequestRewriteRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProviderPluginRequestRewriteRule {
    #[serde(default)]
    pub route_family: Option<String>,
    #[serde(default)]
    pub route_kind: Option<String>,
    #[serde(default)]
    pub auth_endpoint_signature: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub set_path: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub set_headers: BTreeMap<String, String>,
    #[serde(default)]
    pub set_body_fields: BTreeMap<String, Value>,
    #[serde(default)]
    pub remove_body_fields: Vec<String>,
}

impl ProviderPluginRequestRewriteRule {
    pub fn matches(&self, request: &ProviderRequestRewriteInput<'_>) -> bool {
        option_matches(self.method.as_deref(), Some(request.method), true)
            && option_matches(self.path.as_deref(), Some(request.path), false)
            && option_matches(
                self.route_family.as_deref(),
                request.route_family.as_deref(),
                true,
            )
            && option_matches(
                self.route_kind.as_deref(),
                request.route_kind.as_deref(),
                true,
            )
            && option_matches(
                self.auth_endpoint_signature.as_deref(),
                request.auth_endpoint_signature.as_deref(),
                true,
            )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProviderPluginResponseRewriteConfig {
    #[serde(default)]
    pub rules: Vec<ProviderPluginResponseRewriteRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProviderPluginResponseRewriteRule {
    #[serde(default)]
    pub route_family: Option<String>,
    #[serde(default)]
    pub route_kind: Option<String>,
    #[serde(default)]
    pub auth_endpoint_signature: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub status: Option<u16>,
    #[serde(default)]
    pub set_status: Option<u16>,
    #[serde(default)]
    pub set_headers: BTreeMap<String, String>,
    #[serde(default)]
    pub set_body_fields: BTreeMap<String, Value>,
    #[serde(default)]
    pub remove_body_fields: Vec<String>,
}

impl ProviderPluginResponseRewriteRule {
    pub fn matches(&self, response: &ProviderResponseRewriteInput<'_>) -> bool {
        self.status.is_none_or(|status| status == response.status)
            && option_matches(self.method.as_deref(), Some(response.method), true)
            && option_matches(self.path.as_deref(), Some(response.path), false)
            && option_matches(
                self.route_family.as_deref(),
                response.route_family.as_deref(),
                true,
            )
            && option_matches(
                self.route_kind.as_deref(),
                response.route_kind.as_deref(),
                true,
            )
            && option_matches(
                self.auth_endpoint_signature.as_deref(),
                response.auth_endpoint_signature.as_deref(),
                true,
            )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProviderPluginStreamRewriteConfig {
    #[serde(default)]
    pub rules: Vec<ProviderPluginStreamRewriteRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginStreamRewriteRule {
    #[serde(default)]
    pub route_family: Option<String>,
    #[serde(default)]
    pub route_kind: Option<String>,
    #[serde(default)]
    pub auth_endpoint_signature: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub contains: Option<String>,
    pub replace_with: String,
    #[serde(default = "default_true")]
    pub replace_all: bool,
}

impl ProviderPluginStreamRewriteRule {
    pub fn matches(&self, stream: &ProviderStreamRewriteInput<'_>) -> bool {
        option_matches(self.method.as_deref(), Some(stream.method), true)
            && option_matches(self.path.as_deref(), Some(stream.path), false)
            && option_matches(
                self.route_family.as_deref(),
                stream.route_family.as_deref(),
                true,
            )
            && option_matches(
                self.route_kind.as_deref(),
                stream.route_kind.as_deref(),
                true,
            )
            && option_matches(
                self.auth_endpoint_signature.as_deref(),
                stream.auth_endpoint_signature.as_deref(),
                true,
            )
            && self
                .contains
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none_or(|needle| {
                    stream
                        .chunk_text
                        .as_deref()
                        .is_some_and(|chunk| chunk.contains(needle))
                })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginEndpointConfig {
    pub item_key: String,
    pub api_format: String,
    #[serde(default)]
    pub custom_path: Option<String>,
    #[serde(default)]
    pub config_defaults: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginRuntimePolicy {
    pub fixed_provider: bool,
    pub api_format_inheritance: String,
    pub enable_format_conversion_by_default: bool,
    pub allow_auth_channel_mismatch_by_default: bool,
    pub oauth_is_bearer_like: bool,
    pub supports_model_fetch: bool,
    pub supports_local_openai_chat_transport: bool,
    pub supports_local_same_format_transport: bool,
    pub local_embedding_support: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginAuthConfig {
    #[serde(default)]
    pub oauth_template: Option<ProviderPluginOAuthTemplate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginOAuthTemplate {
    pub provider_type: String,
    pub display_name: String,
    pub authorize_url: String,
    pub token_url: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub redirect_uri: String,
    pub use_pkce: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginModelFetchConfig {
    pub supported: bool,
    #[serde(default)]
    pub models: Vec<Value>,
    #[serde(default)]
    pub upstream_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderPluginHealthCheckConfig {
    #[serde(default = "default_true")]
    pub success: bool,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct ProviderModelFetchInput<'a> {
    pub trace_id: &'a str,
    pub provider_id: &'a str,
    pub provider_type: &'a str,
    pub endpoint_id: Option<&'a str>,
    pub key_id: Option<&'a str>,
    pub api_format: Option<&'a str>,
    pub base_url: Option<&'a str>,
    pub auth_type: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderModelFetchOutput {
    pub fetched_model_ids: Vec<String>,
    pub cached_models: Vec<Value>,
    pub errors: Vec<String>,
    pub has_success: bool,
    pub upstream_metadata: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ProviderHealthCheckInput<'a> {
    pub trace_id: &'a str,
    pub provider_id: Option<&'a str>,
    pub provider_type: Option<&'a str>,
    pub base_url: Option<&'a str>,
    pub architecture_id: Option<&'a str>,
    pub config: Value,
    pub credentials: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderHealthCheckOutput {
    pub success: bool,
    pub message: Option<String>,
    pub data: Option<Value>,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderPluginView {
    pub id: String,
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub runtime: PluginRuntimeKind,
    pub source: aether_plugin_core::PluginSource,
    pub capabilities: Vec<String>,
    pub provider: Option<ProviderPluginConfig>,
    pub load_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProviderRequestRewriteInput<'a> {
    pub trace_id: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub query: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub body_json: Option<Value>,
    pub body_base64: Option<String>,
    pub route_family: Option<String>,
    pub route_kind: Option<String>,
    pub auth_endpoint_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderRequestRewriteOutput {
    pub path: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub body_json: Option<Value>,
    pub body_base64: Option<String>,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct ProviderResponseRewriteInput<'a> {
    pub trace_id: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub query: Option<String>,
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body_json: Option<Value>,
    pub body_base64: Option<String>,
    pub route_family: Option<String>,
    pub route_kind: Option<String>,
    pub auth_endpoint_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderResponseRewriteOutput {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body_json: Option<Value>,
    pub body_base64: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProviderStreamRewriteInput<'a> {
    pub trace_id: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub query: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub chunk_text: Option<String>,
    pub chunk_base64: Option<String>,
    pub route_family: Option<String>,
    pub route_kind: Option<String>,
    pub auth_endpoint_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderStreamRewriteOutput {
    pub chunk_text: Option<String>,
    pub chunk_base64: Option<String>,
    pub metadata: BTreeMap<String, Value>,
}

pub fn provider_capability(value: &'static str) -> PluginCapability {
    PluginCapability::new(value).expect("provider capability constants must be valid")
}

pub fn all_provider_capabilities() -> BTreeSet<PluginCapability> {
    [
        CAP_PROVIDER_ROUTE_ALIAS,
        CAP_PROVIDER_REQUEST_REWRITE,
        CAP_PROVIDER_RESPONSE_REWRITE,
        CAP_PROVIDER_STREAM_REWRITE,
        CAP_PROVIDER_MODEL_FETCH,
        CAP_PROVIDER_HEALTH_CHECK,
        CAP_PROVIDER_OAUTH_FLOW,
        CAP_PROVIDER_MANAGEMENT_PROXY,
        CAP_PROVIDER_WEBSOCKET_PROXY,
    ]
    .into_iter()
    .map(provider_capability)
    .collect()
}

pub fn provider_config_from_manifest(manifest: &PluginManifest) -> Option<ProviderPluginConfig> {
    manifest
        .domain_config(PROVIDER_DOMAIN)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub fn provider_plugin_views(registry: &PluginRegistry) -> Vec<ProviderPluginView> {
    registry
        .entries()
        .filter(|entry| provider_config_from_manifest(&entry.manifest).is_some())
        .map(|entry| ProviderPluginView {
            id: entry.manifest.id.clone(),
            name: entry.manifest.name.clone(),
            version: entry.manifest.version.clone(),
            enabled: entry.manifest.enabled,
            runtime: entry.manifest.runtime.kind,
            source: entry.source,
            capabilities: entry
                .manifest
                .capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect(),
            provider: provider_config_from_manifest(&entry.manifest)
                .map(|config| sanitize_provider_config_for_view(&config)),
            load_error: entry.load_error.clone(),
        })
        .collect()
}

pub fn provider_plugin_view(registry: &PluginRegistry, id: &str) -> Option<ProviderPluginView> {
    let entry = registry.get(id)?;
    provider_config_from_manifest(&entry.manifest).map(|provider| ProviderPluginView {
        id: entry.manifest.id.clone(),
        name: entry.manifest.name.clone(),
        version: entry.manifest.version.clone(),
        enabled: entry.manifest.enabled,
        runtime: entry.manifest.runtime.kind,
        source: entry.source,
        capabilities: entry
            .manifest
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_string())
            .collect(),
        provider: Some(sanitize_provider_config_for_view(&provider)),
        load_error: entry.load_error.clone(),
    })
}

pub fn provider_config_for_type(
    registry: &PluginRegistry,
    provider_type: &str,
) -> Option<ProviderPluginConfig> {
    let provider_type = provider_type.trim();
    if provider_type.is_empty() {
        return None;
    }
    registry
        .entries()
        .filter(|entry| entry.manifest.enabled)
        .filter_map(|entry| provider_config_from_manifest(&entry.manifest))
        .find(|config| {
            config
                .provider_types
                .iter()
                .any(|item| item.trim().eq_ignore_ascii_case(provider_type))
        })
}

pub fn provider_oauth_templates(registry: &PluginRegistry) -> Vec<ProviderPluginOAuthTemplate> {
    let capability = provider_capability(CAP_PROVIDER_OAUTH_FLOW);
    registry
        .enabled_with_capability(&capability)
        .filter_map(|entry| provider_config_from_manifest(&entry.manifest))
        .filter_map(provider_oauth_template_from_config)
        .collect()
}

pub fn provider_oauth_template_for_type(
    registry: &PluginRegistry,
    provider_type: &str,
) -> Option<ProviderPluginOAuthTemplate> {
    let provider_type = provider_type.trim();
    if provider_type.is_empty() {
        return None;
    }
    provider_oauth_templates(registry)
        .into_iter()
        .find(|template| {
            template
                .provider_type
                .trim()
                .eq_ignore_ascii_case(provider_type)
        })
}

pub fn provider_config_for_api_format(
    registry: &PluginRegistry,
    api_format: &str,
) -> Option<ProviderPluginConfig> {
    let api_format = normalize_api_format(api_format);
    if api_format.is_empty() {
        return None;
    }
    registry
        .entries()
        .filter(|entry| entry.manifest.enabled)
        .filter_map(|entry| provider_config_from_manifest(&entry.manifest))
        .find(|config| {
            config
                .api_formats
                .iter()
                .any(|item| normalize_api_format(item) == api_format)
        })
}

fn provider_oauth_template_from_config(
    config: ProviderPluginConfig,
) -> Option<ProviderPluginOAuthTemplate> {
    let template = config.auth?.oauth_template?;
    if template.provider_type.trim().is_empty()
        || template.authorize_url.trim().is_empty()
        || template.token_url.trim().is_empty()
        || template.client_id.trim().is_empty()
        || template.redirect_uri.trim().is_empty()
    {
        return None;
    }
    if !config.provider_types.is_empty()
        && !config.provider_types.iter().any(|provider_type| {
            provider_type
                .trim()
                .eq_ignore_ascii_case(&template.provider_type)
        })
    {
        return None;
    }
    Some(template)
}

pub fn provider_runtime_policy_for_type(
    registry: &PluginRegistry,
    provider_type: &str,
) -> ProviderRuntimePolicy {
    provider_config_for_type(registry, provider_type)
        .and_then(|config| config.runtime_policy)
        .and_then(|policy| provider_runtime_policy_from_plugin(&policy))
        .unwrap_or_else(|| provider_runtime_policy(provider_type))
}

pub fn provider_type_enables_format_conversion_by_default(
    registry: &PluginRegistry,
    provider_type: &str,
) -> bool {
    provider_runtime_policy_for_type(registry, provider_type).enable_format_conversion_by_default
}

pub fn provider_type_oauth_is_bearer_like(registry: &PluginRegistry, provider_type: &str) -> bool {
    provider_runtime_policy_for_type(registry, provider_type).oauth_is_bearer_like
}

pub fn provider_type_supports_model_fetch(registry: &PluginRegistry, provider_type: &str) -> bool {
    provider_runtime_policy_for_type(registry, provider_type).supports_model_fetch
}

pub fn provider_type_supports_local_same_format_transport(
    registry: &PluginRegistry,
    provider_type: &str,
) -> bool {
    provider_runtime_policy_for_type(registry, provider_type).supports_local_same_format_transport
}

pub fn provider_type_supports_local_openai_chat_transport(
    registry: &PluginRegistry,
    provider_type: &str,
) -> bool {
    provider_runtime_policy_for_type(registry, provider_type).supports_local_openai_chat_transport
}

pub fn provider_type_supports_local_embedding_transport(
    registry: &PluginRegistry,
    provider_type: &str,
    api_format: &str,
) -> bool {
    provider_runtime_policy_for_type(registry, provider_type)
        .supports_local_embedding_transport(api_format)
}

fn provider_runtime_policy_from_plugin(
    policy: &ProviderPluginRuntimePolicy,
) -> Option<ProviderRuntimePolicy> {
    Some(ProviderRuntimePolicy {
        fixed_provider: policy.fixed_provider,
        api_format_inheritance: api_format_inheritance_from_label(&policy.api_format_inheritance)?,
        enable_format_conversion_by_default: policy.enable_format_conversion_by_default,
        allow_auth_channel_mismatch_by_default: policy.allow_auth_channel_mismatch_by_default,
        oauth_is_bearer_like: policy.oauth_is_bearer_like,
        supports_model_fetch: policy.supports_model_fetch,
        supports_local_openai_chat_transport: policy.supports_local_openai_chat_transport,
        supports_local_same_format_transport: policy.supports_local_same_format_transport,
        local_embedding_support: local_embedding_support_from_label(
            &policy.local_embedding_support,
        )?,
    })
}

fn sanitize_provider_config_for_view(config: &ProviderPluginConfig) -> ProviderPluginConfig {
    let mut config = config.clone();
    if let Some(auth) = config.auth.as_mut() {
        if let Some(template) = auth.oauth_template.as_mut() {
            template.client_secret = None;
        }
    }
    config
}

fn api_format_inheritance_from_label(value: &str) -> Option<ProviderApiFormatInheritance> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Some(ProviderApiFormatInheritance::None),
        "oauth" => Some(ProviderApiFormatInheritance::OAuth),
        "oauth_or_bearer" => Some(ProviderApiFormatInheritance::OAuthOrBearer),
        "oauth_or_configured_bearer" => Some(ProviderApiFormatInheritance::OAuthOrConfiguredBearer),
        _ => None,
    }
}

fn local_embedding_support_from_label(value: &str) -> Option<ProviderLocalEmbeddingSupport> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Some(ProviderLocalEmbeddingSupport::None),
        "any_known" => Some(ProviderLocalEmbeddingSupport::AnyKnown),
        "openai" => Some(ProviderLocalEmbeddingSupport::OpenAi),
        "gemini" => Some(ProviderLocalEmbeddingSupport::Gemini),
        "jina" => Some(ProviderLocalEmbeddingSupport::Jina),
        "doubao" => Some(ProviderLocalEmbeddingSupport::Doubao),
        _ => None,
    }
}

pub fn resolve_provider_route_alias(
    registry: &PluginRegistry,
    method: &str,
    path: &str,
) -> Option<ProviderPluginRouteAlias> {
    let capability = provider_capability(CAP_PROVIDER_ROUTE_ALIAS);
    let alias = registry
        .enabled_with_capability(&capability)
        .filter_map(|entry| provider_config_from_manifest(&entry.manifest))
        .flat_map(|config| config.route_aliases.into_iter())
        .find(|alias| alias.matches(method, path));
    alias
}

pub async fn rewrite_provider_request(
    registry: &PluginRegistry,
    input: ProviderRequestRewriteInput<'_>,
) -> Result<Option<ProviderRequestRewriteOutput>, aether_plugin_core::PluginError> {
    let capability = provider_capability(CAP_PROVIDER_REQUEST_REWRITE);
    let base_envelope = PluginHookEnvelope {
        plugin_id: String::new(),
        trace_id: input.trace_id.to_string(),
        capability: capability.clone(),
        hook: CAP_PROVIDER_REQUEST_REWRITE.to_string(),
        method: Some(input.method.to_string()),
        path: Some(input.path.to_string()),
        query: input.query.clone(),
        headers: input.headers.clone(),
        body_json: input.body_json.clone(),
        body_base64: input.body_base64.clone(),
        context: BTreeMap::from([
            (
                "domain".to_string(),
                Value::String(PROVIDER_DOMAIN.to_string()),
            ),
            (
                "route_family".to_string(),
                input
                    .route_family
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "route_kind".to_string(),
                input
                    .route_kind
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "auth_endpoint_signature".to_string(),
                input
                    .auth_endpoint_signature
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
        ]),
    };
    for entry in registry.enabled_with_capability(&capability) {
        let Some(config) = provider_config_from_manifest(&entry.manifest) else {
            continue;
        };
        if !provider_config_matches_request(
            &config,
            input.route_family.as_deref(),
            input.auth_endpoint_signature.as_deref(),
        ) {
            continue;
        }
        let Some(runtime) = entry.runtime.as_ref() else {
            continue;
        };
        let mut envelope = base_envelope.clone();
        envelope.plugin_id = entry.manifest.id.clone();
        let response = runtime.call_hook(envelope).await?;
        if let Some(output) = provider_request_rewrite_output_from_hook_response(response) {
            return Ok(Some(output));
        }
    }
    Ok(None)
}

pub async fn rewrite_provider_response(
    registry: &PluginRegistry,
    input: ProviderResponseRewriteInput<'_>,
) -> Result<Option<ProviderResponseRewriteOutput>, aether_plugin_core::PluginError> {
    let capability = provider_capability(CAP_PROVIDER_RESPONSE_REWRITE);
    let base_envelope = PluginHookEnvelope {
        plugin_id: String::new(),
        trace_id: input.trace_id.to_string(),
        capability: capability.clone(),
        hook: CAP_PROVIDER_RESPONSE_REWRITE.to_string(),
        method: Some(input.method.to_string()),
        path: Some(input.path.to_string()),
        query: input.query.clone(),
        headers: input.headers.clone(),
        body_json: input.body_json.clone(),
        body_base64: input.body_base64.clone(),
        context: BTreeMap::from([
            (
                "domain".to_string(),
                Value::String(PROVIDER_DOMAIN.to_string()),
            ),
            (
                "status".to_string(),
                Value::Number(serde_json::Number::from(input.status)),
            ),
            (
                "route_family".to_string(),
                input
                    .route_family
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "route_kind".to_string(),
                input
                    .route_kind
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "auth_endpoint_signature".to_string(),
                input
                    .auth_endpoint_signature
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
        ]),
    };
    for entry in registry.enabled_with_capability(&capability) {
        let Some(config) = provider_config_from_manifest(&entry.manifest) else {
            continue;
        };
        if !provider_config_matches_request(
            &config,
            input.route_family.as_deref(),
            input.auth_endpoint_signature.as_deref(),
        ) {
            continue;
        }
        let Some(runtime) = entry.runtime.as_ref() else {
            continue;
        };
        let mut envelope = base_envelope.clone();
        envelope.plugin_id = entry.manifest.id.clone();
        let response = runtime.call_hook(envelope).await?;
        if let Some(output) =
            provider_response_rewrite_output_from_hook_response(response, input.status)
        {
            return Ok(Some(output));
        }
    }
    Ok(None)
}

pub async fn rewrite_provider_stream(
    registry: &PluginRegistry,
    input: ProviderStreamRewriteInput<'_>,
) -> Result<Option<ProviderStreamRewriteOutput>, aether_plugin_core::PluginError> {
    let capability = provider_capability(CAP_PROVIDER_STREAM_REWRITE);
    let body_json = Some(json!({
        "chunk_text": input.chunk_text.clone(),
        "chunk_base64": input.chunk_base64.clone(),
    }));
    let base_envelope = PluginHookEnvelope {
        plugin_id: String::new(),
        trace_id: input.trace_id.to_string(),
        capability: capability.clone(),
        hook: CAP_PROVIDER_STREAM_REWRITE.to_string(),
        method: Some(input.method.to_string()),
        path: Some(input.path.to_string()),
        query: input.query.clone(),
        headers: input.headers.clone(),
        body_json,
        body_base64: input.chunk_base64.clone(),
        context: BTreeMap::from([
            (
                "domain".to_string(),
                Value::String(PROVIDER_DOMAIN.to_string()),
            ),
            (
                "route_family".to_string(),
                input
                    .route_family
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "route_kind".to_string(),
                input
                    .route_kind
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "auth_endpoint_signature".to_string(),
                input
                    .auth_endpoint_signature
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
        ]),
    };

    for entry in registry.enabled_with_capability(&capability) {
        let Some(config) = provider_config_from_manifest(&entry.manifest) else {
            continue;
        };
        if !provider_config_matches_request(
            &config,
            input.route_family.as_deref(),
            input.auth_endpoint_signature.as_deref(),
        ) {
            continue;
        }
        let Some(runtime) = entry.runtime.as_ref() else {
            continue;
        };
        let mut envelope = base_envelope.clone();
        envelope.plugin_id = entry.manifest.id.clone();
        let response = runtime.call_hook(envelope).await?;
        if let Some(output) = provider_stream_rewrite_output_from_hook_response(response) {
            return Ok(Some(output));
        }
    }
    Ok(None)
}

pub async fn fetch_provider_models(
    registry: &PluginRegistry,
    input: ProviderModelFetchInput<'_>,
) -> Result<Option<ProviderModelFetchOutput>, aether_plugin_core::PluginError> {
    let capability = provider_capability(CAP_PROVIDER_MODEL_FETCH);
    let base_envelope = PluginHookEnvelope {
        plugin_id: String::new(),
        trace_id: input.trace_id.to_string(),
        capability: capability.clone(),
        hook: CAP_PROVIDER_MODEL_FETCH.to_string(),
        method: None,
        path: None,
        query: None,
        headers: BTreeMap::new(),
        body_json: None,
        body_base64: None,
        context: BTreeMap::from([
            (
                "domain".to_string(),
                Value::String(PROVIDER_DOMAIN.to_string()),
            ),
            (
                "provider_id".to_string(),
                Value::String(input.provider_id.to_string()),
            ),
            (
                "provider_type".to_string(),
                Value::String(input.provider_type.to_string()),
            ),
            (
                "endpoint_id".to_string(),
                input
                    .endpoint_id
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
            (
                "key_id".to_string(),
                input
                    .key_id
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
            (
                "api_format".to_string(),
                input
                    .api_format
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
            (
                "base_url".to_string(),
                input
                    .base_url
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
            (
                "auth_type".to_string(),
                input
                    .auth_type
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
        ]),
    };

    for entry in registry.enabled_with_capability(&capability) {
        let Some(config) = provider_config_from_manifest(&entry.manifest) else {
            continue;
        };
        if !provider_config_matches_request(&config, Some(input.provider_type), input.api_format) {
            continue;
        }
        let Some(runtime) = entry.runtime.as_ref() else {
            continue;
        };
        let mut envelope = base_envelope.clone();
        envelope.plugin_id = entry.manifest.id.clone();
        let response = runtime.call_hook(envelope).await?;
        if let Some(output) = provider_model_fetch_output_from_hook_response(response) {
            return Ok(Some(output));
        }
    }
    Ok(None)
}

pub async fn check_provider_health(
    registry: &PluginRegistry,
    input: ProviderHealthCheckInput<'_>,
) -> Result<Option<ProviderHealthCheckOutput>, aether_plugin_core::PluginError> {
    let capability = provider_capability(CAP_PROVIDER_HEALTH_CHECK);
    let provider_type = input
        .provider_type
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let base_envelope = PluginHookEnvelope {
        plugin_id: String::new(),
        trace_id: input.trace_id.to_string(),
        capability: capability.clone(),
        hook: CAP_PROVIDER_HEALTH_CHECK.to_string(),
        method: None,
        path: None,
        query: None,
        headers: BTreeMap::new(),
        body_json: Some(json!({
            "config": input.config,
            "credentials": input.credentials,
        })),
        body_base64: None,
        context: BTreeMap::from([
            (
                "domain".to_string(),
                Value::String(PROVIDER_DOMAIN.to_string()),
            ),
            (
                "provider_id".to_string(),
                input
                    .provider_id
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
            (
                "provider_type".to_string(),
                provider_type
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
            (
                "base_url".to_string(),
                input
                    .base_url
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
            (
                "architecture_id".to_string(),
                input
                    .architecture_id
                    .map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
            ),
        ]),
    };

    for entry in registry.enabled_with_capability(&capability) {
        let Some(config) = provider_config_from_manifest(&entry.manifest) else {
            continue;
        };
        if !provider_config_matches_request(&config, provider_type, None) {
            continue;
        }
        let Some(runtime) = entry.runtime.as_ref() else {
            continue;
        };
        let mut envelope = base_envelope.clone();
        envelope.plugin_id = entry.manifest.id.clone();
        let response = runtime.call_hook(envelope).await?;
        if let Some(output) = provider_health_check_output_from_hook_response(response) {
            return Ok(Some(output));
        }
    }
    Ok(None)
}

fn provider_config_matches_request(
    config: &ProviderPluginConfig,
    route_family: Option<&str>,
    auth_endpoint_signature: Option<&str>,
) -> bool {
    let has_provider_types = !config.provider_types.is_empty();
    let has_api_formats = !config.api_formats.is_empty();
    if !has_provider_types && !has_api_formats {
        return true;
    }

    let provider_type_matches = has_provider_types
        && route_family.is_some_and(|route_family| {
            config
                .provider_types
                .iter()
                .any(|provider_type| provider_type.trim().eq_ignore_ascii_case(route_family))
        });
    let api_format_matches = has_api_formats
        && auth_endpoint_signature.is_some_and(|auth_endpoint_signature| {
            let auth_endpoint_signature = normalize_api_format(auth_endpoint_signature);
            config
                .api_formats
                .iter()
                .any(|api_format| normalize_api_format(api_format) == auth_endpoint_signature)
        });

    provider_type_matches || api_format_matches
}

fn provider_request_rewrite_output_from_hook_response(
    response: PluginHookResponse,
) -> Option<ProviderRequestRewriteOutput> {
    match response {
        PluginHookResponse::Continue => None,
        PluginHookResponse::ReplaceRequest {
            path,
            headers,
            body_json,
            body_base64,
            metadata,
        } => Some(ProviderRequestRewriteOutput {
            path,
            headers,
            body_json,
            body_base64,
            metadata,
        }),
        PluginHookResponse::Error { .. }
        | PluginHookResponse::ReplaceResponse { .. }
        | PluginHookResponse::StreamEvents { .. } => None,
    }
}

fn provider_response_rewrite_output_from_hook_response(
    response: PluginHookResponse,
    original_status: u16,
) -> Option<ProviderResponseRewriteOutput> {
    match response {
        PluginHookResponse::Continue => None,
        PluginHookResponse::ReplaceResponse {
            status,
            headers,
            body_json,
            body_base64,
        } => Some(ProviderResponseRewriteOutput {
            status: if status == 0 { original_status } else { status },
            headers,
            body_json,
            body_base64,
        }),
        PluginHookResponse::Error { .. }
        | PluginHookResponse::ReplaceRequest { .. }
        | PluginHookResponse::StreamEvents { .. } => None,
    }
}

fn provider_stream_rewrite_output_from_hook_response(
    response: PluginHookResponse,
) -> Option<ProviderStreamRewriteOutput> {
    match response {
        PluginHookResponse::Continue => None,
        PluginHookResponse::ReplaceResponse {
            body_json,
            body_base64,
            ..
        } => {
            if let Some(body_json) = body_json {
                provider_stream_rewrite_output_from_value(body_json)
            } else {
                provider_stream_rewrite_output_from_body_base64(body_base64)
            }
        }
        PluginHookResponse::Error { .. }
        | PluginHookResponse::ReplaceRequest { .. }
        | PluginHookResponse::StreamEvents { .. } => None,
    }
}

fn provider_stream_rewrite_output_from_value(value: Value) -> Option<ProviderStreamRewriteOutput> {
    let chunk_text = value
        .get("chunk_text")
        .or_else(|| value.get("text"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let chunk_base64 = value
        .get("chunk_base64")
        .or_else(|| value.get("body_base64"))
        .or_else(|| value.get("body_bytes_b64"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    if chunk_text.is_none() && chunk_base64.is_none() {
        return None;
    }
    let metadata = value
        .get("metadata")
        .and_then(Value::as_object)
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default();
    Some(ProviderStreamRewriteOutput {
        chunk_text,
        chunk_base64,
        metadata,
    })
}

fn provider_stream_rewrite_output_from_body_base64(
    body_base64: Option<String>,
) -> Option<ProviderStreamRewriteOutput> {
    let body_base64 = body_base64?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64.as_bytes())
        .ok()?;
    if let Ok(text) = String::from_utf8(bytes) {
        return Some(ProviderStreamRewriteOutput {
            chunk_text: Some(text),
            chunk_base64: None,
            metadata: BTreeMap::new(),
        });
    }
    Some(ProviderStreamRewriteOutput {
        chunk_text: None,
        chunk_base64: Some(body_base64),
        metadata: BTreeMap::new(),
    })
}

fn provider_model_fetch_output_from_hook_response(
    response: PluginHookResponse,
) -> Option<ProviderModelFetchOutput> {
    match response {
        PluginHookResponse::Continue => None,
        PluginHookResponse::ReplaceResponse {
            body_json,
            body_base64,
            ..
        } => {
            let Some(body_json) = body_json.or_else(|| decode_hook_body_base64_json(body_base64))
            else {
                return None;
            };
            provider_model_fetch_output_from_value(body_json)
        }
        PluginHookResponse::Error { .. }
        | PluginHookResponse::ReplaceRequest { .. }
        | PluginHookResponse::StreamEvents { .. } => None,
    }
}

fn provider_health_check_output_from_hook_response(
    response: PluginHookResponse,
) -> Option<ProviderHealthCheckOutput> {
    match response {
        PluginHookResponse::Continue => None,
        PluginHookResponse::ReplaceResponse {
            body_json,
            body_base64,
            ..
        } => {
            let body_json = body_json.or_else(|| decode_hook_body_base64_json(body_base64))?;
            provider_health_check_output_from_value(body_json)
        }
        PluginHookResponse::Error {
            code,
            message,
            retryable,
        } => Some(ProviderHealthCheckOutput {
            success: false,
            message: Some(message),
            data: None,
            metadata: BTreeMap::from([
                ("code".to_string(), Value::String(code)),
                ("retryable".to_string(), Value::Bool(retryable)),
            ]),
        }),
        PluginHookResponse::ReplaceRequest { .. } | PluginHookResponse::StreamEvents { .. } => None,
    }
}

fn provider_health_check_output_from_value(value: Value) -> Option<ProviderHealthCheckOutput> {
    let success = value.get("success").and_then(Value::as_bool)?;
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let data = value.get("data").cloned();
    let metadata = value
        .get("metadata")
        .and_then(Value::as_object)
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default();
    Some(ProviderHealthCheckOutput {
        success,
        message,
        data,
        metadata,
    })
}

fn provider_model_fetch_output_from_value(value: Value) -> Option<ProviderModelFetchOutput> {
    let cached_models = value
        .get("cached_models")
        .or_else(|| value.get("models"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let fetched_model_ids = value
        .get("fetched_model_ids")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| model_ids_from_cached_models(&cached_models));
    let errors = value
        .get("errors")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let has_success = value
        .get("has_success")
        .and_then(Value::as_bool)
        .unwrap_or(!cached_models.is_empty());
    if cached_models.is_empty() && errors.is_empty() && !has_success {
        return None;
    }
    Some(ProviderModelFetchOutput {
        fetched_model_ids,
        cached_models,
        errors,
        has_success,
        upstream_metadata: value.get("upstream_metadata").cloned(),
    })
}

fn decode_hook_body_base64_json(body_base64: Option<String>) -> Option<Value> {
    let body_base64 = body_base64?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64.as_bytes())
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn model_ids_from_cached_models(models: &[Value]) -> Vec<String> {
    models
        .iter()
        .filter_map(|model| {
            model
                .get("id")
                .or_else(|| model.get("name"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(|item| item.strip_prefix("models/").unwrap_or(item).to_string())
        })
        .collect()
}

pub fn builtin_provider_plugin_runtimes() -> Vec<Arc<dyn PluginRuntime>> {
    FIXED_PROVIDER_TYPES
        .iter()
        .filter_map(|provider_type| fixed_provider_template(provider_type))
        .map(|template| {
            Arc::new(BuiltinFixedProviderPlugin::new(template)) as Arc<dyn PluginRuntime>
        })
        .collect()
}

pub fn manifest_provider_runtime(manifest: PluginManifest) -> Arc<dyn PluginRuntime> {
    Arc::new(ProviderManifestRuntime::new(manifest))
}

#[derive(Debug, Clone)]
pub struct BuiltinFixedProviderPlugin {
    manifest: PluginManifest,
}

impl BuiltinFixedProviderPlugin {
    pub fn new(template: &'static FixedProviderTemplate) -> Self {
        Self {
            manifest: fixed_provider_plugin_manifest(template),
        }
    }
}

#[async_trait]
impl PluginRuntime for BuiltinFixedProviderPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn call_hook(
        &self,
        envelope: PluginHookEnvelope,
    ) -> Result<PluginHookResponse, aether_plugin_core::PluginError> {
        if !self.manifest.has_capability(&envelope.capability) {
            return Err(aether_plugin_core::PluginError::CapabilityDenied {
                plugin_id: self.manifest.id.clone(),
                capability: envelope.capability,
            });
        }
        Ok(PluginHookResponse::Continue)
    }
}

#[derive(Debug, Clone)]
pub struct ProviderManifestRuntime {
    manifest: PluginManifest,
}

impl ProviderManifestRuntime {
    pub fn new(manifest: PluginManifest) -> Self {
        Self { manifest }
    }

    fn call_manifest_request_rewrite(&self, envelope: PluginHookEnvelope) -> PluginHookResponse {
        let Some(config) = provider_config_from_manifest(&self.manifest) else {
            return PluginHookResponse::Continue;
        };
        let Some(rewrite_config) = config.request_rewrite else {
            return PluginHookResponse::Continue;
        };
        let request = ProviderRequestRewriteInput {
            trace_id: envelope.trace_id.as_str(),
            method: envelope.method.as_deref().unwrap_or_default(),
            path: envelope.path.as_deref().unwrap_or_default(),
            query: envelope.query.clone(),
            headers: envelope.headers.clone(),
            body_json: envelope.body_json.clone(),
            body_base64: envelope.body_base64.clone(),
            route_family: context_string(&envelope.context, "route_family"),
            route_kind: context_string(&envelope.context, "route_kind"),
            auth_endpoint_signature: context_string(&envelope.context, "auth_endpoint_signature"),
        };

        let mut replacement_path = None;
        let mut replacement_headers = BTreeMap::new();
        let mut replacement_body = envelope.body_json;
        let mut body_changed = false;
        let mut matched = false;

        for rule in rewrite_config
            .rules
            .into_iter()
            .filter(|rule| rule.matches(&request))
        {
            matched = true;
            if let Some(path) = rule
                .set_path
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                replacement_path = Some(path.to_string());
            }
            replacement_headers.extend(rule.set_headers);
            if !rule.set_body_fields.is_empty() || !rule.remove_body_fields.is_empty() {
                let mut object = replacement_body
                    .take()
                    .and_then(|value| value.as_object().cloned())
                    .unwrap_or_default();
                for field in rule.remove_body_fields {
                    object.remove(field.trim());
                }
                for (field, value) in rule.set_body_fields {
                    object.insert(field, value);
                }
                replacement_body = Some(Value::Object(object));
                body_changed = true;
            }
        }

        if !matched
            || (replacement_path.is_none() && replacement_headers.is_empty() && !body_changed)
        {
            return PluginHookResponse::Continue;
        }

        PluginHookResponse::ReplaceRequest {
            path: replacement_path,
            headers: replacement_headers,
            body_json: body_changed.then_some(replacement_body).flatten(),
            body_base64: None,
            metadata: BTreeMap::new(),
        }
    }

    fn call_manifest_response_rewrite(&self, envelope: PluginHookEnvelope) -> PluginHookResponse {
        let Some(config) = provider_config_from_manifest(&self.manifest) else {
            return PluginHookResponse::Continue;
        };
        let Some(rewrite_config) = config.response_rewrite else {
            return PluginHookResponse::Continue;
        };
        let response = ProviderResponseRewriteInput {
            trace_id: envelope.trace_id.as_str(),
            method: envelope.method.as_deref().unwrap_or_default(),
            path: envelope.path.as_deref().unwrap_or_default(),
            query: envelope.query.clone(),
            status: context_u16(&envelope.context, "status").unwrap_or(200),
            headers: envelope.headers.clone(),
            body_json: envelope.body_json.clone(),
            body_base64: envelope.body_base64.clone(),
            route_family: context_string(&envelope.context, "route_family"),
            route_kind: context_string(&envelope.context, "route_kind"),
            auth_endpoint_signature: context_string(&envelope.context, "auth_endpoint_signature"),
        };

        let mut replacement_status = response.status;
        let mut replacement_headers = BTreeMap::new();
        let mut replacement_body = envelope.body_json;
        let mut body_changed = false;
        let mut status_changed = false;
        let mut matched = false;

        for rule in rewrite_config
            .rules
            .into_iter()
            .filter(|rule| rule.matches(&response))
        {
            matched = true;
            if let Some(status) = rule.set_status {
                replacement_status = status;
                status_changed = true;
            }
            replacement_headers.extend(rule.set_headers);
            if !rule.set_body_fields.is_empty() || !rule.remove_body_fields.is_empty() {
                let mut object = replacement_body
                    .take()
                    .and_then(|value| value.as_object().cloned())
                    .unwrap_or_default();
                for field in rule.remove_body_fields {
                    object.remove(field.trim());
                }
                for (field, value) in rule.set_body_fields {
                    object.insert(field, value);
                }
                replacement_body = Some(Value::Object(object));
                body_changed = true;
            }
        }

        if !matched || (replacement_headers.is_empty() && !body_changed && !status_changed) {
            return PluginHookResponse::Continue;
        }

        PluginHookResponse::ReplaceResponse {
            status: replacement_status,
            headers: replacement_headers,
            body_json: body_changed.then_some(replacement_body).flatten(),
            body_base64: None,
        }
    }

    fn call_manifest_stream_rewrite(&self, envelope: PluginHookEnvelope) -> PluginHookResponse {
        let Some(config) = provider_config_from_manifest(&self.manifest) else {
            return PluginHookResponse::Continue;
        };
        let Some(rewrite_config) = config.stream_rewrite else {
            return PluginHookResponse::Continue;
        };
        let chunk_text = manifest_stream_chunk_text(&envelope);
        let stream = ProviderStreamRewriteInput {
            trace_id: envelope.trace_id.as_str(),
            method: envelope.method.as_deref().unwrap_or_default(),
            path: envelope.path.as_deref().unwrap_or_default(),
            query: envelope.query.clone(),
            headers: envelope.headers.clone(),
            chunk_text,
            chunk_base64: manifest_stream_chunk_base64(&envelope),
            route_family: context_string(&envelope.context, "route_family"),
            route_kind: context_string(&envelope.context, "route_kind"),
            auth_endpoint_signature: context_string(&envelope.context, "auth_endpoint_signature"),
        };

        let Some(mut rewritten_text) = stream.chunk_text.clone() else {
            return PluginHookResponse::Continue;
        };
        let mut rules_applied = 0_u64;
        for rule in rewrite_config.rules {
            let mut current_stream = stream.clone();
            current_stream.chunk_text = Some(rewritten_text.clone());
            if !rule.matches(&current_stream) {
                continue;
            }
            let next_text = if let Some(needle) = rule
                .contains
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                if rule.replace_all {
                    rewritten_text.replace(needle, &rule.replace_with)
                } else {
                    rewritten_text.replacen(needle, &rule.replace_with, 1)
                }
            } else {
                rule.replace_with
            };
            if next_text != rewritten_text {
                rules_applied += 1;
                rewritten_text = next_text;
            }
        }

        if rules_applied == 0 {
            return PluginHookResponse::Continue;
        }

        PluginHookResponse::ReplaceResponse {
            status: 200,
            headers: BTreeMap::new(),
            body_json: Some(json!({
                "chunk_text": rewritten_text,
                "metadata": {
                    "runtime": "manifest",
                    "rules_applied": rules_applied,
                }
            })),
            body_base64: None,
        }
    }

    fn call_manifest_model_fetch(&self, _envelope: PluginHookEnvelope) -> PluginHookResponse {
        let Some(config) = provider_config_from_manifest(&self.manifest) else {
            return PluginHookResponse::Continue;
        };
        let Some(model_fetch) = config.model_fetch else {
            return PluginHookResponse::Continue;
        };
        if !model_fetch.supported || model_fetch.models.is_empty() {
            return PluginHookResponse::Continue;
        }
        let fetched_model_ids = model_ids_from_cached_models(&model_fetch.models);
        PluginHookResponse::ReplaceResponse {
            status: 200,
            headers: BTreeMap::new(),
            body_json: Some(json!({
                "fetched_model_ids": fetched_model_ids,
                "cached_models": model_fetch.models,
                "errors": [],
                "has_success": true,
                "upstream_metadata": model_fetch.upstream_metadata,
            })),
            body_base64: None,
        }
    }

    fn call_manifest_health_check(&self, envelope: PluginHookEnvelope) -> PluginHookResponse {
        let Some(config) = provider_config_from_manifest(&self.manifest) else {
            return PluginHookResponse::Continue;
        };
        let default_data = json!({
            "provider_type": context_string(&envelope.context, "provider_type")
                .or_else(|| config.provider_types.first().cloned()),
            "base_url": context_string(&envelope.context, "base_url")
                .or_else(|| config.base_url.clone()),
            "plugin_id": self.manifest.id,
        });
        let (success, message, data, metadata) = if let Some(health_check) = config.health_check {
            let mut metadata = health_check.metadata;
            metadata.insert("runtime".to_string(), json!("manifest"));
            (
                health_check.success,
                health_check.message,
                health_check.data.unwrap_or(default_data),
                metadata,
            )
        } else {
            (
                true,
                None,
                default_data,
                BTreeMap::from([("runtime".to_string(), json!("manifest"))]),
            )
        };
        PluginHookResponse::ReplaceResponse {
            status: 200,
            headers: BTreeMap::new(),
            body_json: Some(json!({
                "success": success,
                "message": message,
                "data": data,
                "metadata": metadata
            })),
            body_base64: None,
        }
    }
}

#[async_trait]
impl PluginRuntime for ProviderManifestRuntime {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn call_hook(
        &self,
        envelope: PluginHookEnvelope,
    ) -> Result<PluginHookResponse, aether_plugin_core::PluginError> {
        if !self.manifest.has_capability(&envelope.capability) {
            return Err(aether_plugin_core::PluginError::CapabilityDenied {
                plugin_id: self.manifest.id.clone(),
                capability: envelope.capability,
            });
        }
        if envelope.capability.as_str() == CAP_PROVIDER_REQUEST_REWRITE {
            return Ok(self.call_manifest_request_rewrite(envelope));
        }
        if envelope.capability.as_str() == CAP_PROVIDER_RESPONSE_REWRITE {
            return Ok(self.call_manifest_response_rewrite(envelope));
        }
        if envelope.capability.as_str() == CAP_PROVIDER_STREAM_REWRITE {
            return Ok(self.call_manifest_stream_rewrite(envelope));
        }
        if envelope.capability.as_str() == CAP_PROVIDER_MODEL_FETCH {
            return Ok(self.call_manifest_model_fetch(envelope));
        }
        if envelope.capability.as_str() == CAP_PROVIDER_HEALTH_CHECK {
            return Ok(self.call_manifest_health_check(envelope));
        }
        Ok(PluginHookResponse::Continue)
    }
}

fn fixed_provider_plugin_manifest(template: &'static FixedProviderTemplate) -> PluginManifest {
    let provider_type = template.provider_type;
    let policy = provider_runtime_policy(provider_type);
    let mut capabilities = BTreeSet::from([
        provider_capability(CAP_PROVIDER_ROUTE_ALIAS),
        provider_capability(CAP_PROVIDER_REQUEST_REWRITE),
        provider_capability(CAP_PROVIDER_RESPONSE_REWRITE),
        provider_capability(CAP_PROVIDER_STREAM_REWRITE),
        provider_capability(CAP_PROVIDER_HEALTH_CHECK),
    ]);
    if policy.supports_model_fetch {
        capabilities.insert(provider_capability(CAP_PROVIDER_MODEL_FETCH));
    }
    if provider_type_admin_oauth_template(provider_type).is_some() {
        capabilities.insert(provider_capability(CAP_PROVIDER_OAUTH_FLOW));
    }
    if matches!(
        provider_type,
        "codex" | "kiro" | "antigravity" | "chatgpt_web"
    ) {
        capabilities.insert(provider_capability(CAP_PROVIDER_MANAGEMENT_PROXY));
    }
    if matches!(provider_type, "amp" | "antigravity") {
        capabilities.insert(provider_capability(CAP_PROVIDER_WEBSOCKET_PROXY));
    }

    let provider_config = fixed_provider_config(template, policy);
    let mut domains = BTreeMap::new();
    domains.insert(
        PROVIDER_DOMAIN.to_string(),
        serde_json::to_value(provider_config)
            .expect("builtin fixed provider config should serialize"),
    );

    PluginManifest {
        id: format!("builtin.provider.{provider_type}"),
        name: format!("Builtin {provider_type} provider"),
        version: template.version.to_string(),
        api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
        runtime: PluginRuntimeManifest {
            kind: PluginRuntimeKind::Builtin,
            entry: None,
            command: None,
            endpoint: None,
            timeout_ms: Some(5_000),
        },
        capabilities,
        enabled: true,
        description: Some(format!(
            "Builtin compatibility provider plugin for {provider_type}"
        )),
        domains,
    }
}

fn fixed_provider_config(
    template: &'static FixedProviderTemplate,
    policy: ProviderRuntimePolicy,
) -> ProviderPluginConfig {
    let provider_type = template.provider_type;
    ProviderPluginConfig {
        provider_types: vec![provider_type.to_string()],
        api_formats: template
            .endpoints
            .iter()
            .map(|endpoint| endpoint.api_format.to_string())
            .collect(),
        base_url: Some(template.base_url.to_string()),
        endpoints: template
            .endpoints
            .iter()
            .map(|endpoint| ProviderPluginEndpointConfig {
                item_key: endpoint.item_key.to_string(),
                api_format: endpoint.api_format.to_string(),
                custom_path: endpoint.custom_path.map(str::to_string),
                config_defaults: endpoint
                    .config_defaults
                    .iter()
                    .map(|item| (item.key.to_string(), fixed_config_value_to_json(item.value)))
                    .collect(),
            })
            .collect(),
        route_aliases: Vec::new(),
        request_rewrite: None,
        response_rewrite: None,
        stream_rewrite: None,
        runtime_policy: Some(runtime_policy_config(policy)),
        auth: Some(ProviderPluginAuthConfig {
            oauth_template: provider_type_admin_oauth_template(provider_type).map(|template| {
                ProviderPluginOAuthTemplate {
                    provider_type: template.provider_type.to_string(),
                    display_name: template.display_name.to_string(),
                    authorize_url: template.authorize_url.to_string(),
                    token_url: template.token_url.to_string(),
                    client_id: template.client_id.to_string(),
                    client_secret: (!template.client_secret.is_empty())
                        .then(|| template.client_secret.to_string()),
                    scopes: template
                        .scopes
                        .iter()
                        .map(|scope| scope.to_string())
                        .collect(),
                    redirect_uri: template.redirect_uri.to_string(),
                    use_pkce: template.use_pkce,
                }
            }),
        }),
        model_fetch: Some(ProviderPluginModelFetchConfig {
            supported: policy.supports_model_fetch,
            models: Vec::new(),
            upstream_metadata: None,
        }),
        health_check: None,
    }
}

fn normalize_path(path: &str) -> String {
    let path = path.trim();
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let path = path.trim_end_matches('/').to_string();
    if path.is_empty() {
        "/".to_string()
    } else {
        path
    }
}

fn normalize_api_format(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn option_matches(expected: Option<&str>, actual: Option<&str>, case_insensitive: bool) -> bool {
    let Some(expected) = expected.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    let Some(actual) = actual.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    if case_insensitive {
        expected.eq_ignore_ascii_case(actual)
    } else {
        normalize_path(expected) == normalize_path(actual)
    }
}

fn context_string(context: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    context
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn context_u16(context: &BTreeMap<String, Value>, key: &str) -> Option<u16> {
    context
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
}

fn manifest_stream_chunk_text(envelope: &PluginHookEnvelope) -> Option<String> {
    envelope
        .body_json
        .as_ref()
        .and_then(|value| {
            value
                .get("chunk_text")
                .or_else(|| value.get("text"))
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned)
        .or_else(|| {
            envelope
                .body_base64
                .as_deref()
                .and_then(|body_base64| {
                    base64::engine::general_purpose::STANDARD
                        .decode(body_base64.as_bytes())
                        .ok()
                })
                .and_then(|bytes| String::from_utf8(bytes).ok())
        })
}

fn manifest_stream_chunk_base64(envelope: &PluginHookEnvelope) -> Option<String> {
    envelope
        .body_json
        .as_ref()
        .and_then(|value| {
            value
                .get("chunk_base64")
                .or_else(|| value.get("body_base64"))
                .or_else(|| value.get("body_bytes_b64"))
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned)
        .or_else(|| envelope.body_base64.clone())
}

fn fixed_config_value_to_json(value: FixedProviderEndpointConfigValue) -> Value {
    match value {
        FixedProviderEndpointConfigValue::String(value) => json!(value),
        FixedProviderEndpointConfigValue::Bool(value) => json!(value),
        FixedProviderEndpointConfigValue::I64(value) => json!(value),
    }
}

fn runtime_policy_config(policy: ProviderRuntimePolicy) -> ProviderPluginRuntimePolicy {
    ProviderPluginRuntimePolicy {
        fixed_provider: policy.fixed_provider,
        api_format_inheritance: api_format_inheritance_label(policy.api_format_inheritance)
            .to_string(),
        enable_format_conversion_by_default: policy.enable_format_conversion_by_default,
        allow_auth_channel_mismatch_by_default: policy.allow_auth_channel_mismatch_by_default,
        oauth_is_bearer_like: policy.oauth_is_bearer_like,
        supports_model_fetch: policy.supports_model_fetch,
        supports_local_openai_chat_transport: policy.supports_local_openai_chat_transport,
        supports_local_same_format_transport: policy.supports_local_same_format_transport,
        local_embedding_support: local_embedding_support_label(policy.local_embedding_support)
            .to_string(),
    }
}

fn api_format_inheritance_label(value: ProviderApiFormatInheritance) -> &'static str {
    match value {
        ProviderApiFormatInheritance::None => "none",
        ProviderApiFormatInheritance::OAuth => "oauth",
        ProviderApiFormatInheritance::OAuthOrBearer => "oauth_or_bearer",
        ProviderApiFormatInheritance::OAuthOrConfiguredBearer => "oauth_or_configured_bearer",
    }
}

fn local_embedding_support_label(value: ProviderLocalEmbeddingSupport) -> &'static str {
    match value {
        ProviderLocalEmbeddingSupport::None => "none",
        ProviderLocalEmbeddingSupport::AnyKnown => "any_known",
        ProviderLocalEmbeddingSupport::OpenAi => "openai",
        ProviderLocalEmbeddingSupport::Gemini => "gemini",
        ProviderLocalEmbeddingSupport::Jina => "jina",
        ProviderLocalEmbeddingSupport::Doubao => "doubao",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_fixed_provider_plugins_include_codex() {
        let mut registry = PluginRegistry::new();
        for runtime in builtin_provider_plugin_runtimes() {
            registry
                .register_builtin(runtime)
                .expect("builtin provider should register");
        }
        let view = provider_plugin_view(&registry, "builtin.provider.codex")
            .expect("codex builtin plugin should exist");
        assert!(view.enabled);
        assert_eq!(view.runtime, PluginRuntimeKind::Builtin);
        assert!(view
            .capabilities
            .iter()
            .any(|capability| capability == CAP_PROVIDER_OAUTH_FLOW));
        let provider = view.provider.expect("provider config should exist");
        assert_eq!(provider.provider_types, vec!["codex"]);
        assert!(provider
            .api_formats
            .iter()
            .any(|api_format| api_format == "openai:responses"));
    }

    #[test]
    fn provider_plugin_view_redacts_oauth_client_secret() {
        let mut registry = PluginRegistry::new();
        for runtime in builtin_provider_plugin_runtimes() {
            registry
                .register_builtin(runtime)
                .expect("builtin provider should register");
        }

        let raw = provider_config_for_type(&registry, "gemini_cli")
            .expect("gemini builtin provider plugin config should exist");
        assert!(raw
            .auth
            .as_ref()
            .and_then(|auth| auth.oauth_template.as_ref())
            .and_then(|template| template.client_secret.as_ref())
            .is_some());

        let view = provider_plugin_view(&registry, "builtin.provider.gemini_cli")
            .expect("gemini builtin provider plugin should exist");
        assert_eq!(
            view.provider
                .and_then(|provider| provider.auth)
                .and_then(|auth| auth.oauth_template)
                .and_then(|template| template.client_secret),
            None
        );
    }

    #[test]
    fn resolves_manifest_provider_route_alias() {
        let manifest = PluginManifest {
            id: "local.provider.test".to_string(),
            name: "Test Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_ROUTE_ALIAS)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "route_aliases": [{
                        "method": "POST",
                        "path": "/v1/test/chat",
                        "route_family": "test",
                        "route_kind": "chat",
                        "auth_endpoint_signature": "test:chat"
                    }]
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let alias = resolve_provider_route_alias(&registry, "post", "/v1/test/chat/")
            .expect("route alias should resolve");
        assert_eq!(alias.route_family, "test");
        assert!(alias.execution_runtime_candidate);
    }

    #[test]
    fn resolves_manifest_provider_oauth_template() {
        let manifest = PluginManifest {
            id: "local.provider.oauth".to_string(),
            name: "OAuth Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_OAUTH_FLOW)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test_oauth"],
                    "api_formats": ["test:chat"],
                    "auth": {
                        "oauth_template": {
                            "provider_type": "test_oauth",
                            "display_name": "Test OAuth",
                            "authorize_url": "https://auth.example.test/authorize",
                            "token_url": "https://auth.example.test/token",
                            "client_id": "client-test",
                            "scopes": ["profile", "offline_access"],
                            "redirect_uri": "http://localhost:9999/callback",
                            "use_pkce": true
                        }
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest,
            aether_plugin_core::PluginSource::Local,
            None,
            None,
        );

        let template = provider_oauth_template_for_type(&registry, "test_oauth")
            .expect("oauth template should resolve");
        assert_eq!(template.display_name, "Test OAuth");
        assert_eq!(template.client_secret, None);
        assert_eq!(
            template.scopes,
            vec!["profile".to_string(), "offline_access".to_string()]
        );
        assert_eq!(provider_oauth_templates(&registry).len(), 1);
    }

    #[test]
    fn ignores_manifest_provider_oauth_template_without_capability() {
        let manifest = PluginManifest {
            id: "local.provider.oauth.disabled".to_string(),
            name: "OAuth Provider Without Capability".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::new(),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test_oauth"],
                    "api_formats": ["test:chat"],
                    "auth": {
                        "oauth_template": {
                            "provider_type": "test_oauth",
                            "display_name": "Test OAuth",
                            "authorize_url": "https://auth.example.test/authorize",
                            "token_url": "https://auth.example.test/token",
                            "client_id": "client-test",
                            "redirect_uri": "http://localhost:9999/callback",
                            "use_pkce": false
                        }
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest,
            aether_plugin_core::PluginSource::Local,
            None,
            None,
        );

        assert!(provider_oauth_template_for_type(&registry, "test_oauth").is_none());
        assert!(provider_oauth_templates(&registry).is_empty());
    }

    #[tokio::test]
    async fn manifest_provider_request_rewrite_sets_headers_and_body_fields() {
        let manifest = PluginManifest {
            id: "local.provider.rewrite".to_string(),
            name: "Rewrite Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_REQUEST_REWRITE)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "request_rewrite": {
                        "rules": [{
                            "route_family": "test",
                            "route_kind": "chat",
                            "set_path": "/v1/rewritten",
                            "set_headers": {
                                "x-provider-plugin": "rewrite"
                            },
                            "set_body_fields": {
                                "model": "plugin-model",
                                "temperature": 0
                            },
                            "remove_body_fields": ["legacy"]
                        }]
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let output = rewrite_provider_request(
            &registry,
            ProviderRequestRewriteInput {
                trace_id: "trace",
                method: "POST",
                path: "/v1/chat",
                query: None,
                headers: BTreeMap::new(),
                body_json: Some(json!({
                    "model": "client-model",
                    "legacy": true,
                    "messages": []
                })),
                body_base64: None,
                route_family: Some("test".to_string()),
                route_kind: Some("chat".to_string()),
                auth_endpoint_signature: Some("test:chat".to_string()),
            },
        )
        .await
        .expect("request rewrite should not fail")
        .expect("request rewrite should apply");

        assert_eq!(output.path.as_deref(), Some("/v1/rewritten"));
        assert_eq!(
            output.headers.get("x-provider-plugin").map(String::as_str),
            Some("rewrite")
        );
        let body = output.body_json.expect("body should be rewritten");
        assert_eq!(body["model"], "plugin-model");
        assert_eq!(body["temperature"], 0);
        assert!(body.get("legacy").is_none());
        assert_eq!(body["messages"], json!([]));
    }

    #[tokio::test]
    async fn manifest_provider_response_rewrite_sets_status_headers_and_body_fields() {
        let manifest = PluginManifest {
            id: "local.provider.response".to_string(),
            name: "Response Rewrite Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_RESPONSE_REWRITE)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "response_rewrite": {
                        "rules": [{
                            "route_family": "test",
                            "route_kind": "chat",
                            "status": 200,
                            "set_status": 201,
                            "set_headers": {
                                "x-provider-plugin": "response"
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
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let output = rewrite_provider_response(
            &registry,
            ProviderResponseRewriteInput {
                trace_id: "trace",
                method: "POST",
                path: "/v1/chat",
                query: None,
                status: 200,
                headers: BTreeMap::new(),
                body_json: Some(json!({
                    "id": "chatcmpl-test",
                    "debug": true
                })),
                body_base64: None,
                route_family: Some("test".to_string()),
                route_kind: Some("chat".to_string()),
                auth_endpoint_signature: Some("test:chat".to_string()),
            },
        )
        .await
        .expect("response rewrite should not fail")
        .expect("response rewrite should apply");

        assert_eq!(output.status, 201);
        assert_eq!(
            output.headers.get("x-provider-plugin").map(String::as_str),
            Some("response")
        );
        let body = output.body_json.expect("body should be rewritten");
        assert_eq!(body["provider"], "plugin");
        assert_eq!(body["id"], "chatcmpl-test");
        assert!(body.get("debug").is_none());
    }

    #[tokio::test]
    async fn manifest_provider_stream_rewrite_replaces_chunk_text() {
        let manifest = PluginManifest {
            id: "local.provider.stream".to_string(),
            name: "Stream Rewrite Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_STREAM_REWRITE)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
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
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let output = rewrite_provider_stream(
            &registry,
            ProviderStreamRewriteInput {
                trace_id: "trace",
                method: "POST",
                path: "/v1/chat",
                query: None,
                headers: BTreeMap::new(),
                chunk_text: Some("data: upstream\n\n".to_string()),
                chunk_base64: None,
                route_family: Some("test".to_string()),
                route_kind: Some("chat".to_string()),
                auth_endpoint_signature: Some("test:chat".to_string()),
            },
        )
        .await
        .expect("stream rewrite should not fail")
        .expect("stream rewrite should apply");

        assert_eq!(output.chunk_text.as_deref(), Some("data: plugin\n\n"));
        assert_eq!(
            output.metadata.get("rules_applied").and_then(Value::as_u64),
            Some(1)
        );
    }

    #[tokio::test]
    async fn manifest_provider_model_fetch_returns_static_models() {
        let manifest = PluginManifest {
            id: "local.provider.models".to_string(),
            name: "Models Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_MODEL_FETCH)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "model_fetch": {
                        "supported": true,
                        "models": [{
                            "id": "test-model",
                            "owned_by": "plugin",
                            "api_format": "test:chat"
                        }],
                        "upstream_metadata": {
                            "plugin": {
                                "source": "manifest"
                            }
                        }
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let output = fetch_provider_models(
            &registry,
            ProviderModelFetchInput {
                trace_id: "trace",
                provider_id: "provider-test",
                provider_type: "test",
                endpoint_id: Some("endpoint-test"),
                key_id: Some("key-test"),
                api_format: Some("test:chat"),
                base_url: Some("https://example.test"),
                auth_type: Some("api_key"),
            },
        )
        .await
        .expect("model fetch should not fail")
        .expect("model fetch should apply");

        assert_eq!(output.fetched_model_ids, vec!["test-model"]);
        assert!(output.has_success);
        assert_eq!(output.cached_models[0]["owned_by"], "plugin");
        assert_eq!(
            output
                .upstream_metadata
                .as_ref()
                .and_then(|value| value.get("plugin"))
                .and_then(|value| value.get("source")),
            Some(&json!("manifest"))
        );
    }

    #[tokio::test]
    async fn manifest_provider_health_check_returns_success_payload() {
        let manifest = PluginManifest {
            id: "local.provider.health".to_string(),
            name: "Health Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_HEALTH_CHECK)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "base_url": "https://health.example.test"
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let output = check_provider_health(
            &registry,
            ProviderHealthCheckInput {
                trace_id: "trace",
                provider_id: Some("provider-test"),
                provider_type: Some("test"),
                base_url: Some("https://override.example.test"),
                architecture_id: Some("plugin"),
                config: json!({}),
                credentials: json!({}),
            },
        )
        .await
        .expect("health check should not fail")
        .expect("health check should apply");

        assert!(output.success);
        assert_eq!(
            output
                .data
                .as_ref()
                .and_then(|value| value.get("plugin_id")),
            Some(&json!("local.provider.health"))
        );
    }

    #[tokio::test]
    async fn manifest_provider_health_check_uses_declarative_config() {
        let manifest = PluginManifest {
            id: "local.provider.health.config".to_string(),
            name: "Configured Health Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_HEALTH_CHECK)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "base_url": "https://health.example.test",
                    "health_check": {
                        "success": false,
                        "message": "maintenance",
                        "data": {
                            "code": "maintenance"
                        },
                        "metadata": {
                            "source": "manifest"
                        }
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let output = check_provider_health(
            &registry,
            ProviderHealthCheckInput {
                trace_id: "trace",
                provider_id: Some("provider-test"),
                provider_type: Some("test"),
                base_url: Some("https://override.example.test"),
                architecture_id: Some("plugin"),
                config: json!({}),
                credentials: json!({}),
            },
        )
        .await
        .expect("health check should not fail")
        .expect("health check should apply");

        assert!(!output.success);
        assert_eq!(output.message.as_deref(), Some("maintenance"));
        assert_eq!(
            output.data.as_ref().and_then(|value| value.get("code")),
            Some(&json!("maintenance"))
        );
        assert_eq!(output.metadata.get("source"), Some(&json!("manifest")));
        assert_eq!(output.metadata.get("runtime"), Some(&json!("manifest")));
    }

    #[test]
    fn provider_runtime_policy_prefers_enabled_plugin_config() {
        let manifest = PluginManifest {
            id: "local.provider.policy".to_string(),
            name: "Policy Provider".to_string(),
            version: "1".to_string(),
            api_version: aether_plugin_core::PLUGIN_API_VERSION_V1.to_string(),
            runtime: aether_plugin_core::PluginRuntimeManifest {
                kind: PluginRuntimeKind::Manifest,
                entry: None,
                command: None,
                endpoint: None,
                timeout_ms: None,
            },
            capabilities: BTreeSet::from([provider_capability(CAP_PROVIDER_MODEL_FETCH)]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["plugin_provider"],
                    "api_formats": ["plugin:chat"],
                    "runtime_policy": {
                        "fixed_provider": false,
                        "api_format_inheritance": "oauth",
                        "enable_format_conversion_by_default": true,
                        "allow_auth_channel_mismatch_by_default": false,
                        "oauth_is_bearer_like": true,
                        "supports_model_fetch": false,
                        "supports_local_openai_chat_transport": false,
                        "supports_local_same_format_transport": true,
                        "local_embedding_support": "none"
                    }
                }),
            )]),
        };
        let mut registry = PluginRegistry::new();
        registry.register_manifest(
            manifest.clone(),
            aether_plugin_core::PluginSource::Local,
            Some(manifest_provider_runtime(manifest)),
            None,
        );

        let policy = provider_runtime_policy_for_type(&registry, "plugin_provider");
        assert!(policy.enable_format_conversion_by_default);
        assert!(policy.oauth_is_bearer_like);
        assert!(!policy.supports_model_fetch);
        assert!(provider_config_for_api_format(&registry, "PLUGIN:CHAT").is_some());
    }
}
