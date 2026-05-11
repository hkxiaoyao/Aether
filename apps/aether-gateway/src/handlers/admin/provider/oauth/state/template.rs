use super::super::errors::build_internal_control_error_response;
use crate::handlers::admin::provider::shared::support::ADMIN_PROVIDER_OAUTH_DATA_UNAVAILABLE_DETAIL;
use crate::handlers::admin::request::{
    admin_provider_oauth_template as request_admin_provider_oauth_template,
    admin_provider_oauth_template_types,
    is_fixed_provider_type_for_admin_oauth as request_is_fixed_provider_type_for_admin_oauth,
    AdminAppState, AdminProviderOAuthTemplate,
};
use axum::{body::Body, http, response::Response};
use serde_json::json;
use std::collections::BTreeSet;

pub(crate) fn is_fixed_provider_type_for_provider_oauth(
    state: &AdminAppState<'_>,
    provider_type: &str,
) -> bool {
    admin_provider_oauth_template(state, provider_type).is_some()
        || request_is_fixed_provider_type_for_admin_oauth(provider_type)
}

pub(crate) fn admin_provider_oauth_template(
    state: &AdminAppState<'_>,
    provider_type: &str,
) -> Option<AdminProviderOAuthTemplate> {
    state
        .app()
        .plugins
        .provider_oauth_template_for_type(provider_type)
        .map(AdminProviderOAuthTemplate::from_provider_plugin)
        .or_else(|| request_admin_provider_oauth_template(provider_type))
}

pub(crate) fn build_admin_provider_oauth_supported_types_payload(
    state: &AdminAppState<'_>,
) -> Vec<serde_json::Value> {
    let mut seen = BTreeSet::new();
    let mut templates = Vec::new();

    for template in state.app().plugins.provider_oauth_templates() {
        let template = AdminProviderOAuthTemplate::from_provider_plugin(template);
        if seen.insert(template.provider_type.to_ascii_lowercase()) {
            templates.push(template);
        }
    }

    for provider_type in admin_provider_oauth_template_types() {
        if seen.contains(provider_type) {
            continue;
        }
        if let Some(template) = request_admin_provider_oauth_template(provider_type) {
            seen.insert(provider_type.to_ascii_lowercase());
            templates.push(template);
        }
    }

    templates
        .into_iter()
        .map(admin_provider_oauth_template_payload)
        .collect()
}

fn admin_provider_oauth_template_payload(
    template: AdminProviderOAuthTemplate,
) -> serde_json::Value {
    json!({
        "provider_type": template.provider_type,
        "display_name": template.display_name,
        "scopes": template.scopes,
        "redirect_uri": template.redirect_uri,
        "authorize_url": template.authorize_url,
        "token_url": template.token_url,
        "use_pkce": template.use_pkce,
    })
}

pub(crate) fn build_admin_provider_oauth_backend_unavailable_response() -> Response<Body> {
    build_internal_control_error_response(
        http::StatusCode::SERVICE_UNAVAILABLE,
        ADMIN_PROVIDER_OAUTH_DATA_UNAVAILABLE_DETAIL,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{plugins::GatewayPluginRegistry, AppState};
    use aether_plugin_core::{PluginCapability, PluginManifest, PluginRuntimeKind};
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn supported_types_payload_includes_provider_plugin_oauth_templates() {
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
            capabilities: BTreeSet::from([PluginCapability::new(
                aether_provider_plugin::CAP_PROVIDER_OAUTH_FLOW,
            )
            .expect("capability should be valid")]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                aether_provider_plugin::PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["plugin_oauth"],
                    "api_formats": ["plugin:chat"],
                    "auth": {
                        "oauth_template": {
                            "provider_type": "plugin_oauth",
                            "display_name": "Plugin OAuth",
                            "authorize_url": "https://auth.plugin.test/authorize",
                            "token_url": "https://auth.plugin.test/token",
                            "client_id": "plugin-client",
                            "client_secret": "plugin-secret",
                            "scopes": ["profile"],
                            "redirect_uri": "http://localhost:19999/callback",
                            "use_pkce": true
                        }
                    }
                }),
            )]),
        };
        let mut registry = aether_plugin_core::PluginRegistry::new();
        registry.register_manifest(
            manifest,
            aether_plugin_core::PluginSource::Local,
            None,
            None,
        );
        let app = AppState::new()
            .expect("app state should build")
            .with_plugin_registry_for_tests(GatewayPluginRegistry::from_registry(registry));
        let admin_state = AdminAppState::new(&app);

        assert!(is_fixed_provider_type_for_provider_oauth(
            &admin_state,
            "plugin_oauth"
        ));
        let template = admin_provider_oauth_template(&admin_state, "plugin_oauth")
            .expect("plugin oauth template should resolve");
        assert_eq!(template.client_id, "plugin-client");
        assert_eq!(template.client_secret, "plugin-secret");

        let payload = build_admin_provider_oauth_supported_types_payload(&admin_state);
        let plugin_item = payload
            .iter()
            .find(|item| item["provider_type"] == "plugin_oauth")
            .expect("plugin oauth item should be listed");
        assert_eq!(plugin_item["display_name"], "Plugin OAuth");
        assert_eq!(
            plugin_item["authorize_url"],
            "https://auth.plugin.test/authorize"
        );
    }
}
