use aether_plugin_core::{
    load_local_plugin_manifests, plugin_dir_from_env, runtime_for_local_manifest, PluginManifest,
    PluginRegistry, PluginRuntime, PluginRuntimeKind, PluginSource,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Clone)]
pub(crate) struct GatewayPluginRegistry {
    registry: PluginRegistry,
    local_dir: PathBuf,
}

impl GatewayPluginRegistry {
    pub(crate) fn load_default() -> Self {
        let local_dir = plugin_dir_from_env();
        let mut registry = PluginRegistry::new();
        for runtime in aether_provider_plugin::builtin_provider_plugin_runtimes() {
            if let Err(err) = registry.register_builtin(runtime) {
                warn!(error = %err, "gateway failed to register builtin provider plugin");
            }
        }

        for manifest_result in load_local_plugin_manifests(&local_dir) {
            match manifest_result {
                Ok(manifest) => {
                    let (runtime, load_error) =
                        runtime_for_gateway_local_manifest(manifest.clone());
                    registry.register_manifest(manifest, PluginSource::Local, runtime, load_error);
                }
                Err(err) => {
                    warn!(error = %err, "gateway failed to load local plugin manifest");
                }
            }
        }

        Self {
            registry,
            local_dir,
        }
    }

    pub(crate) fn registry(&self) -> &PluginRegistry {
        &self.registry
    }

    #[cfg(test)]
    pub(crate) fn from_registry(registry: PluginRegistry) -> Self {
        Self {
            registry,
            local_dir: PathBuf::from(aether_plugin_core::DEFAULT_PLUGIN_DIR),
        }
    }

    pub(crate) fn provider_plugins_payload(&self) -> Value {
        let items = aether_provider_plugin::provider_plugin_views(&self.registry);
        json!({
            "items": items,
            "plugin_dir": self.local_dir.to_string_lossy(),
            "capability_namespaces": ["provider"],
            "provider_capabilities": aether_provider_plugin::all_provider_capabilities()
                .into_iter()
                .map(|capability| capability.as_str().to_string())
                .collect::<Vec<_>>(),
        })
    }

    pub(crate) fn provider_plugin_payload(&self, plugin_id: &str) -> Option<Value> {
        let item = aether_provider_plugin::provider_plugin_view(&self.registry, plugin_id)?;
        Some(json!({ "item": item }))
    }

    pub(crate) fn provider_config_for_type(
        &self,
        provider_type: &str,
    ) -> Option<aether_provider_plugin::ProviderPluginConfig> {
        aether_provider_plugin::provider_config_for_type(&self.registry, provider_type)
    }

    pub(crate) fn provider_oauth_templates(
        &self,
    ) -> Vec<aether_provider_plugin::ProviderPluginOAuthTemplate> {
        aether_provider_plugin::provider_oauth_templates(&self.registry)
    }

    pub(crate) fn provider_oauth_template_for_type(
        &self,
        provider_type: &str,
    ) -> Option<aether_provider_plugin::ProviderPluginOAuthTemplate> {
        aether_provider_plugin::provider_oauth_template_for_type(&self.registry, provider_type)
    }

    pub(crate) fn provider_runtime_policy_for_type(
        &self,
        provider_type: &str,
    ) -> aether_provider_transport::provider_types::ProviderRuntimePolicy {
        aether_provider_plugin::provider_runtime_policy_for_type(&self.registry, provider_type)
    }

    pub(crate) fn provider_type_enables_format_conversion_by_default(
        &self,
        provider_type: &str,
    ) -> bool {
        aether_provider_plugin::provider_type_enables_format_conversion_by_default(
            &self.registry,
            provider_type,
        )
    }
}

fn runtime_for_gateway_local_manifest(
    manifest: PluginManifest,
) -> (Option<Arc<dyn PluginRuntime>>, Option<String>) {
    if manifest.runtime.kind == PluginRuntimeKind::Manifest
        && aether_provider_plugin::provider_config_from_manifest(&manifest).is_some()
    {
        return (
            Some(aether_provider_plugin::manifest_provider_runtime(manifest)),
            None,
        );
    }
    runtime_for_local_manifest(manifest)
}

#[cfg(test)]
mod tests {
    use super::{runtime_for_gateway_local_manifest, GatewayPluginRegistry};
    use aether_plugin_core::{PluginCapability, PluginManifest, PluginRuntimeKind};
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn default_registry_exposes_builtin_provider_plugins() {
        let registry = GatewayPluginRegistry::load_default();
        let payload = registry
            .provider_plugin_payload("builtin.provider.codex")
            .expect("codex builtin provider plugin should be present");
        assert_eq!(payload["item"]["id"], "builtin.provider.codex");
        assert_eq!(payload["item"]["runtime"], "builtin");
        assert_eq!(payload["item"]["provider"]["provider_types"][0], "codex");
    }

    #[tokio::test]
    async fn local_provider_manifest_uses_provider_runtime_for_declarative_hooks() {
        let manifest = PluginManifest {
            id: "local.provider.runtime".to_string(),
            name: "Local Provider Runtime".to_string(),
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
                aether_provider_plugin::CAP_PROVIDER_REQUEST_REWRITE,
            )
            .expect("capability should be valid")]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([(
                aether_provider_plugin::PROVIDER_DOMAIN.to_string(),
                json!({
                    "provider_types": ["test"],
                    "api_formats": ["test:chat"],
                    "request_rewrite": {
                        "rules": [{
                            "route_family": "test",
                            "set_body_fields": {
                                "model": "plugin-model"
                            }
                        }]
                    }
                }),
            )]),
        };

        let (runtime, load_error) = runtime_for_gateway_local_manifest(manifest);
        assert!(load_error.is_none());
        let runtime = runtime.expect("provider manifest should create runtime");
        let response = runtime
            .call_hook(aether_plugin_core::PluginHookEnvelope {
                plugin_id: "local.provider.runtime".to_string(),
                trace_id: "trace".to_string(),
                capability: PluginCapability::new(
                    aether_provider_plugin::CAP_PROVIDER_REQUEST_REWRITE,
                )
                .expect("capability should be valid"),
                hook: aether_provider_plugin::CAP_PROVIDER_REQUEST_REWRITE.to_string(),
                method: Some("POST".to_string()),
                path: Some("/v1/chat".to_string()),
                query: None,
                headers: BTreeMap::new(),
                body_json: Some(json!({"model": "client-model"})),
                body_base64: None,
                context: BTreeMap::from([("route_family".to_string(), json!("test"))]),
            })
            .await
            .expect("hook should execute");

        match response {
            aether_plugin_core::PluginHookResponse::ReplaceRequest { body_json, .. } => {
                assert_eq!(
                    body_json.expect("body should rewrite")["model"],
                    "plugin-model"
                );
            }
            other => panic!("expected replace request, got {other:?}"),
        }
    }
}
