use async_trait::async_trait;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::warn;

pub const DEFAULT_PLUGIN_DIR: &str = "./plugins";
pub const PLUGIN_DIR_ENV: &str = "AETHER_PLUGIN_DIR";
pub const PLUGIN_API_VERSION_V1: &str = "aether.plugin.v1";
const SIDECAR_CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const SIDECAR_CIRCUIT_OPEN_FOR: Duration = Duration::from_secs(30);
const SIDECAR_STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("failed to read plugin manifest {path}: {source}")]
    ReadManifest {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse plugin manifest {path}: {message}")]
    ParseManifest { path: PathBuf, message: String },
    #[error("invalid plugin manifest {path}: {message}")]
    InvalidManifest { path: PathBuf, message: String },
    #[error("plugin {plugin_id} does not declare capability {capability}")]
    CapabilityDenied {
        plugin_id: String,
        capability: PluginCapability,
    },
    #[error("plugin runtime {runtime:?} is not executable in this build")]
    UnsupportedRuntime { runtime: PluginRuntimeKind },
    #[error("plugin hook {hook} failed for {plugin_id}: {message}")]
    HookFailed {
        plugin_id: String,
        hook: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PluginCapability(String);

impl PluginCapability {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("capability must not be empty".to_string());
        }
        if !trimmed.contains('.') {
            return Err(format!("capability {trimmed} must be namespace qualified"));
        }
        if trimmed
            .chars()
            .any(|ch| !(ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '.'))
        {
            return Err(format!(
                "capability {trimmed} may only contain lowercase ascii letters, digits, '_' and '.'"
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn namespace(&self) -> &str {
        self.0.split_once('.').map(|(left, _)| left).unwrap_or("")
    }
}

impl std::fmt::Display for PluginCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl TryFrom<String> for PluginCapability {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PluginCapability {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for PluginCapability {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRuntimeKind {
    Manifest,
    Builtin,
    Sidecar,
    Wasm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginRuntimeManifest {
    pub kind: PluginRuntimeKind,
    #[serde(default)]
    pub entry: Option<String>,
    #[serde(default)]
    pub command: Option<Vec<String>>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl PluginRuntimeManifest {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(5_000).clamp(50, 300_000))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginManifest {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default = "default_plugin_api_version")]
    pub api_version: String,
    pub runtime: PluginRuntimeManifest,
    #[serde(default)]
    pub capabilities: BTreeSet<PluginCapability>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub domains: BTreeMap<String, Value>,
}

fn default_plugin_api_version() -> String {
    PLUGIN_API_VERSION_V1.to_string()
}

impl PluginManifest {
    pub fn validate(&self) -> Result<(), String> {
        validate_plugin_id(&self.id)?;
        if self.name.trim().is_empty() {
            return Err("name must not be empty".to_string());
        }
        if self.version.trim().is_empty() {
            return Err("version must not be empty".to_string());
        }
        if self.api_version.trim() != PLUGIN_API_VERSION_V1 {
            return Err(format!("unsupported api_version {}", self.api_version));
        }
        match self.runtime.kind {
            PluginRuntimeKind::Manifest | PluginRuntimeKind::Builtin => {}
            PluginRuntimeKind::Sidecar => {
                if self
                    .runtime
                    .endpoint
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                    && self.runtime.command.as_ref().is_none_or(Vec::is_empty)
                {
                    return Err(
                        "sidecar runtime requires endpoint or command in manifest".to_string()
                    );
                }
            }
            PluginRuntimeKind::Wasm => {
                if self
                    .runtime
                    .entry
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    return Err("wasm runtime requires entry in manifest".to_string());
                }
            }
        }
        Ok(())
    }

    pub fn has_capability(&self, capability: &PluginCapability) -> bool {
        self.enabled && self.capabilities.contains(capability)
    }

    pub fn domain_config(&self, domain: &str) -> Option<&Value> {
        self.domains.get(domain)
    }
}

fn validate_plugin_id(value: &str) -> Result<(), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("id must not be empty".to_string());
    }
    if trimmed != value {
        return Err("id must not contain leading or trailing whitespace".to_string());
    }
    if trimmed.chars().any(|ch| {
        !(ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-' || ch == '.')
    }) {
        return Err(format!(
            "id {trimmed} may only contain lowercase ascii letters, digits, '_', '-' and '.'"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginHookEnvelope {
    pub plugin_id: String,
    pub trace_id: String,
    pub capability: PluginCapability,
    pub hook: String,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body_json: Option<Value>,
    #[serde(default)]
    pub body_base64: Option<String>,
    #[serde(default)]
    pub context: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PluginHookResponse {
    Continue,
    ReplaceRequest {
        #[serde(default)]
        path: Option<String>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        body_json: Option<Value>,
        #[serde(default)]
        body_base64: Option<String>,
        #[serde(default)]
        metadata: BTreeMap<String, Value>,
    },
    ReplaceResponse {
        status: u16,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        body_json: Option<Value>,
        #[serde(default)]
        body_base64: Option<String>,
    },
    StreamEvents {
        events: Vec<Value>,
    },
    Error {
        code: String,
        message: String,
        #[serde(default)]
        retryable: bool,
    },
}

#[async_trait]
pub trait PluginRuntime: Send + Sync {
    fn manifest(&self) -> &PluginManifest;

    async fn call_hook(
        &self,
        envelope: PluginHookEnvelope,
    ) -> Result<PluginHookResponse, PluginError>;
}

#[derive(Clone, Serialize)]
pub struct PluginRegistryEntry {
    pub manifest: PluginManifest,
    pub source: PluginSource,
    #[serde(skip_serializing)]
    pub runtime: Option<Arc<dyn PluginRuntime>>,
    pub load_error: Option<String>,
}

impl std::fmt::Debug for PluginRegistryEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRegistryEntry")
            .field("manifest", &self.manifest)
            .field("source", &self.source)
            .field("runtime", &self.runtime.as_ref().map(|_| "<runtime>"))
            .field("load_error", &self.load_error)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginSource {
    Builtin,
    Local,
}

#[derive(Debug, Default, Clone)]
pub struct PluginRegistry {
    entries: BTreeMap<String, PluginRegistryEntry>,
    order: Vec<String>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_builtin(&mut self, runtime: Arc<dyn PluginRuntime>) -> Result<(), PluginError> {
        let manifest = runtime.manifest().clone();
        manifest
            .validate()
            .map_err(|message| PluginError::InvalidManifest {
                path: PathBuf::from(format!("builtin:{}", manifest.id)),
                message,
            })?;
        if !self.entries.contains_key(&manifest.id) {
            self.order.push(manifest.id.clone());
        }
        self.entries.insert(
            manifest.id.clone(),
            PluginRegistryEntry {
                manifest,
                source: PluginSource::Builtin,
                runtime: Some(runtime),
                load_error: None,
            },
        );
        Ok(())
    }

    pub fn register_manifest(
        &mut self,
        manifest: PluginManifest,
        source: PluginSource,
        runtime: Option<Arc<dyn PluginRuntime>>,
        load_error: Option<String>,
    ) {
        if !self.entries.contains_key(&manifest.id) {
            self.order.push(manifest.id.clone());
        }
        self.entries.insert(
            manifest.id.clone(),
            PluginRegistryEntry {
                manifest,
                source,
                runtime,
                load_error,
            },
        );
    }

    pub fn get(&self, plugin_id: &str) -> Option<&PluginRegistryEntry> {
        self.entries.get(plugin_id)
    }

    pub fn entries(&self) -> impl Iterator<Item = &PluginRegistryEntry> {
        self.order
            .iter()
            .filter_map(|plugin_id| self.entries.get(plugin_id))
    }

    pub fn enabled_with_capability<'a>(
        &'a self,
        capability: &'a PluginCapability,
    ) -> impl Iterator<Item = &'a PluginRegistryEntry> + 'a {
        self.entries()
            .filter(move |entry| entry.manifest.has_capability(capability))
    }

    pub async fn call_first(
        &self,
        capability: &PluginCapability,
        envelope: PluginHookEnvelope,
    ) -> Result<Option<PluginHookResponse>, PluginError> {
        for entry in self.enabled_with_capability(capability) {
            let Some(runtime) = entry.runtime.as_ref() else {
                continue;
            };
            let mut envelope = envelope.clone();
            envelope.plugin_id = entry.manifest.id.clone();
            envelope.capability = capability.clone();
            return runtime.call_hook(envelope).await.map(Some);
        }
        Ok(None)
    }
}

pub fn plugin_dir_from_env() -> PathBuf {
    std::env::var(PLUGIN_DIR_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_PLUGIN_DIR))
}

pub fn load_local_plugin_manifests(
    root: impl AsRef<Path>,
) -> Vec<Result<PluginManifest, PluginError>> {
    let root = root.as_ref();
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if plugin_manifest_path(&path) {
            paths.push(path);
            continue;
        }
        if !path.is_dir() {
            continue;
        }
        for manifest_name in ["plugin.toml", "plugin.json"] {
            let candidate = path.join(manifest_name);
            if candidate.is_file() {
                paths.push(candidate);
                break;
            }
        }
    }
    paths.sort();
    paths.into_iter().map(load_plugin_manifest_file).collect()
}

fn plugin_manifest_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| {
            name == "plugin.toml"
                || name == "plugin.json"
                || name.ends_with(".plugin.toml")
                || name.ends_with(".plugin.json")
        })
}

pub fn load_plugin_manifest_file(path: impl AsRef<Path>) -> Result<PluginManifest, PluginError> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path).map_err(|source| PluginError::ReadManifest {
        path: path.to_path_buf(),
        source,
    })?;
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    let manifest = if extension.eq_ignore_ascii_case("json") {
        serde_json::from_str::<PluginManifest>(&raw).map_err(|err| PluginError::ParseManifest {
            path: path.to_path_buf(),
            message: err.to_string(),
        })?
    } else {
        toml::from_str::<PluginManifest>(&raw).map_err(|err| PluginError::ParseManifest {
            path: path.to_path_buf(),
            message: err.to_string(),
        })?
    };
    manifest
        .validate()
        .map_err(|message| PluginError::InvalidManifest {
            path: path.to_path_buf(),
            message,
        })?;
    Ok(manifest)
}

#[derive(Debug, Clone)]
pub struct ManifestOnlyRuntime {
    manifest: PluginManifest,
}

impl ManifestOnlyRuntime {
    pub fn new(manifest: PluginManifest) -> Self {
        Self { manifest }
    }
}

#[async_trait]
impl PluginRuntime for ManifestOnlyRuntime {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn call_hook(
        &self,
        envelope: PluginHookEnvelope,
    ) -> Result<PluginHookResponse, PluginError> {
        if !self.manifest.has_capability(&envelope.capability) {
            return Err(PluginError::CapabilityDenied {
                plugin_id: self.manifest.id.clone(),
                capability: envelope.capability,
            });
        }
        Ok(PluginHookResponse::Continue)
    }
}

pub fn runtime_for_local_manifest(
    manifest: PluginManifest,
) -> (Option<Arc<dyn PluginRuntime>>, Option<String>) {
    match manifest.runtime.kind {
        PluginRuntimeKind::Manifest => (Some(Arc::new(ManifestOnlyRuntime::new(manifest))), None),
        PluginRuntimeKind::Sidecar => {
            if manifest
                .runtime
                .endpoint
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some()
            {
                (Some(Arc::new(SidecarHttpRuntime::new(manifest))), None)
            } else {
                (
                    None,
                    Some(
                        "sidecar runtime requires runtime.endpoint so hooks can reach the sidecar"
                            .to_string(),
                    ),
                )
            }
        }
        PluginRuntimeKind::Wasm => {
            let runtime = manifest.runtime.kind;
            (
                None,
                Some(format!(
                    "{runtime:?} runtime manifest loaded but Wasmtime adapter is not enabled yet"
                )),
            )
        }
        PluginRuntimeKind::Builtin => (
            None,
            Some("builtin runtime cannot be loaded from local plugin directory".to_string()),
        ),
    }
}

#[derive(Debug, Default)]
struct SidecarProcessState {
    child: Option<tokio::process::Child>,
    manifest_ready: bool,
}

#[derive(Debug, Default)]
struct SidecarCircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

#[derive(Clone)]
pub struct SidecarHttpRuntime {
    manifest: PluginManifest,
    client: reqwest::Client,
    process: Arc<Mutex<SidecarProcessState>>,
    circuit: Arc<Mutex<SidecarCircuitState>>,
}

impl SidecarHttpRuntime {
    pub fn new(manifest: PluginManifest) -> Self {
        Self {
            manifest,
            client: reqwest::Client::new(),
            process: Arc::new(Mutex::new(SidecarProcessState::default())),
            circuit: Arc::new(Mutex::new(SidecarCircuitState::default())),
        }
    }

    fn endpoint(&self) -> Result<&str, PluginError> {
        let Some(endpoint) = self
            .manifest
            .runtime
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Err(PluginError::UnsupportedRuntime {
                runtime: PluginRuntimeKind::Sidecar,
            });
        };
        Ok(endpoint)
    }

    fn hook_urls(&self, hook: &str) -> Result<Vec<String>, PluginError> {
        let endpoint = self.endpoint()?;
        let hook = hook.trim_start_matches('/');
        let slash_hook = hook.replace('.', "/");
        let base = endpoint.trim_end_matches('/');
        let mut urls = vec![format!("{base}/hook/{slash_hook}")];
        if slash_hook != hook {
            urls.push(format!("{base}/hook/{hook}"));
        }
        Ok(urls)
    }

    fn manifest_url(&self) -> Result<String, PluginError> {
        Ok(format!(
            "{}/manifest",
            self.endpoint()?.trim_end_matches('/')
        ))
    }

    fn has_command(&self) -> bool {
        self.manifest
            .runtime
            .command
            .as_ref()
            .is_some_and(|command| !command.is_empty())
    }

    async fn ensure_command_sidecar_ready(&self, hook: &str) -> Result<(), PluginError> {
        if !self.has_command() {
            return Ok(());
        }

        let should_probe_manifest = {
            let mut state = self.process.lock().await;
            if let Some(child) = state.child.as_mut() {
                match child.try_wait() {
                    Ok(None) if state.manifest_ready => return Ok(()),
                    Ok(None) => true,
                    Ok(Some(status)) => {
                        warn!(
                            plugin_id = self.manifest.id.as_str(),
                            exit_status = %status,
                            "plugin sidecar process exited before hook dispatch"
                        );
                        state.child = None;
                        state.manifest_ready = false;
                        false
                    }
                    Err(err) => {
                        warn!(
                            plugin_id = self.manifest.id.as_str(),
                            error = %err,
                            "plugin sidecar process status check failed"
                        );
                        state.child = None;
                        state.manifest_ready = false;
                        false
                    }
                }
            } else {
                false
            }
        };

        if !should_probe_manifest {
            self.spawn_sidecar_process(hook).await?;
        }
        self.wait_for_sidecar_manifest(hook).await?;
        let mut state = self.process.lock().await;
        state.manifest_ready = true;
        Ok(())
    }

    async fn spawn_sidecar_process(&self, hook: &str) -> Result<(), PluginError> {
        let Some(command) = self
            .manifest
            .runtime
            .command
            .as_ref()
            .filter(|command| !command.is_empty())
        else {
            return Ok(());
        };
        let program = command[0].trim();
        if program.is_empty() {
            return Err(PluginError::HookFailed {
                plugin_id: self.manifest.id.clone(),
                hook: hook.to_string(),
                message: "sidecar command program is empty".to_string(),
            });
        }
        let mut cmd = Command::new(program);
        cmd.args(command.iter().skip(1))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = cmd.spawn().map_err(|err| PluginError::HookFailed {
            plugin_id: self.manifest.id.clone(),
            hook: hook.to_string(),
            message: format!("failed to start sidecar command: {err}"),
        })?;
        let mut state = self.process.lock().await;
        state.child = Some(child);
        state.manifest_ready = false;
        Ok(())
    }

    async fn wait_for_sidecar_manifest(&self, hook: &str) -> Result<(), PluginError> {
        let url = self.manifest_url()?;
        let deadline = Instant::now() + self.manifest.runtime.timeout();
        let mut last_error = "sidecar /manifest did not respond".to_string();
        loop {
            if Instant::now() >= deadline {
                return Err(PluginError::HookFailed {
                    plugin_id: self.manifest.id.clone(),
                    hook: hook.to_string(),
                    message: format!("sidecar command did not become ready: {last_error}"),
                });
            }
            match self.client.get(&url).send().await {
                Ok(response) if response.status().is_success() => {
                    match response.json::<PluginManifest>().await {
                        Ok(manifest) if manifest.id == self.manifest.id => return Ok(()),
                        Ok(manifest) => {
                            last_error = format!(
                                "sidecar /manifest returned plugin id {}, expected {}",
                                manifest.id, self.manifest.id
                            );
                        }
                        Err(err) => {
                            last_error = format!("sidecar /manifest returned invalid JSON: {err}");
                        }
                    }
                }
                Ok(response) => {
                    last_error = format!("sidecar /manifest returned HTTP {}", response.status());
                }
                Err(err) => {
                    last_error = err.to_string();
                }
            }
            tokio::time::sleep(SIDECAR_STARTUP_POLL_INTERVAL).await;
        }
    }

    async fn circuit_open_error(&self, hook: &str) -> Option<PluginError> {
        let mut circuit = self.circuit.lock().await;
        let now = Instant::now();
        let Some(open_until) = circuit.open_until else {
            return None;
        };
        if open_until <= now {
            circuit.open_until = None;
            circuit.consecutive_failures = 0;
            return None;
        }
        let remaining_ms = open_until.saturating_duration_since(now).as_millis();
        Some(PluginError::HookFailed {
            plugin_id: self.manifest.id.clone(),
            hook: hook.to_string(),
            message: format!("sidecar circuit is open for another {remaining_ms}ms"),
        })
    }

    async fn record_hook_success(&self) {
        let mut circuit = self.circuit.lock().await;
        circuit.consecutive_failures = 0;
        circuit.open_until = None;
    }

    async fn record_hook_failure(&self) {
        let mut circuit = self.circuit.lock().await;
        circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
        if circuit.consecutive_failures >= SIDECAR_CIRCUIT_FAILURE_THRESHOLD {
            circuit.open_until = Some(Instant::now() + SIDECAR_CIRCUIT_OPEN_FOR);
        }
    }

    async fn post_hook_url(
        &self,
        url: &str,
        envelope: &PluginHookEnvelope,
        hook: &str,
    ) -> Result<PluginHookResponse, PluginError> {
        let response = tokio::time::timeout(
            self.manifest.runtime.timeout(),
            self.client.post(url).json(envelope).send(),
        )
        .await
        .map_err(|_| PluginError::HookFailed {
            plugin_id: self.manifest.id.clone(),
            hook: hook.to_string(),
            message: "sidecar hook timed out".to_string(),
        })?
        .map_err(|err| PluginError::HookFailed {
            plugin_id: self.manifest.id.clone(),
            hook: hook.to_string(),
            message: err.to_string(),
        })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PluginError::HookFailed {
                plugin_id: self.manifest.id.clone(),
                hook: hook.to_string(),
                message: format!("sidecar returned HTTP {status}: {}", body.trim()),
            });
        }

        response
            .json::<PluginHookResponse>()
            .await
            .map_err(|err| PluginError::HookFailed {
                plugin_id: self.manifest.id.clone(),
                hook: hook.to_string(),
                message: format!("sidecar returned invalid JSON: {err}"),
            })
    }
}

#[async_trait]
impl PluginRuntime for SidecarHttpRuntime {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn call_hook(
        &self,
        envelope: PluginHookEnvelope,
    ) -> Result<PluginHookResponse, PluginError> {
        if !self.manifest.has_capability(&envelope.capability) {
            return Err(PluginError::CapabilityDenied {
                plugin_id: self.manifest.id.clone(),
                capability: envelope.capability,
            });
        }
        let hook = envelope.hook.clone();
        if let Some(err) = self.circuit_open_error(&hook).await {
            return Err(err);
        }
        let result = async {
            self.ensure_command_sidecar_ready(&hook).await?;
            let urls = self.hook_urls(&hook)?;
            let mut last_not_found = None;
            for (index, url) in urls.iter().enumerate() {
                match self.post_hook_url(url, &envelope, &hook).await {
                    Ok(response) => return Ok(response),
                    Err(PluginError::HookFailed {
                        plugin_id,
                        hook,
                        message,
                    }) if message.starts_with("sidecar returned HTTP 404")
                        && index + 1 < urls.len() =>
                    {
                        last_not_found = Some(PluginError::HookFailed {
                            plugin_id,
                            hook,
                            message,
                        });
                    }
                    Err(err) => return Err(err),
                }
            }
            Err(last_not_found.unwrap_or_else(|| PluginError::HookFailed {
                plugin_id: self.manifest.id.clone(),
                hook,
                message: "sidecar hook URL list was empty".to_string(),
            }))
        }
        .await;
        match result {
            Ok(response) => {
                self.record_hook_success().await;
                Ok(response)
            }
            Err(err) => {
                self.record_hook_failure().await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_namespace_capability() {
        assert!(PluginCapability::new("provider.request_rewrite").is_ok());
        assert!(PluginCapability::new("request_rewrite").is_err());
        assert!(PluginCapability::new("Provider.request").is_err());
    }

    #[test]
    fn parses_toml_manifest_with_domain_config() {
        let raw = r#"
id = "openai-compatible"
name = "OpenAI Compatible"
version = "1.0.0"
enabled = true
capabilities = ["provider.request_rewrite", "provider.model_fetch"]

[runtime]
kind = "manifest"

[domains.provider]
provider_types = ["openai"]
api_formats = ["openai:chat"]
"#;
        let manifest = toml::from_str::<PluginManifest>(raw).expect("manifest should parse");
        manifest.validate().expect("manifest should validate");
        assert!(manifest.has_capability(&PluginCapability::new("provider.model_fetch").unwrap()));
        assert!(manifest.domain_config("provider").is_some());
    }

    #[test]
    fn discovers_plugin_manifests_in_root_and_child_dirs() {
        let root =
            std::env::temp_dir().join(format!("aether-plugin-core-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("child")).expect("temp dir should be created");
        fs::write(
            root.join("root.plugin.toml"),
            r#"
id = "root-plugin"
name = "Root"
version = "1"
enabled = true
capabilities = ["provider.request_rewrite"]

[runtime]
kind = "manifest"
"#,
        )
        .expect("root manifest should write");
        fs::write(
            root.join("child").join("plugin.toml"),
            r#"
id = "child-plugin"
name = "Child"
version = "1"
enabled = true
capabilities = ["provider.model_fetch"]

[runtime]
kind = "manifest"
"#,
        )
        .expect("child manifest should write");

        let manifests = load_local_plugin_manifests(&root)
            .into_iter()
            .map(|result| result.expect("manifest should load").id)
            .collect::<Vec<_>>();
        assert_eq!(manifests, vec!["child-plugin", "root-plugin"]);
        fs::remove_dir_all(root).expect("temp dir should be removed");
    }

    #[test]
    fn sidecar_command_requires_endpoint_for_runtime_loading() {
        let manifest = sidecar_manifest(None, Some(vec!["sidecar".to_string()]), 50);
        let (runtime, load_error) = runtime_for_local_manifest(manifest);
        assert!(runtime.is_none());
        assert!(load_error
            .as_deref()
            .is_some_and(|message| message.contains("runtime.endpoint")));
    }

    #[tokio::test]
    async fn sidecar_runtime_opens_circuit_after_repeated_failures() {
        let manifest = sidecar_manifest(Some("http://127.0.0.1:1"), None, 50);
        let runtime = SidecarHttpRuntime::new(manifest);
        let envelope = sidecar_envelope();

        for _ in 0..SIDECAR_CIRCUIT_FAILURE_THRESHOLD {
            assert!(runtime.call_hook(envelope.clone()).await.is_err());
        }
        let err = runtime
            .call_hook(envelope)
            .await
            .expect_err("circuit should reject the next hook")
            .to_string();
        assert!(err.contains("sidecar circuit is open"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sidecar_command_starts_process_before_manifest_probe() {
        let marker = std::env::temp_dir().join(format!(
            "aether-sidecar-command-{}-{}",
            std::process::id(),
            current_test_nanos()
        ));
        let command = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            format!("touch {} && sleep 1", marker.display()),
        ];
        let manifest = sidecar_manifest(Some("http://127.0.0.1:1"), Some(command), 50);
        let runtime = SidecarHttpRuntime::new(manifest);
        let _ = runtime.call_hook(sidecar_envelope()).await;

        for _ in 0..20 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(marker.exists(), "sidecar command should have started");
        let _ = fs::remove_file(marker);
    }

    fn sidecar_manifest(
        endpoint: Option<&str>,
        command: Option<Vec<String>>,
        timeout_ms: u64,
    ) -> PluginManifest {
        PluginManifest {
            id: "sidecar-test".to_string(),
            name: "Sidecar Test".to_string(),
            version: "1".to_string(),
            api_version: PLUGIN_API_VERSION_V1.to_string(),
            runtime: PluginRuntimeManifest {
                kind: PluginRuntimeKind::Sidecar,
                entry: None,
                command,
                endpoint: endpoint.map(ToOwned::to_owned),
                timeout_ms: Some(timeout_ms),
            },
            capabilities: BTreeSet::from([PluginCapability::new("provider.request_rewrite")
                .expect("capability should be valid")]),
            enabled: true,
            description: None,
            domains: BTreeMap::from([("provider".to_string(), json!({}))]),
        }
    }

    fn sidecar_envelope() -> PluginHookEnvelope {
        PluginHookEnvelope {
            plugin_id: "sidecar-test".to_string(),
            trace_id: "trace".to_string(),
            capability: PluginCapability::new("provider.request_rewrite")
                .expect("capability should be valid"),
            hook: "provider.request_rewrite".to_string(),
            method: Some("POST".to_string()),
            path: Some("/v1/chat".to_string()),
            query: None,
            headers: BTreeMap::new(),
            body_json: Some(json!({})),
            body_base64: None,
            context: BTreeMap::new(),
        }
    }

    fn current_test_nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock should be after epoch")
            .as_nanos()
    }
}
