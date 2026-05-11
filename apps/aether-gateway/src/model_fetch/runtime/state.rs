use aether_contracts::{ExecutionPlan, ExecutionResult};
use aether_data_contracts::repository::global_models::{
    AdminGlobalModelListQuery, AdminProviderModelListQuery, StoredAdminGlobalModelPage,
    StoredAdminProviderModel, UpsertAdminProviderModelRecord,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_model_fetch::{ModelFetchAssociationStore, ModelFetchTransportRuntime};
use async_trait::async_trait;
use serde_json::Value;

use crate::provider_transport::GatewayProviderTransportSnapshot;
use crate::GatewayError;

#[async_trait]
pub(crate) trait ModelFetchRuntimeState:
    ModelFetchAssociationStore<Error = String> + ModelFetchTransportRuntime + Sync
{
    fn has_provider_catalog_data_reader(&self) -> bool;
    fn has_provider_catalog_data_writer(&self) -> bool;

    async fn list_provider_catalog_providers(
        &self,
        active_only: bool,
    ) -> Result<Vec<StoredProviderCatalogProvider>, GatewayError>;

    async fn list_provider_catalog_endpoints_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogEndpoint>, GatewayError>;

    async fn read_provider_transport_snapshot(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<GatewayProviderTransportSnapshot>, GatewayError>;

    async fn execute_execution_runtime_sync_plan(
        &self,
        plan: &ExecutionPlan,
    ) -> Result<ExecutionResult, GatewayError>;

    async fn update_provider_catalog_key(
        &self,
        key: &StoredProviderCatalogKey,
    ) -> Result<(), GatewayError>;

    async fn write_upstream_models_cache(
        &self,
        provider_id: &str,
        key_id: &str,
        cached_models: &[Value],
    );

    async fn fetch_models_via_provider_plugin(
        &self,
        _trace_id: &str,
        _transports: &[GatewayProviderTransportSnapshot],
    ) -> Result<Option<aether_model_fetch::ModelsFetchOutcome>, GatewayError> {
        Ok(None)
    }
}
