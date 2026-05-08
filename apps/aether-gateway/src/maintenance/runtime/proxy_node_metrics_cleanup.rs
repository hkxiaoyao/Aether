use aether_data::repository::proxy_nodes::ProxyNodeMetricsCleanupSummary;
use aether_data_contracts::DataLayerError;

use crate::data::GatewayDataState;

use super::now_unix_secs;

const PROXY_NODE_METRICS_1M_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const PROXY_NODE_METRICS_1H_RETENTION_SECS: u64 = 180 * 24 * 60 * 60;

pub(super) async fn cleanup_proxy_node_metrics_once(
    data: &GatewayDataState,
) -> Result<ProxyNodeMetricsCleanupSummary, DataLayerError> {
    let now = now_unix_secs();
    data.cleanup_proxy_node_metrics(
        now.saturating_sub(PROXY_NODE_METRICS_1M_RETENTION_SECS),
        now.saturating_sub(PROXY_NODE_METRICS_1H_RETENTION_SECS),
    )
    .await
}
