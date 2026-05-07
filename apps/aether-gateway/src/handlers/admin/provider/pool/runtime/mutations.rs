use super::keys::{pool_cooldown_index_key, pool_cooldown_key};
use crate::handlers::admin::request::AdminAppState;

pub(crate) async fn clear_admin_provider_pool_cooldown(
    state: &AdminAppState<'_>,
    provider_id: &str,
    key_id: &str,
) {
    let _ = state
        .runtime_state()
        .kv_delete(&pool_cooldown_key(provider_id, key_id))
        .await;
    let _ = state
        .runtime_state()
        .set_remove(&pool_cooldown_index_key(provider_id), key_id)
        .await;
}

pub(crate) async fn reset_admin_provider_pool_cost(
    state: &AdminAppState<'_>,
    provider_id: &str,
    key_id: &str,
) {
    let _ = state
        .runtime_state()
        .score_remove_by_score(&format!("ap:{provider_id}:cost:{key_id}"), f64::INFINITY)
        .await;
}
