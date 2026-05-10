use super::keys::pool_lease_key;
use aether_runtime_state::{DataLayerError, RuntimeLockLease, RuntimeState};
use std::time::Duration;

pub(crate) const ADMIN_PROVIDER_POOL_KEY_LEASE_TTL_MS: u64 = 15 * 60 * 1000;

pub(crate) async fn try_claim_admin_provider_pool_key(
    runtime: &RuntimeState,
    provider_id: &str,
    key_id: &str,
    owner: &str,
) -> Result<Option<RuntimeLockLease>, DataLayerError> {
    runtime
        .lock_try_acquire(
            &pool_lease_key(provider_id, key_id),
            owner,
            Duration::from_millis(ADMIN_PROVIDER_POOL_KEY_LEASE_TTL_MS),
        )
        .await
}

pub(crate) async fn release_admin_provider_pool_key_lease(
    runtime: &RuntimeState,
    lease: &RuntimeLockLease,
) -> Result<bool, DataLayerError> {
    runtime.lock_release(lease).await
}
