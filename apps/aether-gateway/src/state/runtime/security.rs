use crate::state::AdminSecurityBlacklistEntry;
use crate::{AppState, GatewayError};

impl AppState {
    pub(crate) async fn add_admin_security_blacklist(
        &self,
        ip_address: &str,
        reason: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        let key = format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}{ip_address}");
        self.runtime_state
            .kv_set(
                &key,
                reason.to_string(),
                ttl_seconds.map(std::time::Duration::from_secs),
            )
            .await
            .map(|_| true)
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn remove_admin_security_blacklist(
        &self,
        ip_address: &str,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        let key = format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}{ip_address}");
        self.runtime_state
            .kv_delete(&key)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn admin_security_blacklist_stats(
        &self,
    ) -> Result<(bool, usize, Option<String>), GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        let total = self
            .runtime_state
            .scan_keys(&format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}*"), 100)
            .await
            .map(|keys| keys.len())
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        Ok((true, total, None))
    }

    pub(crate) async fn list_admin_security_blacklist(
        &self,
    ) -> Result<Vec<AdminSecurityBlacklistEntry>, GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        let keys = self
            .runtime_state
            .scan_keys(&format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}*"), 100)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let mut entries = Vec::new();
        for full_key in keys {
            let raw_key = self.runtime_state.strip_namespace(&full_key);
            let ip_address = raw_key
                .strip_prefix(ADMIN_SECURITY_BLACKLIST_PREFIX)
                .unwrap_or(raw_key)
                .to_string();
            let Some(reason) = self
                .runtime_state
                .kv_get(raw_key)
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
            else {
                continue;
            };
            let ttl_seconds = self
                .runtime_state
                .kv_ttl_seconds(raw_key)
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
                .filter(|ttl| *ttl >= 0);
            entries.push(AdminSecurityBlacklistEntry {
                ip_address,
                reason,
                ttl_seconds,
            });
        }
        entries.sort_by(|a, b| a.ip_address.cmp(&b.ip_address));
        Ok(entries)
    }

    pub(crate) async fn add_admin_security_whitelist(
        &self,
        ip_address: &str,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_WHITELIST_KEY: &str = "ip:whitelist";

        self.runtime_state
            .set_add(ADMIN_SECURITY_WHITELIST_KEY, ip_address)
            .await
            .map(|_| true)
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn remove_admin_security_whitelist(
        &self,
        ip_address: &str,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_WHITELIST_KEY: &str = "ip:whitelist";

        self.runtime_state
            .set_remove(ADMIN_SECURITY_WHITELIST_KEY, ip_address)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_admin_security_whitelist(&self) -> Result<Vec<String>, GatewayError> {
        const ADMIN_SECURITY_WHITELIST_KEY: &str = "ip:whitelist";

        self.runtime_state
            .set_members(ADMIN_SECURITY_WHITELIST_KEY)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }
}
