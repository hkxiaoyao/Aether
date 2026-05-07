use aether_data::driver::postgres::PostgresPoolConfig;
use aether_data::{DataLayerConfig, SqlDatabaseConfig};
use std::fmt;

#[derive(Clone, Default)]
pub struct GatewayDataConfig {
    database: Option<SqlDatabaseConfig>,
    postgres: Option<PostgresPoolConfig>,
    encryption_key: Option<String>,
}

impl fmt::Debug for GatewayDataConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayDataConfig")
            .field("database", &self.database)
            .field("postgres", &self.postgres)
            .field("has_encryption_key", &self.encryption_key.is_some())
            .finish()
    }
}

impl GatewayDataConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn from_postgres_config(postgres: PostgresPoolConfig) -> Self {
        Self {
            database: Some(SqlDatabaseConfig::from_postgres_config(postgres.clone())),
            postgres: Some(postgres),
            encryption_key: None,
        }
    }

    pub fn from_database_config(database: SqlDatabaseConfig) -> Self {
        let postgres = database.to_postgres_config().ok();
        Self {
            database: Some(database),
            postgres,
            encryption_key: None,
        }
    }

    pub fn from_postgres_url(database_url: impl Into<String>, require_ssl: bool) -> Self {
        let mut postgres = PostgresPoolConfig::default();
        postgres.database_url = database_url.into();
        postgres.require_ssl = require_ssl;
        Self::from_postgres_config(postgres)
    }

    pub fn postgres(&self) -> Option<&PostgresPoolConfig> {
        self.postgres.as_ref()
    }

    pub fn database(&self) -> Option<&SqlDatabaseConfig> {
        self.database.as_ref()
    }

    pub fn with_encryption_key(mut self, encryption_key: impl Into<String>) -> Self {
        let encryption_key = encryption_key.into();
        let encryption_key = encryption_key.trim();
        self.encryption_key = if encryption_key.is_empty() {
            None
        } else {
            Some(encryption_key.to_string())
        };
        self
    }

    pub fn encryption_key(&self) -> Option<&str> {
        self.encryption_key.as_deref()
    }

    pub fn with_redis_url(
        self,
        _url: impl Into<String>,
        _key_prefix: Option<impl Into<String>>,
    ) -> Self {
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.database.is_some() || self.postgres.is_some()
    }

    pub fn to_data_layer_config(&self) -> DataLayerConfig {
        DataLayerConfig {
            database: self.database.clone(),
            postgres: self.postgres.clone(),
        }
    }
}
