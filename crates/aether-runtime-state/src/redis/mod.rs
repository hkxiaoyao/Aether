mod client;
mod kv;
mod lock;
mod namespace;
mod stream;

pub use client::{RedisClient, RedisClientConfig, RedisClientFactory};
pub use kv::{RedisKvRunner, RedisKvRunnerConfig};
pub use lock::{RedisLockKey, RedisLockLease, RedisLockRunner, RedisLockRunnerConfig};
pub use namespace::RedisKeyspace;
pub use stream::{
    RedisConsumerGroup, RedisConsumerName, RedisStreamEntry, RedisStreamName,
    RedisStreamReclaimConfig, RedisStreamReclaimResult, RedisStreamRunner, RedisStreamRunnerConfig,
};

pub(crate) type RedisCmd = redis::Cmd;
pub(crate) type RedisScript = redis::Script;

pub(crate) fn cmd(name: &str) -> RedisCmd {
    redis::cmd(name)
}

pub(crate) fn script(source: &str) -> RedisScript {
    redis::Script::new(source)
}
