use std::{path::PathBuf, time::Duration};

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum Backend {
    Pg,
    Scylla,
}

#[derive(Clone, Debug, Eq, PartialEq, Parser)]
#[command(
    name = "ledger-bench",
    about = "Benchmark ledger transaction workloads"
)]
pub struct Args {
    #[arg(long, value_enum, default_value_t = Backend::Pg)]
    pub backend: Backend,
    /// Number of concurrent logical users (N).
    #[arg(long, default_value_t = 30)]
    pub users: u32,
    /// Transactions per user (K).
    #[arg(long, default_value_t = 100)]
    pub requests_per_user: u64,
    #[arg(long, default_value = "postgres://postgres:postgres@localhost/ledger")]
    pub pg_url: String,
    #[arg(long, default_value = "127.0.0.1:9042")]
    pub scylla_url: String,
    #[arg(long, default_value = "artifacts")]
    pub output_dir: PathBuf,
    /// Optional wrapper handshake: write prepared metadata here and wait for --go-file.
    #[arg(long)]
    pub ready_file: Option<PathBuf>,
    #[arg(long)]
    pub go_file: Option<PathBuf>,
    #[arg(long, default_value_t = 120_000)]
    pub handshake_timeout_ms: u64,
    #[arg(long, default_value_t = 20)]
    pub warmup_requests_per_user: u64,
    #[arg(long, default_value_t = 2_000)]
    pub settle_ms: u64,
    #[arg(long, default_value_t = 96)]
    pub pg_data_pool_size: usize,
    /// Expected Scylla shard count; must match the connected node's driver metadata.
    #[arg(long, default_value_t = 3)]
    pub expected_shards: usize,
    /// Scylla per-shard connections are max(1, ceil(N/(10*shards))) times this multiplier.
    #[arg(long, default_value_t = 1)]
    pub diagnostic_multiplier: usize,
    #[arg(long, default_value_t = 3)]
    pub client_workers: usize,
    #[arg(long, default_value_t = 10_000)]
    pub connect_timeout_ms: u64,
    /// Aggregate deadline for initialization, reset, and whole-account verification (ms).
    #[arg(long, default_value_t = 300_000)]
    pub setup_timeout_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    pub request_timeout_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    pub shutdown_timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedConfig {
    pub args: Args,
    pub expected_total: u64,
    pub scylla_connections_per_shard: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub setup_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Args {
    pub fn validate(self) -> Result<ValidatedConfig, ConfigError> {
        if self.users == 0 {
            return Err(ConfigError("users (N) must be greater than zero"));
        }
        if self.warmup_requests_per_user == 0 {
            return Err(ConfigError(
                "warmup requests per user must be greater than zero",
            ));
        }
        let warmup_total = u64::from(self.users)
            .checked_mul(self.warmup_requests_per_user)
            .ok_or(ConfigError("N*warmup overflows u64"))?;
        if self.warmup_requests_per_user > i64::MAX as u64 || warmup_total > i64::MAX as u64 {
            return Err(ConfigError(
                "warmup and N*warmup must fit in a signed 64-bit database bigint",
            ));
        }
        if self.requests_per_user == 0 {
            return Err(ConfigError(
                "requests per user (K) must be greater than zero",
            ));
        }
        let expected_total = u64::from(self.users)
            .checked_mul(self.requests_per_user)
            .ok_or(ConfigError("N*K overflows u64"))?;
        if self.requests_per_user > i64::MAX as u64 || expected_total > i64::MAX as u64 {
            return Err(ConfigError(
                "K and N*K must fit in a signed 64-bit database bigint",
            ));
        }
        if self.pg_data_pool_size == 0 {
            return Err(ConfigError(
                "PostgreSQL data pool size must be greater than zero",
            ));
        }
        if self.expected_shards == 0 {
            return Err(ConfigError("expected shards must be greater than zero"));
        }
        if self.diagnostic_multiplier == 0 {
            return Err(ConfigError(
                "diagnostic multiplier must be greater than zero",
            ));
        }
        if self.client_workers == 0 {
            return Err(ConfigError("client workers must be greater than zero"));
        }
        if self.ready_file.is_some() != self.go_file.is_some() {
            return Err(ConfigError(
                "ready-file and go-file must be supplied together",
            ));
        }
        if self.settle_ms == 0 {
            return Err(ConfigError("settle time must be greater than zero"));
        }
        if self.handshake_timeout_ms == 0 {
            return Err(ConfigError("handshake timeout"));
        }
        for (name, value) in [
            ("connect timeout", self.connect_timeout_ms),
            ("setup timeout", self.setup_timeout_ms),
            ("request timeout", self.request_timeout_ms),
            ("shutdown timeout", self.shutdown_timeout_ms),
        ] {
            if value == 0 {
                return Err(ConfigError(name));
            }
        }
        if self.pg_url.trim().is_empty() || self.scylla_url.trim().is_empty() {
            return Err(ConfigError("database URLs must not be empty"));
        }
        let divisor = self
            .expected_shards
            .checked_mul(10)
            .ok_or(ConfigError("Scylla shard divisor overflows usize"))?;
        let users = usize::try_from(self.users)
            .map_err(|_| ConfigError("Scylla user count does not fit in usize"))?;
        let base = users.div_ceil(divisor).max(1);
        let scylla_connections_per_shard = base
            .checked_mul(self.diagnostic_multiplier)
            .ok_or(ConfigError("Scylla connection count overflows usize"))?;
        let connect_timeout = Duration::from_millis(self.connect_timeout_ms);
        let request_timeout = Duration::from_millis(self.request_timeout_ms);
        let setup_timeout = Duration::from_millis(self.setup_timeout_ms);
        let shutdown_timeout = Duration::from_millis(self.shutdown_timeout_ms);
        Ok(ValidatedConfig {
            args: self,
            expected_total,
            scylla_connections_per_shard,
            connect_timeout,
            request_timeout,
            setup_timeout,
            shutdown_timeout,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigError(pub &'static str);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args::try_parse_from(["ledger-bench"]).unwrap()
    }

    #[test]
    fn defaults_and_shard_sizing() {
        let mut a = args();
        assert_eq!(a.pg_data_pool_size, 96);
        assert_eq!(a.client_workers, 3);
        assert_eq!(a.request_timeout_ms, 30_000);
        assert_eq!(
            a.clone().validate().unwrap().setup_timeout,
            Duration::from_secs(300)
        );
        a.users = 31;
        assert_eq!(
            a.clone().validate().unwrap().scylla_connections_per_shard,
            2
        );
        a.expected_shards = 6;
        assert_eq!(
            a.clone().validate().unwrap().scylla_connections_per_shard,
            1
        );
        a.users = 61;
        assert_eq!(
            a.clone().validate().unwrap().scylla_connections_per_shard,
            2
        );
        a.diagnostic_multiplier = 3;
        assert_eq!(a.validate().unwrap().scylla_connections_per_shard, 6);
    }

    #[test]
    fn rejects_invalid_counts_and_bigint_overflow() {
        let mut a = args();
        a.setup_timeout_ms = 0;
        assert!(a.validate().is_err());
        let mut a = args();
        a.warmup_requests_per_user = 0;
        assert!(a.validate().is_err());
        let mut a = args();
        a.users = 1;
        a.warmup_requests_per_user = u64::MAX;
        assert_eq!(
            a.validate().unwrap_err().0,
            "warmup and N*warmup must fit in a signed 64-bit database bigint"
        );
        let mut a = args();
        a.warmup_requests_per_user = u64::MAX;
        assert!(a.validate().is_err());
        let mut a = args();
        a.users = 2;
        a.warmup_requests_per_user = i64::MAX as u64;
        assert!(a.validate().is_err());
        let mut a = args();
        a.users = u32::MAX;
        a.warmup_requests_per_user = i64::MAX as u64;
        assert_eq!(a.validate().unwrap_err().0, "N*warmup overflows u64");
        let mut a = args();
        a.users = 0;
        assert!(a.validate().is_err());
        let mut a = args();
        a.requests_per_user = 0;
        assert!(a.validate().is_err());
        let mut a = args();
        a.requests_per_user = u64::MAX;
        assert!(a.validate().is_err());
        let mut a = args();
        a.users = u32::MAX;
        a.requests_per_user = i64::MAX as u64;
        assert!(a.validate().is_err());
    }
}
