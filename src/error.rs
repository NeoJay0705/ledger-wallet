//! Typed errors returned by the formal ledger service.

use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PipelineError {
    InvalidRequest(String),
    InvalidConfiguration(String),
    Conflict,
    ExpiredTime {
        transaction_at: u64,
        watermark: u64,
    },
    Overloaded,
    NotAccepted(String),
    CommitOutcomeUnknown(String),
    HistoryUnavailable(String),
    HistoryConflict(String),
    Corruption(String),
    Closed,
    ShutdownTimeout,
    Infrastructure {
        operation: &'static str,
        detail: String,
    },
}

impl PipelineError {
    pub(crate) fn infrastructure(operation: &'static str, detail: impl Into<String>) -> Self {
        Self::Infrastructure {
            operation,
            detail: detail.into(),
        }
    }

    pub(crate) fn accepted_failure(detail: impl Into<String>) -> Self {
        Self::CommitOutcomeUnknown(detail.into())
    }
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(message) => write!(f, "invalid request: {message}"),
            Self::InvalidConfiguration(message) => write!(f, "invalid configuration: {message}"),
            Self::Conflict => f.write_str("transaction key conflicts with an earlier request"),
            Self::ExpiredTime {
                transaction_at,
                watermark,
            } => write!(
                f,
                "transaction time {transaction_at} is older than watermark {watermark} and is not in history"
            ),
            Self::Overloaded => f.write_str("service is at its in-flight request limit"),
            Self::NotAccepted(message) => write!(f, "request was not accepted: {message}"),
            Self::CommitOutcomeUnknown(message) => {
                write!(
                    f,
                    "request was accepted but its commit outcome is unknown: {message}"
                )
            }
            Self::HistoryUnavailable(message) => write!(f, "history is unavailable: {message}"),
            Self::HistoryConflict(message) => write!(f, "history conflict: {message}"),
            Self::Corruption(message) => write!(f, "durable data is inconsistent: {message}"),
            Self::Closed => f.write_str("service is closing or closed"),
            Self::ShutdownTimeout => {
                f.write_str("shutdown is still running in its owned coordinator")
            }
            Self::Infrastructure { operation, detail } => {
                write!(f, "{operation} failed: {detail}")
            }
        }
    }
}

impl Error for PipelineError {}
