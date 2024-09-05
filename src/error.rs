use crate::LeaderState;
use tokio::sync::mpsc;
use worterbuch_client::ConnectionError;

#[derive(thiserror::Error, Debug)]
pub enum LeaderElectionError {
    #[error("Worterbuch error: {0}")]
    WorterbuchError(#[source] ConnectionError),
    #[error("Send error: {0}")]
    SendError(#[source] mpsc::error::SendError<LeaderState>),
}

pub type Result<T> = std::result::Result<T, LeaderElectionError>;

impl From<ConnectionError> for LeaderElectionError {
    fn from(e: ConnectionError) -> Self {
        LeaderElectionError::WorterbuchError(e)
    }
}

impl From<mpsc::error::SendError<LeaderState>> for LeaderElectionError {
    fn from(e: mpsc::error::SendError<LeaderState>) -> Self {
        LeaderElectionError::SendError(e)
    }
}
