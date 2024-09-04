use worterbuch_client::ConnectionError;

#[derive(thiserror::Error, Debug)]
pub enum LeaderElectionError {
    #[error("Worterbuch error: {0}")]
    WorterbuchError(#[source] ConnectionError),
}

pub type Result<T> = std::result::Result<T, LeaderElectionError>;

impl From<ConnectionError> for LeaderElectionError {
    fn from(e: ConnectionError) -> Self {
        LeaderElectionError::WorterbuchError(e)
    }
}
