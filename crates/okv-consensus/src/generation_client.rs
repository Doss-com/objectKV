use crate::rpc::{read_response, write_request, GENERATION_READ};
use crate::GenerationAuthorityState;
use std::time::Duration;
use tokio::net::TcpStream;

const RETRY_ATTEMPTS: usize = 500;

/// Read-only client for the replicated cell-generation authority.
#[derive(Clone, Debug)]
pub struct GenerationClient {
    endpoints: Vec<String>,
}

impl GenerationClient {
    /// Create a client over one bounded coordinator endpoint set.
    ///
    /// # Errors
    ///
    /// Returns an error when no valid endpoint is supplied.
    pub fn new(endpoints: Vec<String>) -> Result<Self, String> {
        if endpoints.is_empty() || endpoints.iter().any(String::is_empty) {
            return Err("generation client requires non-empty coordinator endpoints".to_owned());
        }
        Ok(Self { endpoints })
    }

    /// Read generation state after a Raft linearizability barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when no coordinator serves a linearizable read within
    /// the bounded retry budget.
    pub async fn read(&self) -> Result<GenerationAuthorityState, String> {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            let endpoint = &self.endpoints[attempt % self.endpoints.len()];
            match generation_read(endpoint).await {
                Ok(state) => return Ok(state),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("generation state could not be read: {last}"))
    }
}

async fn generation_read(endpoint: &str) -> Result<GenerationAuthorityState, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("connect timed out at {endpoint}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, GENERATION_READ, &())
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<GenerationAuthorityState, String> = read_response(&mut stream)
        .await
        .map_err(|error| error.to_string())?;
    response
}

#[cfg(test)]
mod tests {
    use super::GenerationClient;

    #[test]
    fn generation_client_requires_an_endpoint() {
        assert!(GenerationClient::new(Vec::new()).is_err());
        assert!(GenerationClient::new(vec![String::new()]).is_err());
        assert!(GenerationClient::new(vec!["127.0.0.1:1".to_owned()]).is_ok());
    }
}
