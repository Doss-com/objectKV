use crate::rpc::{
    read_response, write_request, PublicationWriteRequest, WriteAck, PUBLICATION_OUTCOME,
    PUBLICATION_READ, PUBLICATION_WRITE,
};
use crate::{
    ApplyResponse, PublicationApplyResponse, PublicationAuthorityState, PublicationCommand,
    RequestIdentity,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;
use tokio::net::TcpStream;

const RETRY_ATTEMPTS: usize = 500;

/// Generation-authority client for replicated publication state.
#[derive(Clone, Debug)]
pub struct PublicationClient {
    endpoints: Vec<String>,
}

impl PublicationClient {
    /// Create a client over one bounded coordinator endpoint set.
    ///
    /// # Errors
    ///
    /// Returns an error when no endpoint is supplied.
    pub fn new(endpoints: Vec<String>) -> Result<Self, String> {
        if endpoints.is_empty() || endpoints.iter().any(String::is_empty) {
            return Err("publication client requires non-empty coordinator endpoints".to_owned());
        }
        Ok(Self { endpoints })
    }

    /// Commit or exactly recover one publication command.
    ///
    /// # Errors
    ///
    /// Returns an error when no coordinator can commit or resolve the request
    /// within the bounded retry budget.
    pub async fn commit(
        &self,
        command: &PublicationCommand,
    ) -> Result<PublicationApplyResponse, String> {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            let endpoint = self.endpoint(attempt);
            match publication_write(endpoint, command).await {
                Ok(ack) => match publication_response(ack) {
                    Ok(response) => return Ok(response),
                    Err(error) => last = error,
                },
                Err(error) => {
                    last = error;
                    if let Ok(Some(response)) = self.outcome_once(command.identity).await {
                        return Ok(response);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "publication command could not be committed or resolved: {last}"
        ))
    }

    /// Read publication state after a linearizability barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when no coordinator serves a linearizable read within
    /// the bounded retry budget.
    pub async fn read(&self) -> Result<PublicationAuthorityState, String> {
        self.retry_read(PUBLICATION_READ, &()).await
    }

    /// Resolve one publication request through a linearizable outcome read.
    ///
    /// # Errors
    ///
    /// Returns an error when no coordinator serves a linearizable outcome read
    /// within the bounded retry budget.
    pub async fn outcome(
        &self,
        identity: RequestIdentity,
    ) -> Result<Option<PublicationApplyResponse>, String> {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control::<_, Option<ApplyResponse>>(
                self.endpoint(attempt),
                PUBLICATION_OUTCOME,
                &identity,
            )
            .await
            {
                Ok(response) => return publication_outcome(response),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("publication outcome could not be read: {last}"))
    }

    fn endpoint(&self, attempt: usize) -> &str {
        &self.endpoints[attempt % self.endpoints.len()]
    }

    async fn outcome_once(
        &self,
        identity: RequestIdentity,
    ) -> Result<Option<PublicationApplyResponse>, String> {
        let mut last = String::new();
        for endpoint in &self.endpoints {
            match control::<_, Option<ApplyResponse>>(endpoint, PUBLICATION_OUTCOME, &identity)
                .await
            {
                Ok(response) => return publication_outcome(response),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    async fn retry_read<Req, Resp>(&self, kind: u8, request: &Req) -> Result<Resp, String>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control(self.endpoint(attempt), kind, request).await {
                Ok(response) => return Ok(response),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("publication read failed: {last}"))
    }
}

fn publication_response(ack: WriteAck) -> Result<PublicationApplyResponse, String> {
    if !ack.committed {
        return Err("publication command was not quorum committed".to_owned());
    }
    ack.response
        .and_then(|response| response.publication)
        .ok_or_else(|| "publication command response is absent".to_owned())
}

fn publication_outcome(
    response: Option<ApplyResponse>,
) -> Result<Option<PublicationApplyResponse>, String> {
    response
        .map(|response| {
            response
                .publication
                .ok_or_else(|| "request outcome is not a publication response".to_owned())
        })
        .transpose()
}

async fn publication_write(
    endpoint: &str,
    command: &PublicationCommand,
) -> Result<WriteAck, String> {
    control(
        endpoint,
        PUBLICATION_WRITE,
        &PublicationWriteRequest {
            command: command.clone(),
            drop_reply_after_commit: false,
        },
    )
    .await
}

async fn control<Req, Resp>(endpoint: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("connect timed out at {endpoint}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, kind, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<Resp, String> =
        tokio::time::timeout(Duration::from_secs(8), read_response(&mut stream))
            .await
            .map_err(|_| format!("response timed out at {endpoint}"))?
            .map_err(|error| error.to_string())?;
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_set_must_be_non_empty() {
        assert!(PublicationClient::new(Vec::new()).is_err());
        assert!(PublicationClient::new(vec![String::new()]).is_err());
        assert!(PublicationClient::new(vec!["127.0.0.1:1".to_owned()]).is_ok());
    }
}
