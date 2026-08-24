use crate::rpc::NodeStatus;
use crate::rpc::{
    read_response, write_request, ControlWrite, WriteAck, CELL_COMMITTED_ENVELOPE_READ,
    CELL_LOG_SET_POLICY_ACTIVATION_ATTEST, CLIENT_WRITE, LINEARIZABLE_STATUS,
};
use crate::{
    ApplyResponse, CellCommittedEnvelopeFeed, CellCommittedEnvelopeRequest,
    CellLogSetPolicyActivationAttestation, CellLogSetPolicyActivationStatement, CellStateSnapshot,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::net::TcpStream;

const RETRY_ATTEMPTS: usize = 500;

/// Linearizable committed-envelope client for disposable serving workers.
#[derive(Clone, Debug)]
pub struct CellTransactionClient {
    endpoints: Vec<String>,
}

impl CellTransactionClient {
    /// Create a client over one bounded transaction-authority endpoint set.
    ///
    /// # Errors
    ///
    /// Returns an error when no valid endpoint is supplied.
    pub fn new(endpoints: Vec<String>) -> Result<Self, String> {
        if endpoints.is_empty() || endpoints.iter().any(String::is_empty) {
            return Err("transaction client requires non-empty authority endpoints".to_owned());
        }
        Ok(Self { endpoints })
    }

    /// Read one committed-envelope suffix after a linearizability barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when no authority endpoint serves the exact suffix
    /// within the bounded retry budget.
    pub async fn committed_envelopes(
        &self,
        request: &CellCommittedEnvelopeRequest,
    ) -> Result<CellCommittedEnvelopeFeed, String> {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            let endpoint = &self.endpoints[attempt % self.endpoints.len()];
            match request_once(endpoint, request).await {
                Ok(feed) => return Ok(feed),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("committed-envelope feed could not be read: {last}"))
    }

    /// Read the current Cell state after a live authority linearizability barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when no configured authority serves a non-empty Cell
    /// snapshot within the bounded retry budget.
    pub async fn linearizable_snapshot(&self) -> Result<CellStateSnapshot, String> {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            let endpoint = &self.endpoints[attempt % self.endpoints.len()];
            match linearizable_snapshot_once(endpoint).await {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "transaction authority snapshot could not be read: {last}"
        ))
    }

    /// Commit one already encoded application command through the live leader.
    ///
    /// # Errors
    ///
    /// Returns an error when no endpoint commits and returns the replicated
    /// application response within the bounded retry budget.
    pub async fn commit_app_data(&self, app_data: &[u8]) -> Result<ApplyResponse, String> {
        let mut last = String::new();
        let mut observed = BTreeSet::new();
        for attempt in 0..RETRY_ATTEMPTS {
            let endpoint = &self.endpoints[attempt % self.endpoints.len()];
            match commit_once(endpoint, app_data).await {
                Ok(response) => return Ok(response),
                Err(error) => {
                    last.clone_from(&error);
                    if observed.len() < 8 {
                        observed.insert(format!("{endpoint}: {error}"));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "application command could not be committed: {last}; observed={observed:?}"
        ))
    }

    /// Collect distinct authority attestations for one applied log-set policy transition.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested quorum does not attest within the
    /// bounded retry budget.
    pub async fn policy_activation_attestations(
        &self,
        statement: &CellLogSetPolicyActivationStatement,
        quorum: usize,
    ) -> Result<Vec<CellLogSetPolicyActivationAttestation>, String> {
        if quorum == 0 || quorum > self.endpoints.len() {
            return Err("policy activation requires a valid authority quorum".to_owned());
        }
        let mut attestations = BTreeMap::new();
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            let endpoint = &self.endpoints[attempt % self.endpoints.len()];
            match activation_attest_once(endpoint, statement).await {
                Ok(attestation) => {
                    attestations.insert(attestation.signer_id, attestation);
                    if attestations.len() >= quorum {
                        return Ok(attestations.into_values().collect());
                    }
                }
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "policy activation authority quorum did not attest: {last}"
        ))
    }
}

async fn linearizable_snapshot_once(endpoint: &str) -> Result<CellStateSnapshot, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("transaction authority connect timed out at {endpoint}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, LINEARIZABLE_STATUS, &())
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<NodeStatus, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("transaction authority snapshot timed out at {endpoint}"))?
            .map_err(|error| error.to_string())?;
    response?
        .cells
        .into_iter()
        .next()
        .ok_or_else(|| "transaction authority returned no Cell snapshot".to_owned())
}

async fn activation_attest_once(
    endpoint: &str,
    statement: &CellLogSetPolicyActivationStatement,
) -> Result<CellLogSetPolicyActivationAttestation, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("transaction authority connect timed out at {endpoint}"))?
        .map_err(|error| error.to_string())?;
    write_request(
        &mut stream,
        CELL_LOG_SET_POLICY_ACTIVATION_ATTEST,
        statement,
    )
    .await
    .map_err(|error| error.to_string())?;
    let response: Result<CellLogSetPolicyActivationAttestation, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("policy activation attest timed out at {endpoint}"))?
            .map_err(|error| error.to_string())?;
    response
}

async fn request_once(
    endpoint: &str,
    request: &CellCommittedEnvelopeRequest,
) -> Result<CellCommittedEnvelopeFeed, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("transaction authority connect timed out at {endpoint}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, CELL_COMMITTED_ENVELOPE_READ, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<CellCommittedEnvelopeFeed, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("transaction authority read timed out at {endpoint}"))?
            .map_err(|error| error.to_string())?;
    response
}

async fn commit_once(endpoint: &str, app_data: &[u8]) -> Result<ApplyResponse, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("transaction authority connect timed out at {endpoint}"))?
        .map_err(|error| error.to_string())?;
    write_request(
        &mut stream,
        CLIENT_WRITE,
        &ControlWrite {
            app_data: app_data.to_vec(),
            drop_reply_after_commit: false,
            credential: None,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let response: Result<WriteAck, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("transaction authority write timed out at {endpoint}"))?
            .map_err(|error| error.to_string())?;
    response?
        .response
        .ok_or_else(|| "transaction authority omitted application response".to_owned())
}
