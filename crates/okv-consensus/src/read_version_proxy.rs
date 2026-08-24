use crate::rpc::{
    read_frame, read_response, write_request, write_response, NodeStatus, LINEARIZABLE_STATUS,
};
use crate::{CellReadVersion, CellStateSnapshot};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

const READ_VERSION_REQUEST: u8 = 41;
const RETRY_ATTEMPTS: usize = 500;
const CELL_ID: [u8; 16] = [0x11; 16];
const TENANT_ID: [u8; 16] = [0x22; 16];

/// Configuration for one bounded read-version proxy process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadVersionProxyProcessConfig {
    pub proxy_id: u64,
    pub listen_address: String,
    pub authority_addresses: Vec<String>,
    pub ignore_session_minimum: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ReadVersionProxyRequest {
    minimum: CellReadVersion,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReadVersionProxyReply {
    pub proxy_id: u64,
    pub snapshot: CellStateSnapshot,
}

/// Run one independent read-version proxy process until its controller stops it.
///
/// # Errors
///
/// Returns an error when binding, accepting, decoding, or responding fails.
pub async fn run_read_version_proxy_process(
    config: ReadVersionProxyProcessConfig,
) -> Result<(), String> {
    if config.authority_addresses.is_empty() {
        return Err("read-version proxy requires at least one authority address".to_owned());
    }
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(|error| error.to_string())?;
    let mut cache = None;
    let mut preferred_authority = 0_usize;
    loop {
        let (mut stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        let result =
            handle_proxy_request(&mut stream, &config, &mut cache, &mut preferred_authority).await;
        write_response(&mut stream, &result)
            .await
            .map_err(|error| error.to_string())?;
    }
}

async fn handle_proxy_request(
    stream: &mut TcpStream,
    config: &ReadVersionProxyProcessConfig,
    cache: &mut Option<CellStateSnapshot>,
    preferred_authority: &mut usize,
) -> Result<ReadVersionProxyReply, String> {
    let kind = stream.read_u8().await.map_err(|error| error.to_string())?;
    if kind != READ_VERSION_REQUEST {
        return Err(format!("unknown read-version proxy request kind {kind}"));
    }
    let body = read_frame(stream)
        .await
        .map_err(|error| error.to_string())?;
    let request = serde_json::from_slice::<ReadVersionProxyRequest>(&body)
        .map_err(|error| error.to_string())?;
    let snapshot = if config.ignore_session_minimum {
        if let Some(snapshot) = cache.clone() {
            snapshot
        } else {
            let snapshot = query_authority_snapshot(
                &config.authority_addresses,
                preferred_authority,
                request.minimum,
            )
            .await?;
            *cache = Some(snapshot.clone());
            snapshot
        }
    } else {
        let snapshot = query_authority_snapshot(
            &config.authority_addresses,
            preferred_authority,
            request.minimum,
        )
        .await?;
        *cache = Some(snapshot.clone());
        snapshot
    };
    Ok(ReadVersionProxyReply {
        proxy_id: config.proxy_id,
        snapshot,
    })
}

async fn query_authority_snapshot(
    addresses: &[String],
    preferred_authority: &mut usize,
    minimum: CellReadVersion,
) -> Result<CellStateSnapshot, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        for offset in 0..addresses.len() {
            let index = (*preferred_authority + offset) % addresses.len();
            match linearizable_authority_snapshot(&addresses[index]).await {
                Ok(snapshot) if version_at_least(read_version_of(&snapshot), minimum) => {
                    *preferred_authority = index;
                    return Ok(snapshot);
                }
                Ok(snapshot) => {
                    let observed = read_version_of(&snapshot);
                    last = format!(
                        "authority returned ({}, {}) below minimum ({}, {})",
                        observed.generation,
                        observed.sequence,
                        minimum.generation,
                        minimum.sequence
                    );
                }
                Err(error) => last = error,
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "read-version proxy could not obtain causal floor: {last}"
    ))
}

async fn linearizable_authority_snapshot(address: &str) -> Result<CellStateSnapshot, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .map_err(|_| format!("connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, LINEARIZABLE_STATUS, &())
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<NodeStatus, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    let status = response?;
    Ok(status.cells.first().cloned().unwrap_or(CellStateSnapshot {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: 1,
        ..CellStateSnapshot::default()
    }))
}

pub(crate) async fn request_read_version_proxy(
    address: &str,
    minimum: CellReadVersion,
) -> Result<ReadVersionProxyReply, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .map_err(|_| format!("proxy connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(
        &mut stream,
        READ_VERSION_REQUEST,
        &ReadVersionProxyRequest { minimum },
    )
    .await
    .map_err(|error| error.to_string())?;
    let response: Result<ReadVersionProxyReply, String> =
        tokio::time::timeout(Duration::from_secs(5), read_response(&mut stream))
            .await
            .map_err(|_| format!("proxy response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

fn read_version_of(snapshot: &CellStateSnapshot) -> CellReadVersion {
    CellReadVersion {
        generation: snapshot.generation,
        sequence: snapshot.latest_sequence,
    }
}

fn version_at_least(version: CellReadVersion, minimum: CellReadVersion) -> bool {
    version.generation > minimum.generation
        || (version.generation == minimum.generation && version.sequence >= minimum.sequence)
}
