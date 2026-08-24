//! Synchronous `PostgreSQL` storage-manager read bridge into objectKV.

use crate::{
    PostgresPage, PostgresPageIdentity, PostgresPageReadSnapshot, PostgresPageReader,
    PostgresRelationExtent, PostgresRelationForkIdentity, POSTGRES_PAGE_SIZE,
};
use async_trait::async_trait;
use okv_consensus::CellMutation;
use okv_object::{
    build_fixture_range_serving_state, serve_range_read_listener, ClientRangeMapSnapshot,
    ClientRangeRoute, KvReadClient, KvReadClientConfig, KvReadRouter, KvReadRouterConfig,
    RangeEngineId, RangeMapSource, RangeReadAssignment, RangeReadProtocolConfig,
    RANGE_SERVING_FIXTURE_CELL_ID, RANGE_SERVING_FIXTURE_TENANT_ID,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const REQUEST_MAGIC: &[u8; 8] = b"OKVPG001";
const RESPONSE_MAGIC: &[u8; 8] = b"OKVPGR01";
const REQUEST_HEADER_BYTES: usize = 52;
const RESPONSE_HEADER_BYTES: usize = 20;
const RESPONSE_OK: u32 = 0;
const RESPONSE_ERROR: u32 = 1;
const RANGE_ID: RangeEngineId = RangeEngineId(101);
const ROUTING_EPOCH: u64 = 1;
const MAP_VERSION: u64 = 1;
const MAXIMUM_ERROR_BYTES: usize = 4 * 1024;

/// One exact relation file imported into a fixed objectKV read view.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresSmgrPageServiceConfig {
    pub seed: u64,
    pub listen_address: String,
    pub source_file: PathBuf,
    pub cluster_id: [u8; 16],
    pub tablespace_oid: u32,
    pub database_oid: u32,
    pub relation_number: u32,
    pub temporary_backend_id: u32,
    pub fork_number: u8,
    pub objectkv_version: u64,
    pub maximum_page_lsn: u64,
    pub maximum_blocks_per_read: usize,
}

/// Machine-readable service identity printed before accepting bridge reads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresSmgrPageServiceReady {
    pub listen_address: String,
    pub source_file: PathBuf,
    pub relation_number: u32,
    pub objectkv_version: u64,
    pub maximum_page_lsn: u64,
    pub imported_pages: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SmgrPageReadRequest {
    tablespace_oid: u32,
    database_oid: u32,
    relation_number: u32,
    temporary_backend_id: u32,
    fork_number: u32,
    first_block: u32,
    block_count: u32,
    objectkv_version: u64,
    maximum_page_lsn: u64,
}

struct StaticRangeMapSource {
    snapshot: ClientRangeMapSnapshot,
}

#[async_trait]
impl RangeMapSource for StaticRangeMapSource {
    async fn snapshot(
        &self,
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
    ) -> Result<ClientRangeMapSnapshot, String> {
        if cell_id != RANGE_SERVING_FIXTURE_CELL_ID || tenant_id != RANGE_SERVING_FIXTURE_TENANT_ID
        {
            return Err("storage-manager bridge received the wrong session identity".to_owned());
        }
        Ok(self.snapshot.clone())
    }
}

/// Import one real relation file and serve bounded synchronous page reads.
///
/// The imported objectKV view is immutable for the life of this process. This
/// is a read-callback seam probe, not a write path or a durability claim.
///
/// # Errors
///
/// Returns an error for invalid configuration, relation-page import failure,
/// objectKV construction failure, or listener failure.
pub fn run_postgres_smgr_page_service(config: PostgresSmgrPageServiceConfig) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(serve(config))
}

async fn serve(config: PostgresSmgrPageServiceConfig) -> Result<(), String> {
    validate_config(&config)?;
    let (mutations, imported_pages) = import_relation_pages(&config)?;
    let state = build_fixture_range_serving_state(
        config.seed,
        config.objectkv_version,
        config.objectkv_version,
        &mutations,
    )
    .await?;
    let range_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let range_address = range_listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .to_string();
    let router = Arc::new(KvReadRouter::new(KvReadRouterConfig {
        cell_id: RANGE_SERVING_FIXTURE_CELL_ID,
        max_in_flight: 32,
        max_key_bytes: 256,
        max_scan_rows: config.maximum_blocks_per_read,
    })?);
    router.assign(
        RangeReadAssignment {
            tenant_id: RANGE_SERVING_FIXTURE_TENANT_ID,
            range_id: RANGE_ID,
            routing_epoch: ROUTING_EPOCH,
            start: vec![0],
            end: vec![0xff],
        },
        state,
    )?;
    let protocol = range_protocol(config.maximum_blocks_per_read)?;
    tokio::spawn(serve_range_read_listener(range_listener, protocol, router));

    let route = ClientRangeRoute {
        endpoint: range_address,
        range_id: RANGE_ID,
        routing_epoch: ROUTING_EPOCH,
        start: vec![0],
        end: vec![0xff],
    };
    let snapshot = ClientRangeMapSnapshot {
        cell_id: RANGE_SERVING_FIXTURE_CELL_ID,
        tenant_id: RANGE_SERVING_FIXTURE_TENANT_ID,
        map_version: MAP_VERSION,
        routes: vec![route],
    };
    let source = Arc::new(StaticRangeMapSource {
        snapshot: snapshot.clone(),
    });
    let client = Arc::new(
        KvReadClient::new(
            RANGE_SERVING_FIXTURE_CELL_ID,
            RANGE_SERVING_FIXTURE_TENANT_ID,
            KvReadClientConfig {
                protocol,
                max_route_refreshes: 1,
            },
            snapshot,
            source,
        )
        .map_err(|error| error.to_string())?,
    );
    let reader = Arc::new(PostgresPageReader::new(client));
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(|error| error.to_string())?;
    let ready = PostgresSmgrPageServiceReady {
        listen_address: listener
            .local_addr()
            .map_err(|error| error.to_string())?
            .to_string(),
        source_file: config.source_file.clone(),
        relation_number: config.relation_number,
        objectkv_version: config.objectkv_version,
        maximum_page_lsn: config.maximum_page_lsn,
        imported_pages,
    };
    println!(
        "{}",
        serde_json::to_string(&ready).map_err(|error| error.to_string())?
    );

    loop {
        let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        let reader = Arc::clone(&reader);
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream, &config, &reader).await {
                eprintln!("objectKV PostgreSQL page request failed: {error}");
            }
        });
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    config: &PostgresSmgrPageServiceConfig,
    reader: &PostgresPageReader,
) -> Result<(), String> {
    let mut header = [0_u8; REQUEST_HEADER_BYTES];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| error.to_string())?;
    let result = parse_request(&header).and_then(|request| validate_request(config, request));
    let request = match result {
        Ok(request) => request,
        Err(error) => return write_error(&mut stream, &error).await,
    };
    let first = PostgresPageIdentity {
        cluster_id: config.cluster_id,
        tablespace_oid: request.tablespace_oid,
        database_oid: request.database_oid,
        relation_number: request.relation_number,
        temporary_backend_id: request.temporary_backend_id,
        fork_number: u8::try_from(request.fork_number)
            .map_err(|_| "fork number does not fit the bridge key".to_owned())?,
        block_number: request.first_block,
    };
    let snapshot = PostgresPageReadSnapshot {
        objectkv_version: request.objectkv_version,
        maximum_page_lsn: request.maximum_page_lsn,
    };
    let count = usize::try_from(request.block_count)
        .map_err(|_| "block count does not fit this process".to_owned())?;
    match reader.read_pages(first, count, snapshot).await {
        Ok(pages) => write_pages(&mut stream, &pages).await,
        Err(error) => write_error(&mut stream, &error.to_string()).await,
    }
}

fn validate_config(config: &PostgresSmgrPageServiceConfig) -> Result<(), String> {
    if config.listen_address.is_empty()
        || config.objectkv_version == 0
        || config.maximum_blocks_per_read == 0
        || config.maximum_blocks_per_read > 128
    {
        return Err(
            "page service requires an address, nonzero version, and 1..=128 read bound".to_owned(),
        );
    }
    Ok(())
}

fn import_relation_pages(
    config: &PostgresSmgrPageServiceConfig,
) -> Result<(BTreeMap<u64, Vec<CellMutation>>, usize), String> {
    let bytes = fs::read(&config.source_file).map_err(|error| error.to_string())?;
    if bytes.is_empty() || bytes.len() % POSTGRES_PAGE_SIZE != 0 {
        return Err(format!(
            "relation file length {} is not a positive multiple of {}",
            bytes.len(),
            POSTGRES_PAGE_SIZE
        ));
    }
    let mut pages = Vec::with_capacity(bytes.len() / POSTGRES_PAGE_SIZE);
    for (block, page_bytes) in bytes.chunks_exact(POSTGRES_PAGE_SIZE).enumerate() {
        let block_number = u32::try_from(block)
            .map_err(|_| "relation has more blocks than the bridge key supports".to_owned())?;
        let page_lsn = read_page_lsn(page_bytes)?;
        if page_lsn > config.maximum_page_lsn {
            return Err(format!(
                "relation block {block_number} page LSN {page_lsn} exceeds configured frontier {}",
                config.maximum_page_lsn
            ));
        }
        let postgres_checksum = read_native_u16(page_bytes, 8)?;
        let page = PostgresPage::new(page_lsn, postgres_checksum, page_bytes.to_vec())
            .map_err(|error| error.to_string())?;
        let identity = PostgresPageIdentity {
            cluster_id: config.cluster_id,
            tablespace_oid: config.tablespace_oid,
            database_oid: config.database_oid,
            relation_number: config.relation_number,
            temporary_backend_id: config.temporary_backend_id,
            fork_number: config.fork_number,
            block_number,
        };
        pages.push(CellMutation::Set {
            key: identity.encode_key(),
            value: page.encode(),
        });
    }
    let imported_pages = pages.len();
    let extent = PostgresRelationForkIdentity {
        cluster_id: config.cluster_id,
        tablespace_oid: config.tablespace_oid,
        database_oid: config.database_oid,
        relation_number: config.relation_number,
        temporary_backend_id: config.temporary_backend_id,
        fork_number: config.fork_number,
    };
    pages.push(CellMutation::Set {
        key: extent.encode_extent_key(),
        value: PostgresRelationExtent {
            nblocks: u32::try_from(imported_pages)
                .map_err(|_| "relation block count does not fit the extent value".to_owned())?,
        }
        .encode(),
    });
    let mut mutations = BTreeMap::new();
    for version in 1..config.objectkv_version {
        mutations.insert(version, Vec::new());
    }
    mutations.insert(config.objectkv_version, pages);
    Ok((mutations, imported_pages))
}

fn parse_request(header: &[u8; REQUEST_HEADER_BYTES]) -> Result<SmgrPageReadRequest, String> {
    if &header[..8] != REQUEST_MAGIC {
        return Err("invalid storage-manager request magic".to_owned());
    }
    Ok(SmgrPageReadRequest {
        tablespace_oid: read_be_u32(header, 8)?,
        database_oid: read_be_u32(header, 12)?,
        relation_number: read_be_u32(header, 16)?,
        temporary_backend_id: read_be_u32(header, 20)?,
        fork_number: read_be_u32(header, 24)?,
        first_block: read_be_u32(header, 28)?,
        block_count: read_be_u32(header, 32)?,
        objectkv_version: read_be_u64(header, 36)?,
        maximum_page_lsn: read_be_u64(header, 44)?,
    })
}

fn validate_request(
    config: &PostgresSmgrPageServiceConfig,
    request: SmgrPageReadRequest,
) -> Result<SmgrPageReadRequest, String> {
    let block_count = usize::try_from(request.block_count)
        .map_err(|_| "block count does not fit this process".to_owned())?;
    if request.tablespace_oid != config.tablespace_oid
        || request.database_oid != config.database_oid
        || request.relation_number != config.relation_number
        || request.temporary_backend_id != config.temporary_backend_id
        || request.fork_number != u32::from(config.fork_number)
    {
        return Err("storage-manager request relation identity mismatch".to_owned());
    }
    if request.objectkv_version != config.objectkv_version
        || request.maximum_page_lsn != config.maximum_page_lsn
    {
        return Err("storage-manager request changed the fixed read frontier".to_owned());
    }
    if block_count == 0 || block_count > config.maximum_blocks_per_read {
        return Err(format!(
            "storage-manager request exceeds block bound {}",
            config.maximum_blocks_per_read
        ));
    }
    request
        .first_block
        .checked_add(request.block_count)
        .ok_or_else(|| "storage-manager block range overflows".to_owned())?;
    Ok(request)
}

async fn write_pages(stream: &mut TcpStream, pages: &[PostgresPage]) -> Result<(), String> {
    let page_count = u32::try_from(pages.len()).map_err(|error| error.to_string())?;
    let payload_bytes = pages
        .len()
        .checked_mul(POSTGRES_PAGE_SIZE)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or_else(|| "page response length overflows protocol".to_owned())?;
    write_response_header(stream, RESPONSE_OK, page_count, payload_bytes).await?;
    for page in pages {
        stream
            .write_all(&page.bytes)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn write_error(stream: &mut TcpStream, error: &str) -> Result<(), String> {
    let error = error.as_bytes();
    let bounded = &error[..error.len().min(MAXIMUM_ERROR_BYTES)];
    let payload_bytes = u32::try_from(bounded.len()).map_err(|error| error.to_string())?;
    write_response_header(stream, RESPONSE_ERROR, 0, payload_bytes).await?;
    stream
        .write_all(bounded)
        .await
        .map_err(|error| error.to_string())
}

async fn write_response_header(
    stream: &mut TcpStream,
    status: u32,
    page_count: u32,
    payload_bytes: u32,
) -> Result<(), String> {
    let mut header = [0_u8; RESPONSE_HEADER_BYTES];
    header[..8].copy_from_slice(RESPONSE_MAGIC);
    header[8..12].copy_from_slice(&status.to_be_bytes());
    header[12..16].copy_from_slice(&page_count.to_be_bytes());
    header[16..20].copy_from_slice(&payload_bytes.to_be_bytes());
    stream
        .write_all(&header)
        .await
        .map_err(|error| error.to_string())
}

fn read_page_lsn(page: &[u8]) -> Result<u64, String> {
    let high = u64::from(read_native_u32(page, 0)?);
    let low = u64::from(read_native_u32(page, 4)?);
    Ok((high << 32) | low)
}

fn read_native_u16(bytes: &[u8], offset: usize) -> Result<u16, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(2))
        .ok_or_else(|| "truncated PostgreSQL page header".to_owned())?;
    Ok(u16::from_ne_bytes([bytes[0], bytes[1]]))
}

fn read_native_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| "truncated PostgreSQL page header".to_owned())?;
    Ok(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_be_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| "truncated storage-manager request".to_owned())?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_be_u64(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(8))
        .ok_or_else(|| "truncated storage-manager request".to_owned())?;
    Ok(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn range_protocol(maximum_blocks_per_read: usize) -> Result<RangeReadProtocolConfig, String> {
    let max_frame_bytes = maximum_blocks_per_read
        .checked_mul(POSTGRES_PAGE_SIZE + 256)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| "range-read frame bound overflows".to_owned())?;
    Ok(RangeReadProtocolConfig {
        max_frame_bytes,
        request_timeout_millis: 5_000,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fixed_width_storage_manager_request() {
        let mut request = [0_u8; REQUEST_HEADER_BYTES];
        request[..8].copy_from_slice(REQUEST_MAGIC);
        request[8..12].copy_from_slice(&1663_u32.to_be_bytes());
        request[12..16].copy_from_slice(&5_u32.to_be_bytes());
        request[16..20].copy_from_slice(&16_384_u32.to_be_bytes());
        request[20..24].copy_from_slice(&0_u32.to_be_bytes());
        request[24..28].copy_from_slice(&0_u32.to_be_bytes());
        request[28..32].copy_from_slice(&7_u32.to_be_bytes());
        request[32..36].copy_from_slice(&3_u32.to_be_bytes());
        request[36..44].copy_from_slice(&11_u64.to_be_bytes());
        request[44..52].copy_from_slice(&900_u64.to_be_bytes());
        assert_eq!(
            parse_request(&request).unwrap(),
            SmgrPageReadRequest {
                tablespace_oid: 1663,
                database_oid: 5,
                relation_number: 16_384,
                temporary_backend_id: 0,
                fork_number: 0,
                first_block: 7,
                block_count: 3,
                objectkv_version: 11,
                maximum_page_lsn: 900,
            }
        );
    }

    #[test]
    fn reads_postgres_split_lsn_in_native_order() {
        let mut page = vec![0_u8; POSTGRES_PAGE_SIZE];
        page[..4].copy_from_slice(&0x1234_5678_u32.to_ne_bytes());
        page[4..8].copy_from_slice(&0x90ab_cdef_u32.to_ne_bytes());
        assert_eq!(read_page_lsn(&page).unwrap(), 0x1234_5678_90ab_cdef);
    }
}
