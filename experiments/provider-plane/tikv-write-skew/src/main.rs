//! Live TiKV write-skew probe for RFC-0041.
//!
//! The expected result is that both optimistic transactions commit under
//! TiKV's documented snapshot isolation. That is a valid TiKV behavior and a
//! knockout failure for objectKV's strict-serializable transaction plane.

use serde_json::json;
use std::env;
use std::error::Error;
use std::time::Instant;
use tikv_client::TransactionClient;

const SERVER_REVISION: &str = "tikv-8.5.7@3f446cfa9eb1d5c653031d261e185911495d0359";
const CLIENT_REVISION: &str = "tikv-client@88688d6eb3a55a864885d7bccc8abf428dce076c";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let endpoint = arguments
        .next()
        .unwrap_or_else(|| "127.0.0.1:2379".to_owned());
    let run_id = arguments.next().ok_or("run id is required")?;
    let prefix = format!("\u{2}okv-provider-preflight/{run_id}/skew/").into_bytes();
    let mut left = prefix.clone();
    left.extend_from_slice(b"left");
    let mut right = prefix;
    right.extend_from_slice(b"right");

    let started = Instant::now();
    let client = TransactionClient::new(vec![endpoint]).await?;
    let mut first = client.begin_optimistic().await?;
    let mut second = client.begin_optimistic().await?;

    let first_left = first.get(left.clone()).await?;
    let first_right = first.get(right.clone()).await?;
    let second_left = second.get(left.clone()).await?;
    let second_right = second.get(right.clone()).await?;
    if first_left.is_some()
        || first_right.is_some()
        || second_left.is_some()
        || second_right.is_some()
    {
        return Err("write-skew keys were not empty".into());
    }

    first.put(left, b"committed".to_vec()).await?;
    second.put(right, b"also-committed".to_vec()).await?;
    let first_commit = first.commit().await;
    let second_commit = second.commit().await;
    let committed = u64::from(first_commit.is_ok()) + u64::from(second_commit.is_ok());
    let strict_serializable = committed <= 1;

    let receipt = json!({
        "schema_version": 1,
        "kind": "tikv_write_skew_preflight",
        "server": SERVER_REVISION,
        "client": CLIENT_REVISION,
        "run_id": run_id,
        "duration_ns": started.elapsed().as_nanos(),
        "transactions_committed": committed,
        "strict_serializable_write_skew": strict_serializable,
        "provider_behavior_matches_documented_snapshot_isolation": committed == 2,
        "eligible_for_objectkv_transaction_plane": strict_serializable,
        "first_commit_error": first_commit.err().map(|error| error.to_string()),
        "second_commit_error": second_commit.err().map(|error| error.to_string()),
        "scope": "single-node R0 semantic negative, not HA or production durability"
    });
    println!("{}", serde_json::to_string_pretty(&receipt)?);
    Ok(())
}
