//! External transaction-authority harness for disposable `PostgreSQL` page services.

use okv_consensus::{CellProcessFixture, CellProcessPrototypeMode};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Stable endpoint identity consumed by one or more disposable page services.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresTransactionAuthorityConfig {
    pub endpoints: Vec<String>,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
}

/// Configuration for the bounded three-process transaction-authority harness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresTransactionAuthorityHarnessConfig {
    pub seed: u64,
    pub status_file: PathBuf,
    pub process_executable: PathBuf,
}

/// Machine-readable receipt emitted after the external authority is ready.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresTransactionAuthorityStatus {
    pub endpoints: Vec<String>,
    pub process_count: usize,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub latest_sequence: u64,
}

/// Run a bounded three-process Cell authority until the harness exits.
///
/// # Errors
///
/// Returns an error for invalid paths, process bootstrap failure, an anomalous
/// baseline history, or status publication failure.
pub fn run_postgres_transaction_authority(
    config: PostgresTransactionAuthorityHarnessConfig,
) -> Result<(), String> {
    if config.seed == 0
        || config.status_file.as_os_str().is_empty()
        || !config.process_executable.is_file()
    {
        return Err("PostgreSQL transaction-authority configuration is invalid".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async move {
        let mut fixture = CellProcessFixture::start(
            config.seed,
            CellProcessPrototypeMode::Correct,
            &config.process_executable,
        )?;
        let baseline = fixture.run_history().await?;
        if baseline.anomaly_count != 0 {
            return Err("PostgreSQL transaction-authority baseline has anomalies".to_owned());
        }
        let snapshot = fixture.linearizable_cell_snapshot().await?;
        let status = PostgresTransactionAuthorityStatus {
            endpoints: fixture.endpoints(),
            process_count: 3,
            cell_id: snapshot.cell_id,
            tenant_id: snapshot.tenant_id,
            generation: snapshot.generation,
            latest_sequence: snapshot.latest_sequence,
        };
        persist_json(&config.status_file, &status)?;
        println!(
            "{}",
            serde_json::to_string(&status).map_err(|error| error.to_string())?
        );
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), String>(())
    })
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&serde_json::to_vec(value).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
