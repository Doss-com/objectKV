use clap::{Parser, ValueEnum};
use okv_object::{
    filesystem_backend, gcs_backend_from_env, memory_backend, minio_backend_from_env,
    run_conformance, validate_conformance_report, ConformanceOptions, ConformanceProfile,
};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BackendArg {
    Memory,
    Filesystem,
    Minio,
    Gcs,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProfileArg {
    Segment,
    Authority,
}

impl From<ProfileArg> for ConformanceProfile {
    fn from(value: ProfileArg) -> Self {
        match value {
            ProfileArg::Segment => Self::Segment,
            ProfileArg::Authority => Self::Authority,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "okv-object",
    about = "objectKV object-store conformance runner"
)]
struct Cli {
    #[arg(long, value_enum, default_value = "memory")]
    backend: BackendArg,
    #[arg(long, value_enum, default_value = "authority")]
    profile: ProfileArg,
    #[arg(long, default_value = ".okv/object-store-fixture")]
    filesystem_root: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, hide = true)]
    inject_immutable_overwrite_bug: bool,
    #[arg(long, hide = true)]
    inject_list_authority_bug: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match execute(Cli::parse()).await {
        Ok(passed) => {
            if passed {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            }
        }
        Err(error) => {
            eprintln!("okv-object: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<bool, Box<dyn std::error::Error>> {
    let backend = match cli.backend {
        BackendArg::Memory => memory_backend(),
        BackendArg::Filesystem => filesystem_backend(&cli.filesystem_root)?,
        BackendArg::Minio => minio_backend_from_env()?,
        BackendArg::Gcs => gcs_backend_from_env()?,
    };
    let report = run_conformance(
        backend,
        cli.profile.into(),
        &ConformanceOptions {
            inject_immutable_overwrite_bug: cli.inject_immutable_overwrite_bug,
            inject_list_authority_bug: cli.inject_list_authority_bug,
        },
    )
    .await;
    validate_conformance_report(&report)?;
    let rendered = format!("{}\n", serde_json::to_string_pretty(&report)?);
    if let Some(output) = cli.output {
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output, &rendered)?;
    }
    print!("{rendered}");
    Ok(report.passed())
}
