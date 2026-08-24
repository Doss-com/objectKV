use clap::{Parser, Subcommand};
use okv_sim::run_generation_fencing;
use std::error::Error;
use std::process::Command;

#[derive(Debug, Parser)]
#[command(name = "okv-sim")]
#[command(about = "Deterministic objectKV simulation and replay probe")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Emit one canonical generation-fencing trace.
    Replay {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long)]
        source_revision: Option<String>,
        #[arg(long)]
        inject_stale_publication_bug: bool,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    match Cli::parse().command {
        Commands::Replay {
            seed,
            source_revision,
            inject_stale_publication_bug,
        } => {
            let source_revision = source_revision.unwrap_or_else(current_revision);
            let trace =
                run_generation_fencing(seed, &source_revision, inject_stale_publication_bug)
                    .map_err(io_error)?;
            println!("{}", serde_json::to_string_pretty(&trace)?);
            if !trace.passed() {
                std::process::exit(2);
            }
        }
    }
    Ok(())
}

fn current_revision() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|revision| revision.trim().to_owned())
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn io_error(message: String) -> std::io::Error {
    std::io::Error::other(message)
}
