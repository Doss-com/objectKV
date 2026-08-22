use okv_model::{ApplyOutcome, CommitBatch, Model, Mutation, Version};
use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let command = env::args().nth(1);
    if command.as_deref() == Some("smoke") {
        smoke()
    } else {
        eprintln!("usage: cargo run -p okv-eval -- smoke");
        ExitCode::from(2)
    }
}

fn smoke() -> ExitCode {
    match run_smoke() {
        Ok(()) => {
            println!(
                "{{\"schema_version\":1,\"suite\":\"smoke\",\"status\":\"pass\",\"correctness_failures\":0}}"
            );
            ExitCode::SUCCESS
        }
        Err(message) => {
            println!(
                "{{\"schema_version\":1,\"suite\":\"smoke\",\"status\":\"fail\",\"correctness_failures\":1,\"message\":\"{message}\"}}"
            );
            ExitCode::FAILURE
        }
    }
}

fn run_smoke() -> Result<(), String> {
    let mut model = Model::default();
    let batch = CommitBatch {
        version: Version::new(1),
        mutations: vec![Mutation::Set {
            key: b"inventory/sku-1".to_vec(),
            value: b"10".to_vec(),
        }],
    };

    if model
        .apply(batch.clone())
        .map_err(|error| error.to_string())?
        != ApplyOutcome::Applied
    {
        return Err("initial commit was not applied".to_owned());
    }

    if model.apply(batch).map_err(|error| error.to_string())? != ApplyOutcome::AlreadyApplied {
        return Err("exact replay was not idempotent".to_owned());
    }

    if model
        .get(b"inventory/sku-1", Version::new(1))
        .map_err(|error| error.to_string())?
        != Some(&b"10"[..])
    {
        return Err("snapshot read returned the wrong value".to_owned());
    }

    Ok(())
}
