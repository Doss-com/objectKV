use okv_postgres::{run_postgres_transaction_authority, PostgresTransactionAuthorityHarnessConfig};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let Some(flag) = arguments.next() else {
        fail("usage: okv-postgres-transaction-authority --config-json <json>");
    };
    if flag != "--config-json" {
        fail("usage: okv-postgres-transaction-authority --config-json <json>");
    }
    let Some(config_json) = arguments.next() else {
        fail("missing --config-json value");
    };
    if arguments.next().is_some() {
        fail("unexpected transaction-authority argument");
    }
    let config: PostgresTransactionAuthorityHarnessConfig =
        serde_json::from_str(&config_json).unwrap_or_else(|error| fail(&error.to_string()));
    if let Err(error) = run_postgres_transaction_authority(config) {
        fail(&error);
    }
}

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}
