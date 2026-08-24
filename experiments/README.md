# Experiment records

`ledger.jsonl` is the append-only compact record of autonomous and human research
runs. Raw logs and large results live outside the repository and are referenced
by content hash or durable artifact URL.

Rules:

- append one schema-valid JSON object per completed attempt;
- never edit or delete an older row;
- supersede a wrong result with a later row that references it;
- keep discarded candidate commits reachable;
- never record credentials, signed URLs, bucket secrets, or private payloads.

The ledger is created by the first admitted benchmark run. Do not add a fake
baseline during repository bootstrap.
