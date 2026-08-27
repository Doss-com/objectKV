# Superseded G4.4 diagnostic

These receipts are retained as a failed experiment record. The implementation
started `recovery.first_correct_read_duration` after catch-up and optional full
hydration, so the latency values measured only the final point reads and did not
match the metric contract.

`serving-recovery-g4.4-v2` moves the timer to worker-process entry and is the
only G4.4 receipt set cited by status or performance claims.
