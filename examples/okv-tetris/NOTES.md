# Prototype findings

The first executable boundary uses one transaction per game input, an MVCC
state key plus materialized cell range, and one application event. It exercises
point reads, range reads, range clear precedence, stable request identities,
log append, replay recovery, snapshots, and branch-local histories.

Open question for the next iteration: should application-log emission be an
explicit field of `TransactRequest`, as frozen here, or a reserved-key mutation
compiled by a higher layer?

