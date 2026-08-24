# PostgreSQL storage-manager dispatch probe

Status: `[COMPLETE]` on 2026-08-23. This is a fork-seam probe, not an
object-storage backend.

## Result

A source patch against PostgreSQL tag `REL_18_6`, exact commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`, adds a second static `f_smgr`
dispatch slot. `OKV_SMGR_PROBE=1` selects that slot before catalog access. The
slot deliberately delegates every operation to the existing `md` manager.

The patched source passed:

- configure with `--without-icu --without-readline --without-zlib`;
- parallel compile and install;
- `initdb`, including bootstrap catalog creation through slot 1;
- server boot and catalog access;
- heap and B-tree create, insert, update, and rollback;
- checkpoint and clean shutdown;
- restart and exact query result `1=alpha`, `2=gamma`.

The server log contained both the probe marker and `database system is ready to
accept connections` on the initial boot and restart.

## Commands

The build used separate source, build, install, data, and socket directories.
After applying `postgres-18.6-smgr-dispatch.patch` to the exact upstream commit:

```sh
src=$PWD
build=/tmp/okv-postgres-build-724edf9
install=/tmp/okv-postgres-install-724edf9
data=/tmp/okv-postgres-probe-data-724edf9
socket=/tmp/okv-postgres-probe-socket-724edf9

mkdir -p "$build" "$socket"
cd "$build"
"$src/configure" --prefix="$install" \
  --without-icu --without-readline --without-zlib
make -j8
make install

OKV_SMGR_PROBE=1 "$install/bin/initdb" -D "$data"
OKV_SMGR_PROBE=1 "$install/bin/pg_ctl" -D "$data" \
  -l /tmp/okv-postgres-probe-724edf9.log \
  -o "-k $socket -p 55439" start
```

The SQL lifecycle was sent as three separate `psql -v ON_ERROR_STOP=1` calls so
the explicit rollback could not roll back table creation:

```sql
CREATE TABLE okv_probe (id bigint PRIMARY KEY, value text NOT NULL);
INSERT INTO okv_probe VALUES (1, 'alpha'), (2, 'beta');

BEGIN;
UPDATE okv_probe SET value = 'rolled-back' WHERE id = 1;
ROLLBACK;

UPDATE okv_probe SET value = 'gamma' WHERE id = 2;
CHECKPOINT;
SELECT id, value FROM okv_probe ORDER BY id;
```

The server was stopped, restarted with the same probe setting, and checked with:

```sql
SELECT id || '=' || value FROM okv_probe ORDER BY id;
```

## Admission boundary

This proves that a pinned PostgreSQL fork can select a second storage-manager
slot early enough for `initdb`, startup, ordinary relations, checkpoint, and
restart. It also proves the fork surface compiles against the complete PG18
`f_smgr` callback table.

It does not prove:

- objectKV reads, writes, or stable barriers;
- remote object-store durability or empty-cache recovery;
- correct PG18 asynchronous I/O without file descriptors;
- WAL-before-page ordering at a remote durability boundary;
- recovery of WAL, `pg_control`, SLRUs, prepared state, or replication slots;
- a production tablespace-to-storage-manager selection contract.

The next admitted milestone replaces at least one callback family with an
objectKV-backed test implementation and runs the poison controls in
`evals/suites/postgres-page-bridge.toml`.
