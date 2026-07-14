# Placement storage generation 3 to 4

This is an offline, full-stop migration. Generation 3 and generation 4 services cannot share a
placement prefix, and service startup never runs this migration automatically.

## Preconditions and decision points

1. Stop every lattice process using the target cluster prefix. Confirm the Coordinator leader key
   is absent and allow member/claim leases to expire. If the leader key is present, stop: the tool
   refuses to inspect or modify the prefix.
2. Preserve the existing etcd backup required by the platform's disaster-recovery procedure. Pick a
   second, local export path for the migration command. `apply` creates this file exclusively and
   refuses to overwrite it; on Unix it is created with mode `0600`.
3. Choose the permanent generation-4 cardinality limits. Every future Coordinator using this prefix
   must use exactly the same limits. A mismatch rejects startup instead of changing capacity.
4. Run `inspect`, then `dry-run`. Stop and investigate any malformed, conflicting-configuration, or
   over-capacity result. Dry-run performs the complete conversion and transitional-state validation
   in memory but writes no etcd key and no export.

The examples below abbreviate the required limit flags as `$LIMITS`. The full set is:

```text
--page-size N --max-slots N --max-plans N --max-members N \
--max-admin-operations N --max-entity-configs N --max-singleton-configs N
```

Do not put endpoint credentials on a shared command line or in a ticket. Prefer endpoints whose TLS
credentials are supplied by the process environment or protected configuration.

```sh
lattice-placement-migrate inspect --endpoints "$ENDPOINTS" --prefix "$PREFIX" $LIMITS
lattice-placement-migrate dry-run --endpoints "$ENDPOINTS" --prefix "$PREFIX" $LIMITS
```

## Apply and resume

`apply` writes the user-selected export before its first durable-key change. It then atomically
changes `schema_generation=3` to `migrating-to-4`, writes a durable cursor, and acquires a
lease-backed migration lock. Old and new services both reject the intermediate schema.

```sh
lattice-placement-migrate apply --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --backup /protected/path/placement-generation-3.json $LIMITS
```

If the process is interrupted, do not restart lattice. Retain the export and wait for the migration
lock's five-minute lease to expire. Inspect the prefix and continue with the same limits:

```sh
lattice-placement-migrate inspect --endpoints "$ENDPOINTS" --prefix "$PREFIX" $LIMITS
lattice-placement-migrate resume --endpoints "$ENDPOINTS" --prefix "$PREFIX" $LIMITS
```

Resume compares the durable cursor, exact record revisions, the unchanged Coordinator term, the
absent leader key, and its current migration lock on every write. Converted pages are not rewritten.
Impossible transitional relationships are made visibly terminal: affected slots become `Fenced`
and affected plan movements become `Failed`. Running or Allocating records without their exact
owner/generation/term claim are fenced.

## Verify and counter repair

Successful apply/resume atomically installs schema generation 4, the selected limit metadata,
slot/plan/member/admin counters, the maximum member/slot state revision, and default automatic
balance settings. Verify the report, then inspect counters while the cluster remains stopped:

```sh
lattice-placement-migrate inspect-counters --endpoints "$ENDPOINTS" --prefix "$PREFIX" $LIMITS
```

If actual bounded inventory and stored counters differ, investigate the cause first. The explicit
repair command requires no leader, compares the schema, limit metadata, and exact counter revisions,
then repairs all counters atomically. Normal service startup never performs this repair.

```sh
lattice-placement-migrate repair-counters --endpoints "$ENDPOINTS" --prefix "$PREFIX" $LIMITS
```

Start generation-4 Coordinators only after verification. The first leader performs structural
reconciliation before accepting mutation traffic, and every node must install a fresh generation-4
snapshot for that leader's term.

## Rollback decision

Before `apply`, rollback means restoring the platform etcd backup or continuing to run generation 3.
After the schema becomes `migrating-to-4`, do not edit records or set the schema back by hand. Either
resume to generation 4, or stop all access and restore the complete pre-migration etcd backup. The
JSON export is audit and record-recovery evidence; it does not recreate lease state and is not a
substitute for the platform backup.

Admin-operation idempotency is guaranteed only while its durable terminal record is retained. The
retention window is the shorter of the configured age and count bounds; clients must use a new
operation ID after that window.
