# Placement storage generation 4 to 5

This is an offline, full-stop hard switch from the unscoped generation-4 topology to membership plus
explicit placement domains in generation 5. Generation 4 and 5 processes, references, control
frames, and storage cannot share a live cluster. Service startup never runs this migration.

## Preconditions

1. Stop every Lattice process using the prefix. Revoke old credentials and wait for the unscoped
   leader, member, and claim leases to disappear. The tool rejects a live leader, any leased member
   or claim, and every active handoff.
2. Take the platform's complete etcd disaster-recovery backup. Also choose a protected JSON export
   path for `apply`; it is created exclusively and never overwritten.
3. Inventory every generation-4 `EntityType` and `SingletonKind`. Create an explicit mapping for all
   of them. There is no implicit or default domain:

   ```json
   {
     "entity_types": {
       "player": "player",
       "account-login": "player",
       "world": "world"
     },
     "singleton_kinds": {
       "battle-scheduler": "battle"
     }
   }
   ```

4. Choose permanent generation-5 cardinality limits. A later CoordinatorHost configured with
   different limits rejects the prefix.
5. Run `inspect`, then `dry-run`. Both execute full decoding, mapping, collision, full-stop, capacity,
   and target-inventory validation without changing etcd.

The examples abbreviate the required limits as `$LIMITS`:

```text
--page-size N --max-slots N --max-plans N --max-members N \
--max-admin-operations N --max-entity-configs N --max-singleton-configs N
```

```sh
lattice-placement-migrate inspect --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --mapping /protected/domain-mapping.json $LIMITS
lattice-placement-migrate dry-run --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --mapping /protected/domain-mapping.json $LIMITS
```

An unmapped type, malformed record, generation-5 destination collision, live lease, active handoff,
or insufficient bound is a hard error. Fix the stopped-cluster input; never add a fallback mapping.

## Apply and resume

`apply` writes the export before its first key change. It atomically changes
`schema_generation=4` to `migrating-to-5`, writes a durable marker containing the exact mapping,
term, limits, backup path, and cursor, then acquires a five-minute lease-backed lock.

```sh
lattice-placement-migrate apply --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --mapping /protected/domain-mapping.json \
  --backup /protected/placement-generation-4.json $LIMITS
```

Each record transaction compares its source revision, destination absence/value, marker, schema,
unchanged coordinator term, absent leader, and exact migration lock. Converted entity/singleton
configuration is persisted under its mapped domain. Members move to the membership scope. Slots,
plans, and admin history move only to their mapped domain.

Configuration targets, per-domain finalization metadata, and cardinality repairs are written in
bounded batches below etcd's transaction-operation ceiling. A crash between batches leaves the
schema at `migrating-to-5`; `resume` verifies already-written values and continues idempotently. The
final schema flip itself remains one compare-guarded transaction.

Old ownership is never revived. Migrated running/allocating authority is written `Fenced`, targets
and active barriers are cleared, and active plans/moves become failed. Assignment generations and
state revisions remain monotonic; the generation-5 leader must establish fresh claims and install a
fresh same-term snapshot before serving.

After interruption, keep the cluster stopped. Wait for or revoke the old migration lock, inspect the
prefix, then resume with the exact same mapping and limits:

```sh
lattice-placement-migrate inspect --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --mapping /protected/domain-mapping.json $LIMITS
lattice-placement-migrate resume --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --mapping /protected/domain-mapping.json $LIMITS
```

Resume compares the marker revision and canonical marker value. Already committed records are not
rewritten. A record/progress compare failure leaves the cursor unchanged. A finalization compare
failure leaves `migrating-to-5`, releases the lock, and is resumable after the conflicting stopped
state is investigated.

## Verify

Successful finalization atomically installs schema generation 5, scoped membership/domain terms and
revisions, durable limits, per-scope counters, entity/singleton configurations, and per-domain
automatic-balance settings. It deletes generation-4 coordinator/counter/settings keys.

```sh
lattice-placement-migrate inspect-counters --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --mapping /protected/domain-mapping.json $LIMITS
```

If inventory and stored counters differ, investigate first. Counter repair requires the stopped
generation-5 prefix and performs exact compare-and-swap updates; startup never repairs silently.

```sh
lattice-placement-migrate repair-counters --endpoints "$ENDPOINTS" --prefix "$PREFIX" \
  --mapping /protected/domain-mapping.json $LIMITS
```

Verify that every expected domain has configuration/counters, every migrated authority is fenced,
the report's inventory matches etcd, and no unscoped generation-4 placement key remains. Then start
generation-5 membership/CoordinatorHost processes, wait for required domain health to become Ready,
start logic nodes, and reopen admission.

## Rollback boundary

Before `apply`, rollback is the platform backup or continued generation-4 operation. Once the schema
is `migrating-to-5`, never edit keys or set the schema back manually. Either resume to generation 5,
or stop all access and restore the complete pre-migration etcd backup. The JSON export is audit and
record-recovery evidence; it cannot recreate lease state and is not a platform backup.
