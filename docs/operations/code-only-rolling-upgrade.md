# Application deployment upgrades

The former code-only rolling-upgrade mechanism has been removed. Lattice no longer
accepts an application release manifest, tracks an N/N+1 release pair, rejects a
third application release, or prefers newer application releases during placement.
Shard and singleton targets use ordinary host eligibility and allocation rules.

No replacement `AppVersion` mechanism is implemented. Rolling updates, old-version
shard migration, rollback, and application compatibility need a separate design.
Do not interpret successful membership admission as proof that two application
binaries can safely coexist.

## Full-stop boundary

Until that design is implemented, deploy one application version at a time:

1. Close external admission and request [cluster shutdown](cluster-shutdown.md).
   Await a durable `Closed { completion: Graceful }` outcome; keep required
   Coordinators and discovery entries available until draining and cleanup finish.
   A timeout or blocked participant is not successful shutdown.
2. Stop all old application and Coordinator processes and confirm their leases and
   ownership are no longer live.
3. For an unchanged framework identity, explicitly start the next run after confirmed
   cleanup. For a framework identity change, prepare a fresh coordination namespace;
   use the old version's maintenance tool for any old-run abnormal reset. Do not
   rewrite the framework marker or load the old runtime schema with the new binary.
   Reprovision definitions and candidate authorization deliberately. Preserve
   externally meaningful ID history and handle durable business data separately.
4. Start the new Coordinators and application nodes, verify readiness, then reopen
   admission.

Exact framework admission, business-message protocol fingerprints, and allocation
configuration checks remain independent safeguards. They do not validate handler
semantics, durable business-state compatibility, or the application service ABI.

Automatic exact Lattice identity checks now guard bootstrap, handshakes, and
Coordinator namespace initialization. Published packages and development builds use
the exact Cargo package version without a source digest. Same-version source changes
are not detected: developers must deploy consistent builds and bump the version for
incompatible protocol or storage changes. A restart alone does not change the identity.
Cluster shutdown and [scoped abnormal reset](cluster-reset.md) have distinct
completion contracts. Reset requires proof by the operator that all old processes
are stopped; it cannot turn missing stop evidence into graceful completion.
Rollback follows the same full-stop and exact-framework-identity rules, not a
mixed-version rollout.
