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

1. Close external admission and drain the running deployment using its existing
   lifecycle APIs. Keep required Coordinators available until draining completes.
2. Stop all old application and Coordinator processes and confirm their leases and
   ownership are no longer live.
3. Perform any required explicit offline state/schema migration.
4. Start the new Coordinators and application nodes, verify readiness, then reopen
   admission.

Transport, Coordinator control, storage-schema, message-protocol, and placement
configuration checks remain independent safeguards. They do not validate handler
semantics, durable business-state compatibility, or the application service ABI.

The strict automatic Lattice identity, cluster-wide shutdown, and scoped runtime
reset described in the [control-plane memo](../cluster-control-plane-memo.md) are
planned work, not capabilities added by removal of the release mechanism.
