# Cluster Control Plane and etcd Scope: Decision Memo

> Status: exploratory; no implementation decision or migration is approved.
> Scope: etcd state, Coordinator candidacy, discovery, and ordinary-node dependencies.
> This memo records questions for a future redesign. The current protocol and
> [placement architecture](architecture/03-placement.md) remain authoritative.

## Motivation

The current design persists substantial membership and placement runtime state in
etcd, including member records, domain members, assignments, claims, handoffs,
plans, and administrative operations. Some of those records outlive the processes
that produced them. A full cluster shutdown followed by restart can therefore
expose old state that must be recognized and reconciled before the cluster can
operate safely. We want to revisit which records actually need durable,
linearizable arbitration, rather than treating etcd as the default home for every
piece of cluster state.

There is a separate topology question: should every ordinary node connect to
etcd, or should only a smaller, explicitly configured set of Coordinator
candidates do so? These decisions interact but are not the same decision.

## Current behavior to preserve or reconsider

- Membership leadership and each placement-domain leadership are independent
  scopes. A `CoordinatorHost` may campaign for multiple scopes.
- Discovery supplies Coordinator **candidates**, not authoritative members or
  business-routing targets. Bootstrap probes identify a current leader; an
  Association to that leader carries membership/domain admission and control
  traffic. Ordinary peer Associations need not be opened at startup.
- A node becomes globally `Up` only after the membership session's
  `MemberHello -> Joining -> JoinReady -> Up` sequence. Hosting a placement
  domain additionally requires its domain session.
- The current design gives Coordinator hosts the election/storage capability;
  ordinary nodes use discovery and control sessions. A ConfigStore-backed
  discovery provider may itself use etcd, so "no direct etcd dependency" is
  true only when the chosen discovery path does not open a per-node etcd client.
- Serving authority is separate from finding a leader or being a member. Claims
  and local fencing must continue to prevent two owners from safely writing at
  once, including during partitions and leader changes.

See [discovery providers](cluster-discovery.md) and the
[control-plane boundary](architecture/03-placement.md#1-control-plane-boundary)
for the currently implemented contracts.

## Direction under consideration: minimal etcd authority

Use etcd for the smallest set of facts that must survive or arbitrate a leader
change safely: for example, scope leadership/term, schema or cluster epoch, and
ownership claims or fencing generations where external authority requires them.
This is a design criterion, **not yet an approved key list**. Review each existing
key family against these questions:

1. What safety invariant requires this record to be in etcd? Is it needed after
   all cluster processes have stopped, or only while a process is alive?
2. Could it instead be a leader-maintained, versioned runtime view distributed
   through control sessions? If so, how does a new leader reconstruct that view?
3. If it remains in etcd, what lease, incarnation, epoch, expiry, and cleanup
   rules prevent a full restart from interpreting old data as live authority?
4. Can recovery prove previous ownership has ended before granting replacement
   authority? Simplifying persistence must not weaken fencing or failover safety.

In particular, decide explicitly whether membership and domain participation
are reconstructed by rejoining after leader election, represented by minimal
lease-backed records, or use another bounded recovery mechanism. Removing
durable runtime records without specifying leader recovery would merely move the
problem. Conversely, attaching a lease to a record does not automatically make
every durable assignment or plan safe to reuse after a full-cluster restart.

## Direction under consideration: restricted Coordinator candidates

Configure a small, failure-domain-diverse candidate set for each required
leadership scope. Only candidates run the election component and hold the etcd
permissions needed for that scope. An ordinary node would:

1. obtain candidate addresses through static seeds, DNS, Kubernetes discovery,
   or a discovery service that does not give every node its own etcd connection;
2. probe/reconcile the current leader for membership and each needed domain;
3. establish control Association(s), complete admission, and install the
   versioned runtime views before becoming ready;
4. open Associations to other business peers only when traffic requires them;
5. on leader loss, stop actions that require fresh authority, rediscover, and
   resynchronize before resuming them.

An ordinary node would not automatically become a leader: it would need an
explicit role/permission change and the Coordinator host capability. Restricting
candidacy does **not** mean only candidates may host actors. Candidate eligibility
also need not be identical for the membership scope and every placement domain.

The alternative remains viable: ordinary nodes could have narrow, read-only
etcd access to fetch/watch leader records directly, while only candidates can
campaign or mutate authority. That simplifies leader discovery but increases
client/watch count, credential distribution, and direct failure coupling to
etcd. Direct leader lookup still does not replace admission or the control
session. Neither approach requires eager Associations to every member.

## Expected load and trade-offs

Restricting etcd access to candidates can reduce connection, watch,
authentication, and reconnect load from ordinary nodes. It does **not** by
itself reduce writes performed by leaders for the current persisted membership,
placement, or claim model. The state-scope redesign is needed to address those
writes and the full-restart stale-state problem.

Thousands of etcd clients are not inherently disqualifying. Capacity depends on
connections and watch streams, update fan-out, write/lease rate, snapshot size,
etcd hardware, and synchronized startup or reconnection. A decision should use
the expected cluster/domain sizes and a measured failure-recovery workload, not
client count alone. The restricted-candidate design also concentrates join and
snapshot work on Coordinators, so their admission capacity and failover behavior
must be measured.

## Open decisions and validation

- Exact minimum persisted key set and the lifecycle of each retained record.
- Whether ordinary nodes need any direct etcd access, and whether the selected
  discovery provider would secretly reintroduce a per-node etcd dependency.
- Candidate selection per scope, redundancy across failure domains, and how a
  node is promoted or removed as a candidate without unsafe overlap.
- How a newly elected leader rebuilds membership, domain participation, and
  routing views if those records are no longer durable.
- What remains safe during an etcd quorum loss, a Coordinator outage, or a
  network partition; which operations must fail closed and when local grants
  expire.
- Behavior after a full-cluster restart with old etcd data, including cluster
  epoch/incarnation handling and bounded cleanup or migration.
- Load tests for a representative large cluster: steady-state watches and
  writes, simultaneous node startup, leader failover, and etcd reconnection.

No protocol, schema, or deployment change should be inferred from this memo.
