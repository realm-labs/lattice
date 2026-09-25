# Placement-domain deployment

Cluster membership and group allocation have independent Coordinators. Deploy at least two candidates
for the membership scope and for every actor group required by an application. One
CoordinatorHost process may campaign for several scopes; dedicated membership-only hosts are
supported by configuring zero actor groups.

Applications select one explicit assembly mode. `EmbeddedCandidate` supervises a Coordinator
candidate and Logic Service in the same operating-system process while retaining separate remoting
identities. `ClientOnly` contains no store credentials and discovers external candidates.
`DedicatedCandidate` contains no Logic Service and is the preferred strict control-plane shape.
Candidate failover still requires a shared durable store and at least two running candidates; the
embedded mode removes application bootstrap plumbing, not the distributed election requirement.
Every enabled candidate must also be explicitly authorized per scope using
[privileged candidate provisioning](cluster-reset.md#candidate-provisioning-and-live-changes).
Local capability is not permission, and discovery endpoints do not self-enroll
candidates. Removing eligibility fences authoritative writes; it does not stop
unrelated business Actors hosted in the same process.

Every embedded instance must receive the same candidate endpoint set through
`EmbeddedCoordinatorConfig::candidates(...)`; its own endpoint is added automatically.

## Process topology

For three domains, a production starting shape is:

```text
CoordinatorHost A: membership candidate, player candidate, world standby
CoordinatorHost B: membership candidate, world candidate, battle standby
CoordinatorHost C: player standby, battle candidate
logic nodes: one membership session plus player/world/battle sessions as registered
gateway nodes: one membership session plus proxy-only sessions for used domains
```

Balance is operational, not a correctness precondition. Each scope has an exact lease-backed leader
and term. Kubernetes Services, DNS, EndpointSlices, static lists, and ConfigStore documents publish
scoped candidate reachability; they never publish placement truth.

## Required configuration

- Give CoordinatorHosts etcd credentials scoped to the cluster prefix. Logic and
  gateway processes receive no general placement-store credentials.
- Configure membership discovery separately from every `CoordinatorScope::Group(domain)`.
- Declare every entity and singleton with an explicit `ActorGroupId`. Configure a positive
  capacity quota on each node/domain pair that may host authority.
- Bound domains per host, snapshot/session/control queues per domain, total service buffering, and
  group/entity and source/target movement concurrency. Cross-group global quotas
  are not implemented.
- Gate each endpoint on its exact required domain set. Do not make all traffic depend on an
  unrelated optional domain.

## Kubernetes lifecycle

Startup readiness remains closed until the exact local membership record is `Up` and every required
domain has installed a full same-term snapshot. A `Joining` membership snapshot is not readiness.
Liveness does not fail for one degraded domain; domain health and route availability are separate
signals. Application readiness may select required domains.

`preStop` begins aggregate drain: close admission, drain/fence every joined domain, remove global
membership only after all domain completions, then stop remoting. Set `terminationGracePeriodSeconds`
longer than the configured drain deadline. A PodDisruptionBudget must preserve enough
CoordinatorHost candidates for each scope, not merely enough total Pods.

Expose the management-only `lattice-ops::health` router on a dedicated port and configure Kubernetes
to call `/startupz`, `/readyz`, and `/livez`. Keep that listener alive throughout aggregate drain:
`/readyz` returns `503` as soon as the node leaves `Ready`, while `/livez` continues returning `200`
until the managed service reaches `Terminated`. Stop the health server only after
`LatticeApplication::shutdown()` completes. The probe endpoints are intentionally unauthenticated,
so restrict the management port with a NetworkPolicy and do not expose it through the public
Service.

The former code-only rolling-upgrade mechanism is no longer supported. Deploy one
application version at a time using the [full-stop procedure](code-only-rolling-upgrade.md).
Placement has no application-release preference.

## Full-stop framework rollout

Mixed framework versions are unsupported:

1. close application admission and complete the [whole-cluster shutdown](cluster-shutdown.md)
   operation, then stop all old processes;
2. if graceful completion is blocked, explicitly stop every old process before the
   [abnormal reset](cluster-reset.md); absence or lease expiry is not stop proof;
3. prepare a fresh coordination namespace under the [full-stop procedure](code-only-rolling-upgrade.md); do not stamp a new version onto old records;
4. initialize the empty namespace with the new framework and intended limits,
   provision candidate eligibility, then deploy Cluster and Group CoordinatorHosts;
5. wait for one leader per required scope;
6. deploy logic/gateway processes and wait for selected domain readiness;
7. reopen admission.

Use the cluster/domain dashboard and partial-degradation runbook during rollout. Never work around a
failed domain by assigning its types to a default domain or by routing to another domain.
