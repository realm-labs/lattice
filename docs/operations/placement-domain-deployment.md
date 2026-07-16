# Placement-domain deployment

Generation 5 separates global membership from placement authority. Deploy at least two candidates
for the membership scope and for every placement domain required by an application. One
CoordinatorHost process may campaign for several scopes; dedicated membership-only hosts are
supported by configuring zero placement domains.

Applications select one explicit assembly mode. `EmbeddedCandidate` supervises a Coordinator
candidate and Logic Service in the same operating-system process while retaining separate remoting
identities. `ClientOnly` contains no store credentials and discovers external candidates.
`DedicatedCandidate` contains no Logic Service and is the preferred strict control-plane shape.
Candidate failover still requires a shared durable store and at least two running candidates; the
embedded mode removes application bootstrap plumbing, not the distributed election requirement.
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

- Give CoordinatorHosts generation-5 etcd credentials scoped to the cluster prefix. Logic and
  gateway processes receive no general placement-store credentials.
- Configure membership discovery separately from every `CoordinatorScope::Placement(domain)`.
- Declare every entity and singleton with an explicit `PlacementDomainId`. Configure a positive
  capacity quota on each node/domain pair that may host authority.
- Bound domains per host, snapshot/session/control queues per domain, total service buffering, and
  host-wide movement concurrency.
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

## Full-stop generation-5 rollout

Mixed generation 4/5 rollout is unsupported:

1. close application admission and stop all generation-4 processes;
2. wait for every old leader/member/claim lease and active handoff to disappear;
3. run the explicit generation-4-to-5 migration and verify scoped inventory;
4. deploy membership and placement-domain CoordinatorHosts;
5. wait for one leader per required scope;
6. deploy logic/gateway processes and wait for selected domain readiness;
7. reopen admission.

Use the cluster/domain dashboard and partial-degradation runbook during rollout. Never work around a
failed domain by assigning its types to a default domain or by routing to another domain.
