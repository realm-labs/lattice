# Cluster discovery and member lifecycle runbook

Discovery records are reachability hints only. Never repair membership by editing DNS,
EndpointSlices, or the ConfigStore discovery document. The membership leader owns the exact member
revision and incarnation. Each placement-domain leader independently owns that domain's members,
configuration, slots, claims, plans, and revision.

## Signals and first checks

The dashboard at `docs/operations/dashboards/cluster-lifecycle.json` groups discovery, bootstrap,
node lifecycle, domain leader/session, route availability, capacity/load, authority inventory,
reconciliation, cross-domain drain, removal, and stale-incarnation signals. Queries use bounded
labels such as cluster, placement domain, provider, lifecycle state, and reason. Node IDs,
addresses, operation IDs, and certificate identities belong in traces and bounded inspection
output, not metric labels. Leader concentration is aggregated without a host label.

For every incident, capture the exact cluster ID, node ID/incarnation, membership term/revision,
affected placement domain and its term/revision, discovery generations, selected bootstrap
endpoints, and current membership/domain snapshots before taking a mutating action.

## Stuck Joining

1. Confirm the node is listening and remains `Joining`; external readiness must be closed.
2. Inspect each provider's last generation, target count, error, and stale age. An empty or stale
   candidate set explains reachability only; it does not imply missing membership.
3. Probe a candidate with the configured TLS hostname. Check cluster mismatch, expected-node
   mismatch, feature/version rejection, certificate URI identity, redirect loops, and RetryAfter.
4. Inspect the membership leader for a `Joining` record with the node's exact incarnation and snapshot
   revision. A different live incarnation under the same node ID must be drained or force-removed;
   never rewrite discovery to make the new process inherit it.
5. If the join is retryable, repair discovery/network/TLS and let the supervised backoff continue.
   Protocol/catalogue rejection is terminal and requires a compatible full-stop deployment.

## Membership loss

1. Verify new cluster/domain admission closed. Do not clear still-valid domain routes or claims.
2. Compare the last membership term/revision with the membership leader. Same-term conflicting
   leaders are a safety incident; stop changes and preserve artifacts.
3. Restore membership-scope discovery or its authenticated redirect path. Discovery is never a
   member directory and cannot extend placement claims.
4. Require a fresh atomic membership snapshot after a gap or rollover. Admission may reopen only
   after the exact local `Up` record is installed; a `Joining` snapshot is insufficient.
5. Check required domains separately. Membership recovery can restore node readiness while one
   domain remains visibly `Degraded`.

## Partial placement-domain degradation

1. Select the exact domain and confirm unrelated domains retain leader term, session readiness,
   route availability, and claim counts.
2. Compare the failed domain's last placement term/revision with its current leader. A higher-term
   delta cannot reopen it; a complete same-term domain snapshot is required first.
3. Restore only that domain's scoped discovery/control path. Do not restart membership or clear
   another entry in the domain router directory.
4. Known routes remain usable only while exact owner generation and local claim deadline remain
   valid. Unknown homes, new allocations, and expired authority fail as domain unavailable.
5. If leadership is concentrated, plan capacity from the aggregate concentration panel; do not
   force leadership movement during an active safety incident.

## Drain timeout

1. Inspect the aggregate drain operation, exact incarnation, and each joined domain's remaining
   authorities, handoff barriers, and actor stop failures.
2. Confirm domain/external admission is closed. Global membership remains `Up` until every required
   domain completion is acknowledged; only then may it pass through `Leaving` to removal.
3. Resume each domain's persisted handoffs independently after leader failover. A completed domain
   stays complete when another domain times out.
4. At deadline, force/fence every unfinished domain independently. Record unfinished domains and
   verify membership removal happens only after their authorities are resolved or fenced.

## Actor StopFailed and quarantine

1. Inspect `LatticeService::retained_actor_cells()` and record the exact `LocalActorRef`, logical
   Actor ID, lifecycle, persistence error, attempt count, and authoritative/quarantined status.
   Never operate on a logical Actor ID alone when more than one fenced activation is retained.
2. Repair the persistence dependency, then call `LatticeService::retry_actor_stop(local_ref)`.
   Successful persistence terminates that exact activation and automatically resumes a blocked
   placement handoff. Retry never restores authority to a quarantined activation.
3. If persistence cannot be recovered and data loss is explicitly approved, call
   `LatticeService::force_stop_actor(local_ref, reason, ticket)`. Use a unique incident/change
   ticket and preserve the forced-data-loss event.
4. Quarantine capacity exhaustion is a safety backpressure condition. The overflow activation is
   fenced and retained as a non-routable Registry blocker; resolve it before expecting another
   activation with the same logical ID on that process.

## Force removal

Use the authenticated membership command with a unique operation ID, node ID, and the observed
expected incarnation. Repeat the same operation ID only with identical arguments. An incarnation
mismatch is protective: refresh the member snapshot and never force-remove a newer replacement.
After success, verify the revisioned `Removed(ForceRemoved)` event, revoked member lease, fenced
claims, closed exact Association, and DeathWatch termination.

## Evidence and replay

Docker and kind runs write manifests, structured traces, snapshots, logs, JUnit, and resource samples
under `target/test-artifacts/<run-id>`. Preserve the first failing artifact. Replay deterministic
failures with the command in `manifest.json`. Every Docker/kind path must finish through
`scripts/docker-image-lifecycle.sh`; only images labeled
`org.realm-labs.lattice.test=true` are eligible for retention cleanup.
