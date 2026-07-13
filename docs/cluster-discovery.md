# Cluster Discovery Providers

Cluster discovery publishes bootstrap candidates only. A discovered address is not a member, is not
eligible for business routing, and does not become authoritative until the Coordinator admits the
exact probed `NodeIncarnation`. Provider updates never add or remove members.

Applications construct providers through their defining module paths and may combine them with
`lattice_discovery::aggregate::AggregateDiscovery`. Aggregation deduplicates by canonical
`NodeAddress`, retains every source origin, selects the lowest numeric priority, rejects conflicting
expected node IDs, and rotates candidates within one priority.

## Configuration

The canonical service configuration is:

```yaml
cluster:
  discovery:
    - type: static
      name: primary-seeds
      priority: 10
      endpoints:
        - host: node-a
          port: 7447
          node_id: node-a
        - host: node-b
          port: 7447
          node_id: node-b
    - type: config_store
      priority: 20
      key: /lattice/clusters/prod/discovery/endpoints
    - type: dns
      priority: 30
      service: _lattice._tcp.cluster.example.com
      min_refresh: 5s
      max_refresh: 5m
    - type: kubernetes
      priority: 40
      namespace: lattice
      service: lattice-nodes
      port_name: remoting
      label_selector: app=lattice
      credentials: in_cluster
```

`StaticDiscovery` validates endpoints at construction and emits one immutable generation.

`ConfigStoreDiscovery` reads before watching and reconciles the watch's current value to close the
get/watch race. The configured key contains one complete JSON document:

```json
{
  "schema_version": 1,
  "generation": 42,
  "endpoints": [
    { "host": "node-a", "port": 7447, "node_id": "node-a", "priority": 10 }
  ]
}
```

Document generations are nonzero and strictly increasing. Malformed, rolled-back, duplicate, or
temporarily empty updates report an error and retain the last valid targets. An absent initial key
produces an empty initial snapshot.

`DnsDiscovery` supports either an SRV service or a hostname plus fixed port. It resolves A and AAAA
records, refreshes at the returned TTL clamped to configured bounds, and retains the previous valid
targets across NXDOMAIN and transient resolver failures. DNS source metadata retains the original
TLS server name; an IP address returned by resolution must not replace certificate hostname
validation.

`KubernetesEndpointSliceDiscovery` watches `discovery.k8s.io/v1` through the Kubernetes API. It
selects the named service and port, accepts IPv4, IPv6, and FQDN address types, and ignores endpoints
with `ready: false` or `terminating: true`. Expired resource versions trigger the watcher's full
relist and atomic replacement. Production uses `KubernetesCredentials::InCluster`; kubeconfig use
must be selected explicitly for tests or local development.

## Kubernetes RBAC

Grant only namespace-scoped read access to EndpointSlices:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: lattice-node
  namespace: lattice
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: lattice-endpoint-slice-reader
  namespace: lattice
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: lattice-endpoint-slice-reader
  namespace: lattice
subjects:
  - kind: ServiceAccount
    name: lattice-node
    namespace: lattice
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: lattice-endpoint-slice-reader
```

The workload sets `serviceAccountName: lattice-node`. It does not receive permission to write
EndpointSlices or read Coordinator membership and placement keys.

## Deployment and hard-switch upgrade

The bootstrap feature bit, Coordinator generation-3 member schema, and revisioned lifecycle control
messages are one full-stop boundary. Drain and stop every old node, revoke old placement credentials,
perform the documented schema-generation preflight/cleanup, and then start only the new release.
Mixed handshake versions, dual member formats, fallback routing, and rolling old/new membership are
unsupported.

Kubernetes workloads use a namespace-scoped ServiceAccount with only `list` and `watch` on
`discovery.k8s.io/v1` EndpointSlices. Configure a named remoting Service port, a readiness probe that
opens only in `Ready`, and a `preStop` hook that starts `leave`. Set
`terminationGracePeriodSeconds` above `cluster.join.shutdown_timeout`; a Pod that exhausts that
budget force-fences locally and is removed later through its exact member lease.

Use immutable run/commit image tags for acceptance and label every workspace-built test image
`org.realm-labs.lattice.test=true`. `scripts/docker-image-lifecycle.sh` enforces the shared 72-hour CI
or seven-day developer retention policy and the 80/90-percent disk watermarks without touching
unlabeled images.

Operational diagnosis and force-removal procedure are in
[the cluster lifecycle runbook](operations/cluster-lifecycle-runbook.md). The accompanying Grafana
dashboard is stored at `docs/operations/dashboards/cluster-lifecycle.json`.

