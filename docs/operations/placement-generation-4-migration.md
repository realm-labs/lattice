# Retired placement schema migration

The generation-4-to-5 migration API and CLI have been removed. This page remains
only to explain old links; its former inspect/dry-run/apply/resume procedure is no
longer supported. The CLI's attached offline counter inspection/repair commands
were removed with it; normal runtime cardinality admission and reconciliation remain.

Lattice now uses an exact Cargo package version at connection and storage admission.
It does not convert old runtime schemas or overwrite a namespace's framework marker.
See the [full-stop deployment procedure](code-only-rolling-upgrade.md).

Before upgrading, stop all old processes and establish that old ownership is no
longer live. Use a fresh isolated coordination namespace; preserve external fencing
history, worker-ID history, persistent configuration, and business data separately.
Do not delete a broad etcd prefix or treat a manually edited framework marker as an upgrade.

The planned run-scoped inspect/reset and guarded cleanup tooling belongs to work
package 3 in the [control-plane memo](../cluster-control-plane-memo.md). It is not
implemented by removing the old migration tool.
