//! Node-level integration tests for `lattice-service`, grouped by the behaviour under test.

mod admission;
#[cfg(feature = "tls")]
mod candidate_administration;
mod cluster_shutdown;
mod membership;
mod node_lifecycle;
mod remote_routing;
mod remote_tell;
mod supervision;
mod support;
