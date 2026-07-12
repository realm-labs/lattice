#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod matrix;
pub mod metrics;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};
use lattice_remoting::association::Association;
use lattice_remoting::association::AssociationKey;
use lattice_remoting::association::LaneAttachment;
use lattice_remoting::association::LaneKind;
use lattice_remoting::config::RemotingConfig;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::messaging::target::SenderIdentity;
use lattice_remoting::protocol::{ProtocolDescriptor, ProtocolFingerprint};
use metrics::WorkloadReport;

pub const BENCH_PROTOCOL_ID: u64 = 0x6265_6e63_6800_0001;

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub requests: usize,
    pub payload_bytes: usize,
    pub bulk_stripes: usize,
}

impl BenchmarkConfig {
    pub fn from_env() -> Self {
        Self {
            requests: env_usize("LATTICE_BENCH_REQUESTS", 10_000),
            payload_bytes: env_usize("LATTICE_BENCH_PAYLOAD_BYTES", 128),
            bulk_stripes: env_usize("LATTICE_BENCH_BULK_STRIPES", 1).clamp(1, 4),
        }
    }

    pub fn test_default() -> Self {
        Self {
            requests: 256,
            payload_bytes: 128,
            bulk_stripes: 4,
        }
    }
}

pub struct RemotingTopology {
    association: Arc<Association>,
    messaging: OutboundMessaging,
    target: ActorRef<()>,
    fingerprint: ProtocolFingerprint,
    drains: Vec<tokio::task::JoinHandle<()>>,
}

impl RemotingTopology {
    pub fn start(config: &BenchmarkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("remoting-benchmark")?;
        let local_incarnation = NodeIncarnation::generate();
        let remote_incarnation = NodeIncarnation::generate();
        let remote_address = NodeAddress::new("127.0.0.1", 25541)?;
        let remoting = RemotingConfig {
            bulk_stripes: config.bulk_stripes,
            ..RemotingConfig::default()
        };
        let association = Arc::new(Association::new(
            AssociationKey {
                cluster_id: cluster_id.clone(),
                local_incarnation,
                remote_address: remote_address.clone(),
                remote_incarnation,
            },
            remoting,
        )?);
        let id = association.id();
        let key = association.key().clone();
        for (lane, nonce) in [(LaneKind::Control, 1), (LaneKind::Interactive, 2)] {
            association.attach(LaneAttachment {
                association_id: id,
                key: key.clone(),
                lane,
                connection_nonce: nonce,
            })?;
        }
        for index in 0..config.bulk_stripes {
            association.attach(LaneAttachment {
                association_id: id,
                key: key.clone(),
                lane: LaneKind::Bulk(index as u8),
                connection_nonce: 3 + index as u128,
            })?;
        }
        let protocol_id = ProtocolId::new(BENCH_PROTOCOL_ID)?;
        let fingerprint = ProtocolFingerprint::digest(b"remoting-benchmark/v1:bulk-tell");
        association.install_peer_catalogue([ProtocolDescriptor {
            protocol_id,
            fingerprint,
        }])?;
        let target = ActorRef::new(
            cluster_id,
            remote_address,
            remote_incarnation,
            ActorPath::user(["user", "bench", "u-1"])?,
            ActivationId::new(remote_incarnation, 1)?,
            protocol_id,
        )?;
        let receivers = association
            .take_receivers()
            .ok_or_else(|| std::io::Error::other("association receivers already taken"))?;
        let drains = receivers
            .bulk
            .into_iter()
            .map(|mut receiver| {
                let association = association.clone();
                tokio::spawn(async move {
                    while let Some(frame) = receiver.recv().await {
                        association.release_queued_bytes(frame.payload.len());
                    }
                })
            })
            .collect();
        Ok(Self {
            association,
            messaging: OutboundMessaging::new(1)?,
            target,
            fingerprint,
            drains,
        })
    }

    pub async fn run_bulk_tell(
        &self,
        requests: usize,
        payload_bytes: usize,
    ) -> Result<WorkloadReport, Box<dyn std::error::Error>> {
        let started = Instant::now();
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let sender = SenderIdentity::Process(1);
        let mut successes = 0;
        for _ in 0..requests {
            loop {
                match self.messaging.tell(
                    &self.association,
                    &sender,
                    &self.target,
                    self.fingerprint,
                    1,
                    payload.clone(),
                ) {
                    Ok(_) => {
                        successes += 1;
                        break;
                    }
                    Err(lattice_remoting::messaging::error::TellError::Association(
                        lattice_remoting::association::AssociationError::QueueFull,
                    )) => tokio::task::yield_now().await,
                    Err(error) => return Err(Box::new(error)),
                }
            }
        }
        Ok(WorkloadReport {
            name: "association_bulk_tell_admission",
            requests,
            successes,
            errors: requests - successes,
            elapsed: started.elapsed(),
            latencies: vec![Duration::ZERO; successes],
            observed_actor_ids: [1].into_iter().collect(),
        })
    }

    pub async fn shutdown(self) {
        self.association.begin_close();
        self.association.finish_close();
        for task in self.drains {
            task.abort();
            let _ = task.await;
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
