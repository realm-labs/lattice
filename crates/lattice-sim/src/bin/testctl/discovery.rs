use std::path::Path;
use std::time::{Duration, Instant};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct DiscoveryLifecycleArtifact {
    node_id: String,
    incarnation: u128,
    provider: String,
    lifecycle: String,
    authoritative_up_members: Vec<(String, u128)>,
}

pub fn verify_case(artifacts: &Path, name: &str, provider: &str) -> Result<(), String> {
    let path = artifacts.join(name);
    let ready = read(&path)?;
    if ready.provider != provider || ready.lifecycle != "Ready" || ready.incarnation == 0 {
        return Err(format!(
            "{provider} discovery member did not reach exact Ready state"
        ));
    }
    if !contains_self(&ready) {
        return Err(format!(
            "{provider} discovery member lacks authoritative Coordinator Up admission"
        ));
    }
    std::fs::write(path.with_extension("leave"), b"leave").map_err(|error| error.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stopped = read(&path)?;
        if stopped.lifecycle == "Terminated" {
            if contains_self(&stopped) {
                return Err(format!(
                    "{provider} member retained authoritative Up state after leave"
                ));
            }
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!("{provider} member did not complete graceful leave"));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

fn read(path: &Path) -> Result<DiscoveryLifecycleArtifact, String> {
    let encoded = std::fs::read(path).map_err(|error| error.to_string())?;
    serde_json::from_slice(&encoded).map_err(|error| error.to_string())
}

fn contains_self(artifact: &DiscoveryLifecycleArtifact) -> bool {
    artifact
        .authoritative_up_members
        .iter()
        .any(|(node_id, incarnation)| {
            node_id == &artifact.node_id && *incarnation == artifact.incarnation
        })
}
