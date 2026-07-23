use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use super::{
    Profile,
    testctl_outcomes::{ScenarioOutcome, ScenarioStatus},
};

#[derive(Serialize)]
pub(super) struct Manifest {
    pub profile: Profile,
    pub seed: u64,
    pub source_commit: String,
    pub source_status: String,
    pub source_fingerprint: String,
    pub started_unix_millis: u128,
    pub elapsed_millis: u128,
    pub success: bool,
    pub replay: String,
    pub platform: String,
    pub pinned_images: String,
    pub scenarios: Vec<ScenarioOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infrastructure_error: Option<String>,
    pub configuration: serde_json::Value,
}

#[derive(Serialize)]
pub(super) struct ResourceSample {
    pub elapsed_millis: u128,
    pub open_file_descriptors: Option<usize>,
    pub resident_memory_kib: Option<u64>,
    pub threads: Option<u64>,
    pub process_status: Option<String>,
}

#[derive(Serialize)]
pub(super) struct MonitorCommand {
    pub sequence: u64,
    pub stop: bool,
}

#[derive(Deserialize)]
pub(super) struct MonitorResult {
    pub success: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct ScopedLeadershipArtifact {
    pub node_id: String,
    pub term: u64,
    #[serde(with = "lattice_sim::serde_u128")]
    pub incarnation: u128,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct MultiDomainHostArtifact {
    pub node_id: String,
    #[serde(with = "lattice_sim::serde_u128")]
    pub incarnation: u128,
    pub scopes: BTreeMap<String, ScopedLeadershipArtifact>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct MultiDomainLogicArtifact {
    pub node_id: String,
    #[serde(with = "lattice_sim::serde_u128")]
    pub incarnation: u128,
    pub lifecycle: String,
    pub domains: BTreeMap<String, String>,
    #[serde(default)]
    pub membership_version: Option<MembershipVersionArtifact>,
    #[serde(default)]
    pub members: Vec<MemberArtifact>,
    #[serde(default)]
    pub associations: usize,
    #[serde(default)]
    pub attached_lanes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub(super) struct MembershipVersionArtifact {
    pub term: u64,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub(super) struct MemberArtifact {
    pub node_id: String,
    #[serde(with = "lattice_sim::serde_u128")]
    pub incarnation: u128,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct ScaleNodeArtifact {
    pub node_id: String,
    #[serde(with = "lattice_sim::serde_u128")]
    pub incarnation: u128,
    pub lifecycle: String,
    pub domains: BTreeMap<String, String>,
    pub membership_version: Option<MembershipVersionArtifact>,
    pub members: Vec<MemberArtifact>,
    pub join_millis: Option<u128>,
    pub ring: Option<RingArtifact>,
    pub resources: ProcessResourceArtifact,
    pub associations: usize,
    pub attached_lanes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub(super) struct RingArtifact {
    pub peer_node_id: String,
    pub request: u64,
    pub reply: u64,
    pub elapsed_millis: u128,
    pub data_lanes_slept: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub(super) struct ProcessResourceArtifact {
    pub resident_memory_kib: Option<u64>,
    pub threads: Option<u64>,
    pub open_file_descriptors: Option<usize>,
}

pub(super) fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let encoded = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    std::fs::write(path, encoded).map_err(|error| error.to_string())
}

pub(super) fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let encoded = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temporary, encoded).map_err(|error| error.to_string())?;
    std::fs::rename(temporary, path).map_err(|error| error.to_string())
}

pub(super) fn write_junit(
    path: &Path,
    profile: Profile,
    outcomes: &[ScenarioOutcome],
    infrastructure_error: Option<&str>,
) -> Result<(), String> {
    let failures = outcomes
        .iter()
        .filter(|outcome| outcome.status == ScenarioStatus::Failed)
        .count()
        + usize::from(infrastructure_error.is_some());
    let skipped = outcomes
        .iter()
        .filter(|outcome| outcome.status == ScenarioStatus::NotRun)
        .count();
    let tests = outcomes.len() + usize::from(infrastructure_error.is_some());
    let mut cases = String::new();
    for outcome in outcomes {
        let body = match outcome.status {
            ScenarioStatus::Passed => String::new(),
            ScenarioStatus::Failed => format!(
                "<failure message=\"{}\"/>",
                escape_xml(outcome.error.as_deref().unwrap_or("scenario failed"))
            ),
            ScenarioStatus::NotRun => "<skipped message=\"not run\"/>".to_owned(),
        };
        cases.push_str(&format!(
            "<testcase name=\"{}\" time=\"{:.3}\">{body}</testcase>",
            escape_xml(&outcome.name),
            outcome.elapsed_millis as f64 / 1000.0,
        ));
    }
    if let Some(error) = infrastructure_error {
        cases.push_str(&format!(
            "<testcase name=\"infrastructure-cleanup\"><failure message=\"{}\"/></testcase>",
            escape_xml(error)
        ));
    }
    let xml = format!(
        "<testsuite name=\"lattice-{profile:?}\" tests=\"{tests}\" failures=\"{failures}\" skipped=\"{skipped}\">{cases}</testsuite>\n"
    );
    std::fs::write(path, xml).map_err(|error| error.to_string())
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
