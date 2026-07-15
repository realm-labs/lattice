use super::ResourceSample;
use std::time::Duration;

pub(super) fn sample(elapsed: Duration) -> ResourceSample {
    let process_status = std::fs::read_to_string("/proc/self/status").ok();
    ResourceSample {
        elapsed_millis: elapsed.as_millis(),
        open_file_descriptors: std::fs::read_dir("/proc/self/fd")
            .ok()
            .map(|entries| entries.count()),
        resident_memory_kib: status_value(&process_status, "VmRSS:"),
        threads: status_value(&process_status, "Threads:"),
        process_status,
    }
}

fn status_value(status: &Option<String>, key: &str) -> Option<u64> {
    status.as_deref()?.lines().find_map(|line| {
        line.strip_prefix(key)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

pub(super) fn assert_growth(
    samples: &[ResourceSample],
    final_sample: &ResourceSample,
) -> Result<(), String> {
    let initial = samples
        .first()
        .ok_or_else(|| "soak has no initial resource sample".to_owned())?;
    if let (Some(before), Some(after)) = (
        initial.open_file_descriptors,
        final_sample.open_file_descriptors,
    ) && after > before.saturating_add(32)
    {
        return Err(format!(
            "soak file descriptors grew from {before} to {after}"
        ));
    }
    if let (Some(before), Some(after)) = (
        initial.resident_memory_kib,
        final_sample.resident_memory_kib,
    ) && after > before.saturating_add(128 * 1024)
    {
        return Err(format!("soak RSS grew from {before} KiB to {after} KiB"));
    }
    if let (Some(before), Some(after)) = (initial.threads, final_sample.threads)
        && after > before.saturating_add(16)
    {
        return Err(format!("soak threads grew from {before} to {after}"));
    }
    Ok(())
}
