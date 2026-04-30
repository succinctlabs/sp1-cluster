use crate::MAX_WEIGHT_OVERRIDE;

/// Resolution order:
///   1. `WORKER_MAX_WEIGHT_OVERRIDE` env var — explicit operator override.
///   2. Cgroup memory limit — the correct answer inside Docker / k8s pods.
///   3. `/proc/meminfo` — the correct answer on bare hosts.
///
/// Why the cgroup step matters: `/proc/meminfo` inside a container reports
/// the **host's** RAM, not the container's cgroup limit. The previous code
/// only knew about (1) and (3), so workers in `deploy.resources.limits.memory`-
/// limited containers auto-detected the host's RAM, the coordinator
/// over-scheduled them, and we OOMed unless an operator manually pinned
/// `WORKER_MAX_WEIGHT_OVERRIDE`. The cgroup step closes that.
pub fn get_max_weight() -> usize {
    if let Some(max_weight) = *MAX_WEIGHT_OVERRIDE {
        tracing::info!(
            max_weight,
            source = "WORKER_MAX_WEIGHT_OVERRIDE",
            "worker max weight"
        );
        return max_weight;
    }

    let (total_bytes, source) = cgroup_memory_limit_bytes()
        .map(|b| (b, "cgroup"))
        .unwrap_or_else(|| {
            let info = sys_info::mem_info().unwrap();
            // sys_info::MemInfo::total is in KB.
            (info.total * 1024, "/proc/meminfo")
        });
    let total_gb = total_bytes / (1024 * 1024 * 1024);
    // Report the limit as-is. The previous code rounded up to the nearest
    // 16 GB to compensate for /proc/meminfo's "advertised total minus
    // kernel" wiggle, but that overshoots cgroup-reported limits
    // (a 24 GB container would round to 32 → over-schedule → OOM).
    // Task weights aren't 16-quantized either, so rounding doesn't buy
    // alignment.
    let max_weight = total_gb as usize;
    tracing::info!(max_weight, source, total_gb, "worker max weight");
    max_weight
}

/// Read the cgroup memory limit (in bytes) for the current process.
///
/// Returns `None` when there is no limit, the cgroup files don't exist, or
/// the value is the kernel's "unlimited" sentinel. Tries cgroup v2 first
/// (default on modern Linux), then falls back to v1.
fn cgroup_memory_limit_bytes() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        if let Some(v) = parse_cgroup_v2(&s) {
            return Some(v);
        }
    }
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Some(v) = parse_cgroup_v1(&s) {
            return Some(v);
        }
    }
    None
}

/// cgroup v2 memory.max: either a byte count or the literal string `max`
/// for "no limit".
fn parse_cgroup_v2(content: &str) -> Option<u64> {
    let s = content.trim();
    if s == "max" {
        return None;
    }
    s.parse().ok()
}

/// cgroup v1 memory.limit_in_bytes: always a byte count. The kernel writes
/// a sentinel value (typically `0x7FFFFFFFFFFFF000`) for "no limit". Treat
/// anything above 1 PiB as effectively unlimited so we fall back to
/// `/proc/meminfo`, which gives a more useful answer on bare hosts than
/// "we have 8 exabytes of RAM."
fn parse_cgroup_v1(content: &str) -> Option<u64> {
    let v: u64 = content.trim().parse().ok()?;
    if v >= (1 << 50) {
        return None;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_v2_max_means_no_limit() {
        assert_eq!(parse_cgroup_v2("max\n"), None);
        assert_eq!(parse_cgroup_v2("  max "), None);
    }

    #[test]
    fn cgroup_v2_byte_count_parses() {
        assert_eq!(parse_cgroup_v2("51539607552\n"), Some(51_539_607_552));
        assert_eq!(parse_cgroup_v2("0"), Some(0));
    }

    #[test]
    fn cgroup_v2_garbage_is_none() {
        assert_eq!(parse_cgroup_v2("garbage"), None);
        assert_eq!(parse_cgroup_v2(""), None);
    }

    #[test]
    fn cgroup_v1_byte_count_parses() {
        assert_eq!(parse_cgroup_v1("51539607552\n"), Some(51_539_607_552));
    }

    #[test]
    fn cgroup_v1_unlimited_sentinel_is_none() {
        // 0x7FFFFFFFFFFFF000 — what the kernel writes for "no limit" on
        // 4k-page x86_64. Anything past 1 PiB should be treated as
        // unlimited.
        assert_eq!(parse_cgroup_v1("9223372036854771712\n"), None);
        assert_eq!(parse_cgroup_v1(&format!("{}", 1u64 << 50)), None);
        assert_eq!(
            parse_cgroup_v1(&format!("{}", (1u64 << 50) - 1)),
            Some((1u64 << 50) - 1)
        );
    }
}
