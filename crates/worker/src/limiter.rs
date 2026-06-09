use crate::MAX_WEIGHT_OVERRIDE;

/// Per-worker concurrency budget: the smaller of the RAM- and `/dev/shm`-derived limits.
///
/// `WORKER_MAX_WEIGHT_OVERRIDE` bypasses the cap for workers whose weight doesn't track host RAM
/// (e.g. GPU workers).
pub fn get_max_weight() -> usize {
    let total_ram_gb = total_memory_gb();
    let shm_gb = dev_shm_gb();
    let max_weight = compute_max_weight(total_ram_gb, shm_gb, *MAX_WEIGHT_OVERRIDE);
    // Surface the inputs so operators can see what /dev/shm was read and whether the cap fired.
    tracing::info!(
        total_ram_gb,
        ?shm_gb,
        max_weight,
        "computed worker max_weight"
    );
    max_weight
}

/// Pure budget logic, split from the I/O in [`get_max_weight`]. Weight ≈ GiB of working set per task.
fn compute_max_weight(
    total_ram_gb: u64,
    shm_gb: Option<u64>,
    override_weight: Option<usize>,
) -> usize {
    if let Some(max_weight) = override_weight {
        if max_weight as u64 > total_ram_gb {
            tracing::error!(
                max_weight,
                total_ram_gb,
                "WORKER_MAX_WEIGHT_OVERRIDE exceeds available RAM; risk of OOM"
            );
        }
        return max_weight;
    }
    // Measured RAM; never rounded above it, so the budget can't exceed physical memory.
    let ram_weight = total_ram_gb;
    // /dev/shm is the binding constraint for execution workers: cap to it when below RAM, error if
    // unset (~0 GiB), else fall back to RAM. Conservative proxy — weight only approximates resident shm.
    match shm_gb {
        Some(0) => {
            tracing::error!(
                ram_weight,
                "/dev/shm unconfigured (shm_size required); worker will accept no work until set"
            );
            0
        }
        Some(shm) if shm < ram_weight => shm as usize,
        _ => ram_weight as usize,
    }
}

/// Total memory budget in GiB, cgroup-aware.
///
/// Resolution order:
///   1. Cgroup memory limit — the correct answer inside Docker / k8s pods.
///   2. `/proc/meminfo` — the correct answer on bare hosts.
///
/// The cgroup step matters because `/proc/meminfo` inside a container reports
/// the host's RAM, not the cgroup limit; without it, memory-limited workers
/// detect host RAM and the coordinator over-schedules them.
///
/// Only a *hard* limit is read (`memory.max` / `memory.limit_in_bytes`, set by
/// Docker `--memory` or k8s `limits`). Soft reservations (k8s `requests`, ECS
/// `memoryReservation`) live in a cgroup file this does not read, so the hard
/// file reads "unlimited" and resolution falls back to host RAM. Deployments
/// that set only soft reservations must bound the budget via `/dev/shm`
/// ([`dev_shm_gb`]) or `WORKER_MAX_WEIGHT_OVERRIDE`.
fn total_memory_gb() -> u64 {
    let (total_bytes, source) = match cgroup_memory_limit_bytes() {
        Some(b) => (b, MemorySource::Cgroup),
        // sys_info::MemInfo::total is in KB.
        None => match sys_info::mem_info() {
            Ok(info) => (info.total * 1024, MemorySource::ProcMeminfo),
            Err(e) => {
                tracing::error!(error = %e, "failed to read /proc/meminfo; memory budget = 0");
                (0, MemorySource::Unavailable)
            }
        },
    };
    let total_gb = total_bytes / (1024 * 1024 * 1024);
    if total_gb == 0 {
        tracing::error!(
            ?source,
            total_bytes,
            "memory budget resolved to 0 GiB; worker will accept no work"
        );
    }
    tracing::debug!(?source, total_gb, "resolved total memory budget");
    total_gb
}

/// Where [`total_memory_gb`] read its figure, for operator-facing logs.
#[derive(Clone, Copy, Debug)]
enum MemorySource {
    Cgroup,
    ProcMeminfo,
    Unavailable,
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

/// `/dev/shm` size in GiB, or `None` if it can't be read or looks degenerate.
///
/// Execution children mmap their buffers into this tmpfs, so its size bounds concurrent execution
/// independently of total RAM.
fn dev_shm_gb() -> Option<u64> {
    let stat = rustix::fs::statvfs("/dev/shm").ok()?;
    // total bytes = blocks × fragment size; a 0 in either means "unknown", not a 0-GiB budget.
    if stat.f_frsize == 0 || stat.f_blocks == 0 {
        return None;
    }
    Some(stat.f_blocks.saturating_mul(stat.f_frsize) / 1024 / 1024 / 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NO_OVERRIDE: Option<usize> = None;

    #[test]
    fn override_takes_precedence_over_ram_and_shm() {
        assert_eq!(compute_max_weight(64, Some(32), Some(100)), 100);
    }

    #[test]
    fn ram_budget_when_shm_unknown() {
        // 60 (not a multiple of 16) must pass through unchanged — the budget is the
        // measured RAM, never rounded.
        assert_eq!(compute_max_weight(60, None, NO_OVERRIDE), 60);
    }

    #[test]
    fn caps_to_shm_when_smaller_than_ram() {
        assert_eq!(compute_max_weight(64, Some(48), NO_OVERRIDE), 48);
    }

    #[test]
    fn no_cap_when_shm_at_or_above_ram() {
        assert_eq!(compute_max_weight(64, Some(64), NO_OVERRIDE), 64);
        assert_eq!(compute_max_weight(64, Some(128), NO_OVERRIDE), 64);
    }

    #[test]
    fn unconfigured_shm_caps_to_zero() {
        assert_eq!(compute_max_weight(64, Some(0), NO_OVERRIDE), 0);
    }

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
