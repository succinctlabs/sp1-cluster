use crate::MAX_WEIGHT_OVERRIDE;

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

/// Per-worker concurrency budget: the smaller of the RAM- and `/dev/shm`-derived limits.
///
/// `WORKER_MAX_WEIGHT_OVERRIDE` bypasses the cap for workers whose weight doesn't track host RAM
/// (e.g. GPU workers).
pub fn get_max_weight() -> usize {
    let total_ram_gb = sys_info::mem_info().unwrap().total / 1024 / 1024;
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
        assert_eq!(compute_max_weight(64, None, NO_OVERRIDE), 64);
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
    fn ram_budget_never_exceeds_measured() {
        // 60 (not a multiple of 16) must stay 60, not snap up to 64.
        assert_eq!(compute_max_weight(60, None, NO_OVERRIDE), 60);
    }
}
