use crate::MAX_WEIGHT_OVERRIDE;

// Get current total RAM as MB, round up to nearest 16 GB
pub fn get_max_weight() -> usize {
    let info = sys_info::mem_info().unwrap();
    let total_ram_gb = info.total / 1024 / 1024;
    if let Some(max_weight) = *MAX_WEIGHT_OVERRIDE {
        if max_weight as u64 > total_ram_gb {
            tracing::error!(
                max_weight,
                total_ram_gb,
                "WORKER_MAX_WEIGHT_OVERRIDE exceeds available RAM; risk of OOM"
            );
        }
        return max_weight;
    }
    // Round up to nearest 16 GB
    let max_weight = total_ram_gb.div_ceil(16) * 16;
    max_weight as usize
}
