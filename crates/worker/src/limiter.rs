use crate::MAX_WEIGHT_OVERRIDE;

// Get current total RAM as MB, round up to nearest 16 GB
pub fn get_max_weight() -> usize {
    if let Some(max_weight) = *MAX_WEIGHT_OVERRIDE {
        return max_weight;
    }
    let info = sys_info::mem_info().unwrap();
    let total_ram = info.total;
    let total_ram_gb = total_ram / 1024 / 1024;
    // Round up to nearest 16 GB
    let max_weight = total_ram_gb.div_ceil(16) * 16;
    max_weight as usize
}
