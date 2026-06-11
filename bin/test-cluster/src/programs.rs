use std::sync::LazyLock;

use sp1_sdk::{Elf, SP1Stdin};

pub static FIBONACCI_ELF: LazyLock<Elf> =
    LazyLock::new(|| load_elf(include_bytes!("../programs/fibonacci.elf.zst")));

pub static FIBONACCI_STDIN: LazyLock<SP1Stdin> =
    LazyLock::new(|| load_stdin(include_bytes!("../programs/fibonacci.stdin.zst")));

pub static RSP_ELF: LazyLock<Elf> =
    LazyLock::new(|| load_elf(include_bytes!("../programs/rsp.elf.zst")));

pub static RSP_STDIN: LazyLock<SP1Stdin> =
    LazyLock::new(|| load_stdin(include_bytes!("../programs/rsp.stdin.zst")));

/// Stdin for the fibonacci program with a custom iteration count `n` — the knob for
/// long-running proofs without committing new artifacts (the program reads one u32).
pub fn fibonacci_stdin(n: u32) -> SP1Stdin {
    let mut stdin = SP1Stdin::new();
    stdin.write(&n);
    stdin
}

fn load_elf(data_zst: &[u8]) -> Elf {
    Elf::Dynamic(
        zstd::decode_all(data_zst)
            .expect("Failed to decompress elf")
            .into(),
    )
}

fn load_stdin(data_zst: &[u8]) -> SP1Stdin {
    bincode::deserialize(
        zstd::decode_all(data_zst)
            .expect("Failed to decompress stdin")
            .as_slice(),
    )
    .expect("Failed to deserialize stdin")
}
