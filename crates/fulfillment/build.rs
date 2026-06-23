use vergen_git2::{Emitter, Git2Builder};

/// Emit `VERGEN_GIT_SHA` so the fulfiller can self-report the git commit it was
/// built from via the network `ReportProverInfo` RPC. Mirrors the worker crate's
/// build.rs but keeps to git info only (the only build constant the fulfiller
/// needs), to avoid pulling in heavier vergen instruction sets.
///
/// In Docker builds the value is supplied by the `VERGEN_GIT_SHA` build ARG/ENV
/// (see `infra/Dockerfile`); vergen honours that env override. Local builds read
/// it from the `.git` directory.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let git2 = Git2Builder::all_git()?;
    Emitter::default().add_instructions(&git2)?.emit()?;
    Ok(())
}
