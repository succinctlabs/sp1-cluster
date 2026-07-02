use chrono::prelude::*;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=BUILD_GIT_SHA");
    println!("cargo:rerun-if-env-changed=VERGEN_GIT_SHA");

    // The base infra/Dockerfile sets VERGEN_GIT_SHA (not BUILD_GIT_SHA) in the
    // build stage, so prefer it. Fall back to BUILD_GIT_SHA for any caller that
    // still sets the old name, then "unknown" for plain local builds.
    let git_sha = env::var("VERGEN_GIT_SHA")
        .or_else(|_| env::var("BUILD_GIT_SHA"))
        .unwrap_or_else(|_| "unknown".to_string());

    // Clean git sha for build-identity reporting (cluster component manifest).
    println!("cargo:rustc-env=BUILD_GIT_SHA={git_sha}");

    // Existing human-readable version string (metrics / GetStats); unchanged shape.
    let pretty_time = Utc::now().format("%Y-%m-%dT%H:%M:%S");
    let version_string = format!("{git_sha} {pretty_time}");
    println!("cargo:rustc-env=BUILD_VERSION={version_string}");
    Ok(())
}
