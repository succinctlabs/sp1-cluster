use chrono::prelude::*;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=BUILD_GIT_SHA");

    let git_sha = match env::var("BUILD_GIT_SHA") {
        Ok(git_sha) => git_sha,
        Err(_) => "unknown".to_string(),
    };

    let pretty_time = Utc::now().format("%Y-%m-%dT%H:%M:%S");
    let version_string = format!("{} {}", git_sha, pretty_time);
    println!("cargo:rustc-env=BUILD_VERSION={}", version_string);
    Ok(())
}
