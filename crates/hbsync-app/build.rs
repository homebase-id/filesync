//! Stamps the version with the git commit it was built from ([`hbsync_app::VERSION`]):
//! `0.1.0 (47f0273)`, or `0.1.0` outside a git checkout.

use std::path::Path;
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    let text = String::from_utf8(out.stdout).ok()?;
    (out.status.success() && !text.trim().is_empty()).then(|| text.trim().to_owned())
}

fn main() {
    let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    // Again when HEAD moves: a checkout, or a commit on the current branch.
    if let (Some(dir), Some(common)) = (
        git(&["rev-parse", "--absolute-git-dir"]),
        git(&["rev-parse", "--path-format=absolute", "--git-common-dir"]),
    ) {
        println!("cargo:rerun-if-changed={dir}/HEAD");
        let refs = git(&["symbolic-ref", "-q", "HEAD"])
            .map(|branch| format!("{common}/{branch}"))
            .into_iter()
            .chain(Some(format!("{common}/packed-refs")));
        for path in refs.filter(|p| Path::new(p).exists()) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let stamped = match git(&["rev-parse", "--short=7", "HEAD"]) {
        Some(commit) => format!("{version} ({commit})"),
        None => version,
    };
    println!("cargo:rustc-env=HBSYNC_VERSION={stamped}");
}
