fn main() {
    let output = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .expect("failed to run rustc --version");
    let version = String::from_utf8_lossy(&output.stdout);
    println!("cargo:rustc-env=RUSTC_VERSION={}", version.trim());
    println!("cargo:rerun-if-changed=build.rs");
}
