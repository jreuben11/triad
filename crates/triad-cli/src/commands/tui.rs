use clap::Args;

#[derive(Args)]
pub struct TuiArgs {
    #[arg(long, env = "TRIAD_ADMIN_URL", default_value = "http://localhost:8080")]
    pub admin_url: String,
    #[arg(long, default_value = "1000")]
    pub poll_ms: u64,
}

pub async fn run_tui(args: TuiArgs) -> anyhow::Result<()> {
    let status = std::process::Command::new("triad-tui")
        .arg("--admin-url")
        .arg(&args.admin_url)
        .arg("--poll-ms")
        .arg(args.poll_ms.to_string())
        .status();
    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => anyhow::bail!("triad-tui exited with status {}", s),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!(
                "triad-tui binary not found in PATH — build it first with `cargo build -p triad-tui`"
            )
        }
        Err(e) => Err(e.into()),
    }
}
