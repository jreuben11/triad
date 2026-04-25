use anyhow::Result;

pub fn version() -> Result<()> {
    println!("triad       {}", env!("CARGO_PKG_VERSION"));
    println!(
        "os/arch     {}/{}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!("schema      v1");
    Ok(())
}
