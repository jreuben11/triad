fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/triad_admin.proto"], &["proto", "/usr/include"])?;
    Ok(())
}
