fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/zoekt/webserver/v1/query.proto",
        "proto/zoekt/webserver/v1/webserver.proto",
    ];

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&protos, &["proto", "/usr/include"])?;

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
