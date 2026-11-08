fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/symbols/v1/symbols.proto",
        "proto/searcher/v1/searcher.proto",
        "proto/gitserver/v1/gitserver.proto",
        "proto/internalapi/v1/internalapi.proto",
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto", "/usr/include"])?;

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
