fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client_protos = [
        "proto/zoekt/webserver/v1/query.proto",
        "proto/zoekt/webserver/v1/webserver.proto",
    ];
    let server_protos = ["proto/sourcegraph/zoekt/configuration/v1/configuration.proto"];

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&client_protos, &["proto", "/usr/include"])?;
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&server_protos, &["proto", "/usr/include"])?;

    for proto in client_protos.iter().chain(server_protos.iter()) {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
