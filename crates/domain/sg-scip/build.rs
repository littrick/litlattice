fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["proto/scip.proto"];
    prost_build::Config::new().compile_protos(&protos, &["proto"])?;

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
