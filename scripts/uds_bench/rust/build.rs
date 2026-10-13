fn main() {
    println!("cargo:rerun-if-changed=../schemas/envelope.capnp");
    println!("cargo:rerun-if-changed=../schemas/envelope.fbs");
    capnpc::CompilerCommand::new()
        .src_prefix("../schemas")
        .file("../schemas/envelope.capnp")
        .run()
        .expect("generate Cap'n Proto bindings");
    let status = std::process::Command::new("flatc")
        .args(["--rust", "-o"])
        .arg(std::env::var_os("OUT_DIR").unwrap())
        .arg("../schemas/envelope.fbs")
        .status()
        .expect("run flatc 25.12.19");
    assert!(status.success(), "generate FlatBuffers bindings");
}
