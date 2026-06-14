//! Build script: compile the Vela protobuf definitions into Rust via
//! `prost`/`tonic` codegen.
//!
//! `prost-build` requires a `protoc` binary. Rather than depend on one being
//! installed on the host, we point `PROTOC` at the binary vendored by
//! `protoc-bin-vendored`, keeping the build self-contained.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Use the vendored protoc so the build does not require protoc on the host.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    // Recompile when the proto definitions change.
    println!("cargo:rerun-if-changed=proto/vela.proto");

    // Generate client and server stubs; only message types exist today, but
    // enabling both means appending services in task 2.2 needs no build changes.
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/vela.proto"], &["proto"])?;

    Ok(())
}
