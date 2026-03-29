fn main() {
    prost_build::compile_protos(
        &["src/statistical_manifest.proto"],
        &["src/"],
    ).expect("Failed to compile Protobuf manifests. Ensure 'protoc' is installed on your system.");
}
