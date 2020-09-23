fn main() {
    tonic_build::compile_protos("src/proto/lakh.proto").unwrap();
}
