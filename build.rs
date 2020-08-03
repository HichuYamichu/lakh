fn main() {
    tonic_build::compile_protos("src/proto/workplace.proto").unwrap();
}
