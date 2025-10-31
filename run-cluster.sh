path=$1
mode=$2

echo "Running benchmark for $path in $mode mode"
RUST_LOG=info cargo run --bin sp1-cluster-cli -- bench input programs/$path/program.bin programs/$path/stdin.bin --mode $mode