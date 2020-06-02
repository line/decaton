#!/bin/bash
set -eu

config="${1:-}"
out_dir="${2:-}"
rev="${3:-}"
if [ -z "$config" ] || [ -z "$out_dir" ] || [ -z "$rev" ]; then
    echo "Usage: $0 PATH_TO_CONFIG OUT_DIR REVISION" >&2
    exit 1
fi
. "$config"

root_dir=$(dirname $0)/..

$root_dir/gradlew clean benchmark:shadowJar

function run_with_opts() {
    name=$1; shift
    $root_dir/benchmark/debm.sh \
        --title "$rev-$name" \
        --format=json \
        --profile \
        --profiler-bin="$PROFILER_BIN"  \
        --profiler-opts="-f $out_dir/$name-profile.svg" \
        --warmup 100000 \
        --param=decaton.max.pending.records=10000 \
        "$@" \
        >$out_dir/$name-benchmark.json
}

run_with_opts "tasks_10k_latency_10ms_concurrency_20" --tasks 10000 --simulate-latency=10 --param=decaton.partition.concurrency=20
run_with_opts "tasks_100k_latency_0ms_concurrency_20" --tasks 100000 --simulate-latency=0 --param=decaton.partition.concurrency=20
