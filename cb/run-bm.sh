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

$root_dir/gradlew --no-daemon clean benchmark:shadowJar

function run_with_opts() {
    name=$1; shift
    tmp=$(mktemp)
    $root_dir/benchmark/debm.sh \
        --runs 3 \
        --title "$rev-$name" \
        --format=json \
        --profile \
        --profiler-bin="$PROFILER_BIN"  \
        --profiler-opts="-f $out_dir/$name-profile.html" \
        --file-name-only \
        --warmup 10000000 \
        --param=decaton.max.pending.records=10000 \
        --output=$tmp \
        "$@"
    mv $tmp $out_dir/$name-benchmark.json
}

$root_dir/cb/sysinfo.sh >$out_dir/sysinfo.json

run_with_opts "tasks_100k_latency_10ms_concurrency_20" --tasks 100000 --simulate-latency=10 --param=decaton.partition.concurrency=20
run_with_opts "tasks_1000k_latency_0ms_concurrency_20" --tasks 1000000 --simulate-latency=0 --param=decaton.partition.concurrency=20
run_with_opts "tasks_100k_latency_20ms_vthread" --tasks 100000 --simulate-latency=4 --latency-count=5 --param=decaton.subpartition.runtime=VIRTUAL_THREAD
run_with_opts "tasks_1000k_latency_0ms_vthread" --tasks 1000000 --simulate-latency=0 --param=decaton.subpartition.runtime=VIRTUAL_THREAD
