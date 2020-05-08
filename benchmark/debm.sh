#!/bin/bash
set -e

ASYNC_PROFILER_VERSION=1.7
ASYNC_PROFILER_URL_BASE="https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}"
extra_opts=""
classpath="$CLASSPATH:$(ls $(dirname $0)/build/libs/benchmark-*-shadow.jar | sort -nr | head -1)"

if [[ "$*" == *--profile* ]] && [[ "$*" != *--profiler-bin* ]] && ! which profiler.sh >/dev/null 2>&1; then
    dir="/tmp/async-profiler-${ASYNC_PROFILER_VERSION}"
    if ! [ -e "$dir/profiler.sh" ]; then
        case "$(uname -s)" in
            Linux*)     platform=linux;;
            Darwin*)    platform=macos;;
            *)          echo "Cannot determine platform to download async-profiler" >&2; exit 1;;
        esac
        url="$ASYNC_PROFILER_URL_BASE/async-profiler-${ASYNC_PROFILER_VERSION}-${platform}-x64.tar.gz"
        echo "Downloading async-profiler from $url into $dir" >&2
        mkdir -p $dir
        curl -L "$url" | tar zx -C $dir
    fi
    extra_opts="$extra_opts --profiler-bin=$dir/profiler.sh"
fi

exec java -XX:+UseG1GC -cp "$classpath" com.linecorp.decaton.benchmark.Main $extra_opts "$@"
