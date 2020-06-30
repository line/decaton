#!/bin/bash
set -e

ASYNC_PROFILER_VERSION=1.7
ASYNC_PROFILER_URL_BASE="https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}"
JTASKSTATS_VERSION=0.2.0
JTASKSTATS_URL_BASE="https://github.com/kawamuray/jtaskstats/releases/download/v${JTASKSTATS_VERSION}"

extra_opts=""
classpath="${CLASSPATH:-$(ls $(dirname $0)/build/libs/benchmark-*-shadow.jar | sort -nr | head -1)}"

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

if [[ "$*" == *--taskstats* ]] && [[ "$*" != *--taskstats-bin* ]] && ! which jtaskstats >/dev/null 2>&1; then
    file="/tmp/jtaskstats-${JTASKSTATS_VERSION}"
    if ! [ -e "$file" ]; then
        case "$(uname -s)" in
            Linux*)     platform=linux;;
            *)          echo "jtaskstats supports only linux" >&2; exit 1;;
        esac
        url="$JTASKSTATS_URL_BASE/jtaskstats-x86_64-linux-musl-${JTASKSTATS_VERSION}.gz"
        echo "Downloading jtaskstats from $url into $file" >&2
        curl -L "$url" | gunzip -c >$file
        chmod +x $file
        if [ "$DEBM_SKIP_SETCAP" != "yes" ]; then
            echo "Running 'sudo setcap cap_net_admin+ep $file' to grant required capability for jtaskstats" >&2
            sudo setcap cap_net_admin+ep $file
        fi
    fi
    extra_opts="$extra_opts --taskstats-bin=$file"
fi

exec java -XX:+UseG1GC -cp "$classpath" com.linecorp.decaton.benchmark.Main $extra_opts "$@"
