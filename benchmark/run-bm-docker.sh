#!/bin/bash

# Usage:
# $ ./run-bm-docker.sh ./debm.sh --title "xxx" --tasks 10000 ...

opts=""
while [[ "$1" == '-'* ]]; do
    opts="$opts $1"
    shift
done

root_dir=$(cd $(dirname $0); pwd)
exec docker run --rm \
     --network host --cap-add NET_ADMIN --cap-add SYS_PTRACE  \
     -e DEBM_SKIP_SETCAP=yes \
     -v $root_dir:/decaton \
     $opts \
     decaton-benchmark:latest "$@"
