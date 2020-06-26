#!/bin/bash
set -ue

cpu_model=$(grep 'model name' /proc/cpuinfo  | head -1 | awk 'BEGIN {FS=": "}; {print $2}')
cpu_cores=$(grep 'vendor_id' /proc/cpuinfo  | wc -l)
mem_kb=$(grep 'MemTotal' /proc/meminfo | grep -oE '[0-9]+ kB' | awk '{print $1}')
mem_bytes=$(($mem_kb * 1024))
java_version=$(java -version 2>&1 | head -1 | grep -oE '"[^"]+"' | sed 's/"//g')
env_id=$(echo -n "$cpu_model:$cpu_cores:$mem_bytes:$java_version" | md5sum  | awk '{print $1}')


cat <<EOF
{
    "cpuModel": "$cpu_model",
    "cpuCores": $cpu_cores,
    "memoryBytes": $mem_bytes,
    "javaVersion": "$java_version",
    "envId": "$env_id"
}
EOF
