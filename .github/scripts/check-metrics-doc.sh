#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
metrics_java="${repo_root}/processor/src/main/java/com/linecorp/decaton/processor/metrics/Metrics.java"
monitoring_doc="${repo_root}/docs/monitoring.adoc"

for file in "${metrics_java}" "${monitoring_doc}"; do
    if [[ ! -f "${file}" ]]; then
        echo "File not found: ${file}" >&2
        exit 1
    fi
done

# All meter declarations in Metrics.java use *.builder("<name>"), then MeterFilter prepends "decaton.".
code_metrics="$(sed -nE 's/.*\.builder\("([^"]+)".*/decaton.\1/p' "${metrics_java}" | sort -u)"
doc_metrics="$(awk '
    BEGIN { in_section = 0 }
    /^\/\/ metrics-doc-check:start$/ { in_section = 1; next }
    /^\/\/ metrics-doc-check:end$/ { in_section = 0; next }
    in_section && /^\|decaton\.[A-Za-z0-9_.-]+$/ {
        print substr($0, 2)
    }
' "${monitoring_doc}" | sort -u)"

if [[ -z "${code_metrics}" ]]; then
    echo "No metrics were extracted from ${metrics_java}." >&2
    exit 1
fi
if [[ -z "${doc_metrics}" ]]; then
    echo "No documented metrics were found in ${monitoring_doc}." >&2
    echo "Please keep the metrics table between // metrics-doc-check:start and // metrics-doc-check:end." >&2
    exit 1
fi

missing_docs="$(comm -23 <(printf '%s\n' "${code_metrics}") <(printf '%s\n' "${doc_metrics}"))"
stale_docs="$(comm -13 <(printf '%s\n' "${code_metrics}") <(printf '%s\n' "${doc_metrics}"))"

if [[ -n "${missing_docs}" || -n "${stale_docs}" ]]; then
    echo "Metric documentation check failed." >&2

    if [[ -n "${missing_docs}" ]]; then
        echo "Missing in docs/monitoring.adoc:" >&2
        printf '%s\n' "${missing_docs}" >&2
    fi
    if [[ -n "${stale_docs}" ]]; then
        echo "Not found in Metrics.java (remove or update docs):" >&2
        printf '%s\n' "${stale_docs}" >&2
    fi
    exit 1
fi

echo "Metric documentation check passed."
