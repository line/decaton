#!/bin/bash
#
# Copyright 2025 LY Corporation
#
# LY Corporation licenses this file to you under the Apache License,
# version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at:
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

# A script to validate and compile Decaton ProcessorProperties with its schemas
#
# Usage: ./scripts/validate-jsonschema.sh

set -euo pipefail
shopt -s nullglob

cd "$(git rev-parse --show-toplevel || echo .)"

AJV_CLI_VER="5.0.0"
SCHEMA_DIR="jsonschema/dist"
INSTANCE="${SCHEMA_DIR}/decaton-processor-properties-central-dogma-example.json"

DRAFTS=(draft_7 draft_2019_09 draft_2020_12)
SUFFIXES=("" "-allow-additional-properties")

spec_opt() {
  case "$1" in
    draft_7)         echo "--spec=draft7"     ;;
    draft_2019_09)   echo "--spec=draft2019"  ;;
    draft_2020_12)   echo "--spec=draft2020"  ;;
    *)               echo ""                  ;;
  esac
}

# If npx is not installed, show an error message and exit
if ! command -v npx &> /dev/null; then
  echo "'npx' is not installed. Please install Node.js and npm to use npx." >&2
  exit 1
fi

echo "Start validation / compilation ..."
for draft in "${DRAFTS[@]}"; do
  for suffix in "${SUFFIXES[@]}"; do
    schema="${SCHEMA_DIR}/decaton-processor-properties-central-dogma-schema-${draft}${suffix}.json"

    [[ -f $schema ]] || { echo "Missing schema: $schema" >&2; exit 1; }

    spec=$(spec_opt "$draft")


    echo "Testing ${schema##*/} ..."
    # Check jsonschema itself
    npx -y ajv-cli@"$AJV_CLI_VER" compile  -s "$schema"                $spec
    # Check example json with jsonschema
    npx -y ajv-cli@"$AJV_CLI_VER" validate -s "$schema" -d "$INSTANCE" $spec
    echo
  done
done

echo "All done."
