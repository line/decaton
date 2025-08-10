#!/bin/bash
#
# Copyright 2020 LINE Corporation
#
# LINE Corporation licenses this file to you under the Apache License,
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

# A script to publish maven artifacts of Decaton.
#
# Usage: ./publish.sh $RELEASE_VERSION
#
set -eu

cd $(dirname $0)/..

# Ensure current branch is master and up-to-date
# Also checks there is no local modification to avoid it's included in build for making release artfiact
validate_git_status() {
    if [ $(git rev-parse --abbrev-ref HEAD) != "master" ]; then
        echo "git branch must be set to master"
        exit 1
    fi

    git fetch origin

    if [ $(git diff --stat master origin/master | wc -l) -ne 0 ]; then
        echo "there are differences between local and remote"
        exit 1
    fi

    if [ $(git status --porcelain | wc -l) -ne 0 ]; then
        echo "working directory is not clean"
        exit 1
    fi
}

# Bump version in gradle.properties and create a tag
bump_version() {
    new_version=$1

    sed -i "" -e "s/^version=.*$/version=$new_version/" gradle.properties
    ./gradlew build
    git add gradle.properties
    git add centraldogma/src/jsonschema/dist
    git commit -m "Release $new_version"

    git push origin master

    tag="v$new_version"
    git tag $tag
    git push origin $tag
}

################
# Main procedure
################

validate_git_status

# Extract version to be released
version="$1"
if [ -z "$version" ]; then
    echo "Usage: $0 RELEASE_VERSION" >&2
    exit 1
fi

# prevent double publishing
if [ $(git tag | grep "^v$version\$" | wc -l) -ne 0 ]; then
    echo "$version already released"
    exit 1
fi

echo "Publishing Decaton $version"
./gradlew -P snapshot=false -P version="$version" clean build validateDocs publish
./gradlew sonatypeUpload # this will take a while
bump_version $version
