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
# Usage: ./publish.sh [--release]
#   *  --relese: publish new version of artifacts to Maven Central
#   * otherwise: publish (or overwrite if exists) current SNAPSHOT version without changing version
set -eu

cd $(dirname $0)/..

# Ensure current branch is master and up-to-date
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
    version=$1
    new_version=$(echo $version | awk -F '.' '{print $1"."$2"."$3+1}')

    sed -i "" -e "s/^version=$version/version=$new_version/" gradle.properties

    git add gradle.properties

    if [ "$(git diff --name-only --staged)" != "gradle.properties" ]; then
          echo "unnecessary file is staged"
          exit 1
    fi
    git commit -m "Release $version"

    git push origin master

    tag="v$version"
    git tag $tag
    git push origin $tag
}

################
# Main procedure
################

validate_git_status

is_snapshot=true
if [[ $@ == *--release* ]]; then
    is_snapshot=false
fi

# Extract version to be released
version=$(grep "^version=" gradle.properties | cut -f 2 -d=)
if [ $is_snapshot = true ]; then
    version="$version-SNAPSHOT"
fi

echo "Publishing Decaton $version"

./gradlew -P snapshot=$is_snapshot clean build publish

if [ $is_snapshot = false ]; then
    bump_version $version
fi
