#!/bin/bash

# A script to create a tag to publish new version of Decaton
#
# Usage: ./release.sh

set -eu

cd $(dirname $0)/..

# Ensure current branch is master and up-to-date
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

# Extract version to be released
VERSION=$(grep version gradle.properties | awk -F '=' '{printf $2}')
echo "Preparing to release Decaton $VERSION"

# Ensure current commit passes the tests
./gradlew --no-daemon -P snapshot=false clean test

# Create tag and push it (will trigger CI)
echo "git tag $VERSION"
echo "git push origin $VERSION"
