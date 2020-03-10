#!/bin/bash

# A script to publish new version of Decaton
# DO NOT run this script manually

set -eu

cd $(dirname $0)/..

git config --global user.email "travis@travis-ci.org"
git config --global user.name "Travis CI"

# Publish the artifacts
./gradlew -P snapshot=false clean test publish

# Bump version in gradle.properties
VERSION=$(grep version gradle.properties | awk -F '=' '{printf $2}')
grep -v "version=$VERSION" gradle.properties > gradle.properties.tmp
cat gradle.properties.tmp > gradle.properties
rm gradle.properties.tmp
echo $VERSION | awk -F '.' '{print "version="$1"."$2"."$3+1}' >> gradle.properties

git add gradle.properties
git commit -m "$VERSION"

git push https://${GITHUB_TOKEN}@github.com/line/decaton.git master
