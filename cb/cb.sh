#!/bin/bash
set -eu

config="${1:-}"
if [ -z "$config" ]; then
    echo "Usage: $0 PATH_TO_CONFIG" >&2
    exit 1
fi
. "$config"

base_dir=$(dirname $0)
repo=$WORK_DIR/repo
last_rev_file=$WORK_DIR/last_revision
store_dir=$repo/$STORE_DIR

function log() {
    echo "[$(date +%Y-%m-%dT%H:%M:%S)] $*" >&2
}

function checkout() {
    log "Checking out '$1'"
    cleanup
    git checkout "$1"
}

function pull() {
    log "Pulling latest revision"
    git pull
}

function load_last_revision() {
    if [ ! -e $last_rev_file ]; then
        log "Last revision file $last_rev_file missing, creating with current HEAD"
        git rev-parse HEAD > $last_rev_file
    fi
    cat $last_rev_file
}

function revisions_since() {
    git rev-list --reverse $1..HEAD
}

function commit_rev() {
    rev=$1
    log "Committing revision $rev"
    git --git-dir=$repo/.git --work-tree=$repo add $store_dir/$rev
    git --git-dir=$repo/.git --work-tree=$repo commit -m"Auto add commit data: $rev"
    git --git-dir=$repo/.git --work-tree=$repo push
}

function update_last_rev() {
    log "Updating last revision to: $rev"
    echo -n "$1" > $last_rev_file
}

function cleanup() {
    git checkout .
    git clean -f .
}

mkdir -p $WORK_DIR
if [ ! -e $repo ]; then
    url=$(git config --get remote.origin.url)
    log "Cloning $url into $repo"
    git clone $url $repo
fi

log "Checking out $STORE_BRANCH at $repo"
git --git-dir=$repo/.git --work-tree=$repo checkout $STORE_BRANCH

checkout $BUILD_BRANCH
pull

last_rev=$(load_last_revision)
log "Loaded last revision: $last_rev"

mkdir -p $store_dir
for rev in $(revisions_since $last_rev); do
    rev_dir=$store_dir/$rev
    if [ -e $rev_dir ]; then
        log "Removing old commit data: $rev_dir"
        rm -rf $rev_dir
    fi
    mkdir -p $rev_dir

    checkout $rev
    log "Running benchmark for the revision $rev"
    $base_dir/run-bm.sh $config $rev_dir $rev

    commit_rev $rev
    update_last_rev $rev
done
