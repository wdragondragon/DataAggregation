#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PACKAGE_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
BUILD_SCRIPT="$PACKAGE_DIR/build_plugin_repository.sh"
TEST_ROOT="$(mktemp -d)"
AGGREGATION_HOME="$TEST_ROOT/aggregation"
OUTPUT_ROOT="$TEST_ROOT/repository"
RELEASE="test-20260812-v1"

cleanup() {
    rm -rf -- "$TEST_ROOT"
}
trap cleanup EXIT

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

create_plugin() {
    local type="$1"
    local name="$2"
    local directory="$AGGREGATION_HOME/plugin/$type/$name"

    mkdir -p -- "$directory"
    printf '{"name":"%s"}\n' "$name" > "$directory/plugin.json"
    printf 'test jar for %s/%s\n' "$type" "$name" > "$directory/$name.jar"
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print tolower($1)}'
    else
        shasum -a 256 "$1" | awk '{print tolower($1)}'
    fi
}

create_plugin source alpha

first_output="$(bash "$BUILD_SCRIPT" \
    --aggregation-home "$AGGREGATION_HOME" \
    --output-root "$OUTPUT_ROOT" \
    --type source \
    --name alpha \
    --release "$RELEASE")"
[[ "$first_output" == *"(1 built, 0 skipped)"* ]] || fail "first build summary is incorrect"

alpha_base="$OUTPUT_ROOT/production/source/alpha"
alpha_zip="$alpha_base/releases/$RELEASE/plugin.zip"
alpha_pointer="$alpha_base/current.json"
[[ -f "$alpha_zip" && -f "$alpha_pointer" ]] || fail "first build did not create the expected files"
alpha_zip_hash="$(sha256_file "$alpha_zip")"
alpha_pointer_hash="$(sha256_file "$alpha_pointer")"

printf 'changed after immutable release\n' > "$AGGREGATION_HOME/plugin/source/alpha/alpha.jar"
second_output="$(bash "$BUILD_SCRIPT" \
    --aggregation-home "$AGGREGATION_HOME" \
    --output-root "$OUTPUT_ROOT" \
    --type source \
    --name alpha \
    --release "$RELEASE")"
[[ "$second_output" == *"SKIP: source/alpha release $RELEASE already exists"* ]] \
    || fail "repeat build did not print the skip message"
[[ "$second_output" == *"(0 built, 1 skipped)"* ]] || fail "repeat build summary is incorrect"
[[ "$(sha256_file "$alpha_zip")" == "$alpha_zip_hash" ]] || fail "repeat build changed the immutable ZIP"
[[ "$(sha256_file "$alpha_pointer")" == "$alpha_pointer_hash" ]] || fail "repeat build changed current.json"

create_plugin source beta
create_plugin file-transfer filetransfer
all_output="$(bash "$BUILD_SCRIPT" \
    --aggregation-home "$AGGREGATION_HOME" \
    --output-root "$OUTPUT_ROOT" \
    --all \
    --release "$RELEASE")"
[[ "$all_output" == *"SKIP: source/alpha release $RELEASE already exists"* ]] \
    || fail "all build did not skip the existing plugin"
[[ "$all_output" == *"Built source/beta release $RELEASE"* ]] \
    || fail "all build did not build the missing plugin"
[[ "$all_output" == *"Built file-transfer/filetransfer release $RELEASE"* ]] \
    || fail "all build did not discover the file-transfer plugin"
[[ "$all_output" == *"(2 built, 1 skipped)"* ]] || fail "all build summary is incorrect"
[[ -f "$OUTPUT_ROOT/production/source/beta/releases/$RELEASE/plugin.zip" ]] \
    || fail "all build did not create the missing release"
[[ -f "$OUTPUT_ROOT/production/file-transfer/filetransfer/releases/$RELEASE/plugin.zip" ]] \
    || fail "all build did not create the file-transfer release"

create_plugin source broken
mkdir -p -- "$OUTPUT_ROOT/production/source/broken/releases/$RELEASE"
set +e
broken_output="$(bash "$BUILD_SCRIPT" \
    --aggregation-home "$AGGREGATION_HOME" \
    --output-root "$OUTPUT_ROOT" \
    --type source \
    --name broken \
    --release "$RELEASE" 2>&1)"
broken_status=$?
set -e
[[ $broken_status -ne 0 ]] || fail "incomplete existing release was silently skipped"
[[ "$broken_output" == *"Existing immutable plugin release is incomplete"* ]] \
    || fail "incomplete existing release did not report the expected error"

printf 'PASS: existing releases are skipped and missing releases are built\n'
