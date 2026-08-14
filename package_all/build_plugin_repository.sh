#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
AGGREGATION_HOME="$SCRIPT_DIR/aggregation"
OUTPUT_ROOT="$SCRIPT_DIR/plugin-repository"
PLUGIN_TYPE=""
PLUGIN_NAME=""
BUILD_ALL=false
CHANNEL="production"
RELEASE="$(date -u +%Y%m%dT%H%M%SZ)"
RUNTIME_VERSION="${STUDIO_RUNTIME_VERSION:-1.0_jdk17-SNAPSHOT}"
MAX_ARTIFACT_BYTES=$((512 * 1024 * 1024))

declare -a COORDINATE_TYPES=()
declare -a COORDINATE_NAMES=()
declare -a COORDINATE_DIRECTORIES=()
declare -a TEMPORARY_PATHS=()
BUILT_COUNT=0
SKIPPED_COUNT=0

usage() {
    cat <<'EOF'
Build immutable DataAggregation plugin releases for an object-storage repository.

Usage:
  build_plugin_repository.sh [options]

Options:
  --aggregation-home PATH     Aggregation runtime directory (default: package_all/aggregation)
  --output-root PATH          Repository output directory (default: package_all/plugin-repository)
  --type TYPE                 Plugin type: source, reader, writer, transformer, report, or file-transfer
  --name NAME                 Plugin name; use together with --type
  --all                       Build every plugin below aggregation/plugin
  --channel CHANNEL           Repository channel (default: production)
  --release RELEASE           Immutable release identifier (default: current UTC timestamp)
  --runtime-version VERSION   Worker runtime compatibility value
  --max-artifact-bytes BYTES  Maximum generated plugin.zip size (default: 536870912)
  -h, --help                  Show this help

With neither --all nor --type/--name, all plugins are built.
Existing immutable releases are skipped without changing current.json.
Requires bash, jq, zip, and sha256sum or shasum.
EOF
}

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

require_value() {
    local option="$1"
    local value="${2:-}"
    [[ -n "$value" ]] || fail "$option requires a value"
}

assert_safe_segment() {
    local value="$1"
    local label="$2"
    if [[ -z "$value" || "$value" == "." || "$value" == ".." || ! "$value" =~ ^[A-Za-z0-9._-]+$ ]]; then
        fail "$label contains unsupported path characters: $value"
    fi
}

cleanup() {
    local path
    for path in "${TEMPORARY_PATHS[@]}"; do
        if [[ -e "$path" ]]; then
            rm -rf -- "$path"
        fi
    done
    return 0
}
trap cleanup EXIT

while (($# > 0)); do
    case "$1" in
        --aggregation-home)
            require_value "$1" "${2:-}"
            AGGREGATION_HOME="$2"
            shift 2
            ;;
        --output-root)
            require_value "$1" "${2:-}"
            OUTPUT_ROOT="$2"
            shift 2
            ;;
        --type)
            require_value "$1" "${2:-}"
            PLUGIN_TYPE="$2"
            shift 2
            ;;
        --name)
            require_value "$1" "${2:-}"
            PLUGIN_NAME="$2"
            shift 2
            ;;
        --all)
            BUILD_ALL=true
            shift
            ;;
        --channel)
            require_value "$1" "${2:-}"
            CHANNEL="$2"
            shift 2
            ;;
        --release)
            require_value "$1" "${2:-}"
            RELEASE="$2"
            shift 2
            ;;
        --runtime-version)
            require_value "$1" "${2:-}"
            RUNTIME_VERSION="$2"
            shift 2
            ;;
        --max-artifact-bytes)
            require_value "$1" "${2:-}"
            MAX_ARTIFACT_BYTES="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown option: $1"
            ;;
    esac
done

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v zip >/dev/null 2>&1 || fail "zip is required"
if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
    fail "sha256sum or shasum is required"
fi

assert_safe_segment "$CHANNEL" "Channel"
assert_safe_segment "$RELEASE" "Release"
[[ -n "$RUNTIME_VERSION" ]] || fail "RuntimeVersion must not be blank"
[[ "$MAX_ARTIFACT_BYTES" =~ ^[0-9]+$ ]] || fail "MaxArtifactBytes must be a positive integer"
((MAX_ARTIFACT_BYTES > 0)) || fail "MaxArtifactBytes must be positive"

case "$PLUGIN_TYPE" in
    ""|source|reader|writer|transformer|report|file-transfer) ;;
    *) fail "unsupported plugin type: $PLUGIN_TYPE" ;;
esac

if [[ "$BUILD_ALL" == false && -z "$PLUGIN_TYPE" && -z "$PLUGIN_NAME" ]]; then
    BUILD_ALL=true
fi
if [[ "$BUILD_ALL" == true && ( -n "$PLUGIN_TYPE" || -n "$PLUGIN_NAME" ) ]]; then
    fail "do not combine --all with --type or --name"
fi
if [[ "$BUILD_ALL" == false && ( -z "$PLUGIN_TYPE" || -z "$PLUGIN_NAME" ) ]]; then
    fail "specify both --type and --name for one plugin, or use --all"
fi

[[ -d "$AGGREGATION_HOME/plugin" ]] || fail "Aggregation plugin directory does not exist: $AGGREGATION_HOME/plugin"
AGGREGATION_HOME="$(cd -- "$AGGREGATION_HOME" && pwd -P)"
PLUGIN_ROOT="$AGGREGATION_HOME/plugin"
mkdir -p -- "$OUTPUT_ROOT"
OUTPUT_ROOT="$(cd -- "$OUTPUT_ROOT" && pwd -P)"

validate_coordinate() {
    local type="$1"
    local directory="$2"
    local name
    local config_name
    local config_path
    local metadata_name
    local jar_path

    name="$(basename -- "$directory")"
    assert_safe_segment "$name" "Name"
    config_name="plugin.json"
    [[ "$type" == "transformer" ]] && config_name="transformer.json"
    config_path="$directory/$config_name"
    [[ -f "$config_path" ]] || fail "Plugin $type/$name is missing $config_name"

    metadata_name="$(jq -er 'if (.name | type) == "string" then .name else error("name must be a string") end' "$config_path")" \
        || fail "Plugin $type/$name contains invalid $config_name"
    [[ "$metadata_name" == "$name" ]] || fail "Plugin metadata name does not match directory $type/$name"

    jar_path="$(find "$directory" -type f -iname '*.jar' -print -quit)"
    [[ -n "$jar_path" ]] || fail "Plugin $type/$name does not contain a JAR"

    COORDINATE_TYPES+=("$type")
    COORDINATE_NAMES+=("$name")
    COORDINATE_DIRECTORIES+=("$directory")
}

if [[ "$BUILD_ALL" == true ]]; then
    shopt -s nullglob
    for type in source reader writer transformer report file-transfer; do
        type_root="$PLUGIN_ROOT/$type"
        [[ -d "$type_root" ]] || continue
        for plugin_directory in "$type_root"/*; do
            [[ -d "$plugin_directory" ]] || continue
            validate_coordinate "$type" "$plugin_directory"
        done
    done
else
    assert_safe_segment "$PLUGIN_NAME" "Name"
    plugin_directory="$PLUGIN_ROOT/$PLUGIN_TYPE/$PLUGIN_NAME"
    [[ -d "$plugin_directory" ]] || fail "Plugin directory does not exist: $PLUGIN_TYPE/$PLUGIN_NAME"
    validate_coordinate "$PLUGIN_TYPE" "$plugin_directory"
fi

((${#COORDINATE_TYPES[@]} > 0)) || fail "No plugins were found below $PLUGIN_ROOT"

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print tolower($1)}'
    else
        shasum -a 256 "$1" | awk '{print tolower($1)}'
    fi
}

for ((index = 0; index < ${#COORDINATE_TYPES[@]}; index++)); do
    type="${COORDINATE_TYPES[$index]}"
    name="${COORDINATE_NAMES[$index]}"
    plugin_directory="${COORDINATE_DIRECTORIES[$index]}"
    plugin_base="$OUTPUT_ROOT/$CHANNEL/$type/$name"
    release_parent="$plugin_base/releases"
    release_directory="$release_parent/$RELEASE"
    if [[ -e "$release_directory" ]]; then
        [[ -d "$release_directory" && -f "$release_directory/plugin.zip" ]] \
            || fail "Existing immutable plugin release is incomplete: $release_directory"
        printf 'SKIP: %s/%s release %s already exists; current.json was not changed\n' "$type" "$name" "$RELEASE"
        SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
        continue
    fi

    mkdir -p -- "$release_parent"
    temporary_release="$release_parent/.${RELEASE}.tmp.$$.$index"
    mkdir -- "$temporary_release"
    TEMPORARY_PATHS+=("$temporary_release")
    zip_path="$temporary_release/plugin.zip"

    (cd -- "$plugin_directory" && zip -q -r "$zip_path" .)
    size="$(wc -c < "$zip_path")"
    size=$((size + 0))
    if ((size <= 0 || size > MAX_ARTIFACT_BYTES)); then
        fail "Plugin artifact size is outside the allowed range: $type/$name ($size bytes)"
    fi
    sha256="$(sha256_file "$zip_path")"
    mv -- "$temporary_release" "$release_directory"

    mkdir -p -- "$plugin_base"
    current_path="$plugin_base/current.json"
    temporary_pointer="$plugin_base/.current.json.tmp.$$.$index"
    TEMPORARY_PATHS+=("$temporary_pointer")
    updated_at="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
    jq -n \
        --arg type "$type" \
        --arg name "$name" \
        --arg release "$RELEASE" \
        --arg artifact "releases/$RELEASE/plugin.zip" \
        --arg sha256 "$sha256" \
        --argjson size "$size" \
        --arg runtimeVersion "$RUNTIME_VERSION" \
        --arg updatedAt "$updated_at" \
        '{schemaVersion: 1, type: $type, name: $name, release: $release, artifact: $artifact, sha256: $sha256, size: $size, runtimeVersion: $runtimeVersion, updatedAt: $updatedAt}' \
        > "$temporary_pointer"
    mv -f -- "$temporary_pointer" "$current_path"

    BUILT_COUNT=$((BUILT_COUNT + 1))
    printf 'Built %s/%s release %s (%s bytes, %s)\n' "$type" "$name" "$RELEASE" "$size" "$sha256"
done

printf 'Plugin repository is ready at %s (%s built, %s skipped)\n' "$OUTPUT_ROOT" "$BUILT_COUNT" "$SKIPPED_COUNT"
