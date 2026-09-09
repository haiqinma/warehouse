#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
MODULE_DIR="$(realpath "${SCRIPT_DIR}/..")"

usage() {
    echo "Usage: $0 <target-dir>" >&2
}

main() {
    if [[ $# -ne 1 ]]; then
        usage
        return 1
    fi

    local target_dir="$1"
    local source_config="${MODULE_DIR}/config.yaml"

    if [[ ! -f "$source_config" ]]; then
        echo "config file not found: $source_config" >&2
        return 1
    fi

    if [[ ! -d "$target_dir" ]]; then
        echo "target directory not found: $target_dir" >&2
        return 1
    fi

    cp "$source_config" "${target_dir}/config.yaml" || return 1

    return 0
}

main "$@"
