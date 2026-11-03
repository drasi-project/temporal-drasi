#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 2 ]]; then
    echo "usage: $0 source destination" >&2
    exit 1
fi
source="$1"
destination="$2"
staged="${destination}.publish-$$"
if [[ -e "$staged" ]]; then
    echo "Library publication staging file already exists" >&2
    exit 1
fi
trap 'rm -f "$staged"' EXIT
cp "$source" "$staged"
# Rename preserves the mapped inode of any process using the previous library.
mv -f "$staged" "$destination"
