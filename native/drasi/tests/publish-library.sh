#!/usr/bin/env bash
set -euo pipefail

native="$(cd "$(dirname "$0")/.." && pwd)"
directory="$native/.verification/library-publication-$$"
mkdir "$directory"
trap 'rm -rf "$directory"' EXIT
printf 'old\n' > "$directory/library"
printf 'new\n' > "$directory/source"
exec 3< "$directory/library"
bash "$native/publish-library.sh" "$directory/source" "$directory/library"
read -r previous <&3
exec 3<&-
[[ "$previous" == old ]]
[[ "$(cat "$directory/library")" == new ]]
[[ "$(find "$directory" -name '*.publish-*' | wc -l | tr -d ' ')" == 0 ]]
echo "Shared library publication: previous inode preserved"
