#!/usr/bin/env bash
# Prints the Conventional Commit types, one per line. release-please's
# changelog-sections is the one list that PR titles and branch names follow.
set -euo pipefail

config="$(dirname "$0")/../../release-please-config.json"
jq -r '.["changelog-sections"][].type' "$config"
