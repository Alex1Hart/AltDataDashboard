#!/usr/bin/env bash
set -euo pipefail

database_path="${1:-data/portwatch.duckdb}"
workflow_file="${2:-hiringwatch-daily.yml}"
branch="${3:-main}"

mkdir -p "$(dirname "$database_path")"
previous_run_id="$(gh run list \
  --workflow "$workflow_file" \
  --branch "$branch" \
  --status success \
  --limit 1 \
  --json databaseId \
  --jq '.[0].databaseId // empty')"

if [[ -z "$previous_run_id" ]]; then
  echo "No prior successful HiringWatch run exists; a new baseline will be created."
  exit 0
fi

temporary_directory="$(mktemp -d)"
trap 'rm -rf "$temporary_directory"' EXIT

echo "Restoring HiringWatch state from workflow run $previous_run_id."
gh run download "$previous_run_id" \
  --name hiringwatch-state \
  --dir "$temporary_directory"

artifact_database="$(find "$temporary_directory" -name 'portwatch.duckdb' -print -quit)"
if [[ -z "$artifact_database" ]]; then
  echo "The prior successful run did not contain portwatch.duckdb; refusing to reset history." >&2
  exit 1
fi

cp "$artifact_database" "$database_path"
echo "Restored $database_path."
