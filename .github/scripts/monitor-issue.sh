#!/usr/bin/env bash
# Keep ONE open issue per monitor, so a problem notifies once rather than every
# 15 minutes, and its recovery closes it.
#
#   monitor-issue.sh <label> <title> <success|failure> <report.md> <comment|quiet>
#
#   failure, no open issue  -> open one (GitHub notifies the repo's watchers)
#   failure, issue open     -> "comment": add the report as a comment (daily jobs);
#                              "quiet": leave it (the 15-minute job)
#   success, issue open     -> comment that it recovered, close it
#   success, no issue       -> nothing
set -euo pipefail
label="$1"; title="$2"; outcome="$3"; report="$4"; repeat="$5"
run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"

gh label create "$label" --color B60205 --description "ניטור חיצוני (external-monitor.yml)" --force >/dev/null

open=$(gh issue list --label "$label" --state open --json number --jq '.[0].number // empty')

body() { cat "$report"; printf '\n[הריצה ב-GitHub Actions](%s)\n' "$run_url"; }

if [ "$outcome" = "failure" ]; then
  if [ -z "$open" ]; then
    body | gh issue create --title "$title" --label "$label" --body-file -
  elif [ "$repeat" = "comment" ]; then
    body | gh issue comment "$open" --body-file -
  fi
elif [ -n "$open" ]; then
  { printf '✅ **חזר לתקינות.**\n\n'; body; } | gh issue comment "$open" --body-file -
  gh issue close "$open"
fi
