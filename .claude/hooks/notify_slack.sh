#!/usr/bin/env bash
# Post a short message to the #alfred Slack channel.
#
# Usage:
#   notify_slack.sh "message"          Post immediately (Notification hook).
#   notify_slack.sh --mark-start       Record "user sent a message now" (UserPromptSubmit hook).
#   notify_slack.sh --stop "message"   Post only if the work stretch ran 5+ minutes and
#                                      nothing was posted in the last 10 minutes (Stop hook).
#                                      Appends the branch name and open PR URL.
#
# Safety rules:
# - Never prints or logs the webhook URL (all output is discarded, and the URL
#   is passed to curl via stdin so it never appears in `ps` output).
# - Hook stdin (which may contain code or file contents) is never read.
# - Only the branch name and PR URL are added to messages, never code or file contents.
# - Times out after 5 seconds per network call.
# - Always exits 0 so a Slack outage can never block Claude.

# Never fail the hook, whatever happens below.
trap 'exit 0' EXIT

MIN_WORK_SECONDS=300 # Stop posts only after 5+ minutes of work.
MIN_GAP_SECONDS=600  # Stop posts at most once every 10 minutes.

project_dir="${CLAUDE_PROJECT_DIR:-$(pwd)}"
state_dir="${project_dir}/.claude/.notify-state" # git-ignored
prompt_file="${state_dir}/last_prompt_ts"
posted_file="${state_dir}/last_stop_post_ts"

# Print the epoch seconds stored in a state file, or nothing if absent/invalid.
read_ts() {
  local value
  value="$(cat "$1" 2>/dev/null)" || return 0
  [[ "$value" =~ ^[0-9]+$ ]] && printf '%s' "$value"
  return 0
}

write_ts() {
  mkdir -p "$state_dir" 2>/dev/null || return 0
  printf '%s\n' "$(date +%s)" >"$1" 2>/dev/null
  return 0
}

# Stop-only gate: true when enough work time has passed and we are not rate limited.
should_post_stop() {
  local now start last
  now="$(date +%s)"

  start="$(read_ts "$prompt_file")"
  [ -n "$start" ] || return 1
  [ $((now - start)) -ge "$MIN_WORK_SECONDS" ] || return 1

  last="$(read_ts "$posted_file")"
  [ -z "$last" ] || [ $((now - last)) -ge "$MIN_GAP_SECONDS" ] || return 1
  return 0
}

# Branch name and open PR URL (if any), as one short suffix. Names only, no code.
context_suffix() {
  local branch pr_url suffix=""
  branch="$(git -C "$project_dir" branch --show-current 2>/dev/null)"
  [ -z "$branch" ] || suffix=" Branch: ${branch}."
  if command -v gh >/dev/null 2>&1; then
    # macOS has no `timeout` by default; run without a cap if it is missing.
    local cap=()
    command -v timeout >/dev/null 2>&1 && cap=(timeout 5)
    pr_url="$(cd "$project_dir" && "${cap[@]}" gh pr view --json url --jq .url 2>/dev/null)"
    [ -z "$pr_url" ] || suffix="${suffix} PR: ${pr_url}"
  fi
  printf '%s' "$suffix"
}

mode="post"
case "${1:-}" in
--mark-start)
  write_ts "$prompt_file"
  exit 0
  ;;
--stop)
  mode="stop"
  shift
  ;;
esac

message="${1:-}"
[ -n "$message" ] || exit 0

if [ "$mode" = "stop" ]; then
  should_post_stop || exit 0
  # Record the attempt before posting so a slow or failing call cannot cause a burst.
  write_ts "$posted_file"
  message="${message}$(context_suffix)"
fi

command -v op >/dev/null 2>&1 || exit 0
command -v curl >/dev/null 2>&1 || exit 0

url="$(op read 'op://dev_vault/cm2bdbgjqbgirz3nnu5ujt5req/credential' 2>/dev/null)" || exit 0
[ -n "$url" ] || exit 0

# Escape quotes/backslashes so the message cannot break the JSON payload.
escaped="${message//\\/\\\\}"
escaped="${escaped//\"/\\\"}"
payload="{\"text\":\"${escaped}\"}"

# `-K -` reads the config (including the URL) from stdin, keeping it out of argv.
printf 'url = "%s"\n' "$url" | curl --silent --show-error --max-time 5 \
  --request POST \
  --header 'Content-Type: application/json' \
  --data "$payload" \
  --output /dev/null \
  -K - >/dev/null 2>&1

exit 0
