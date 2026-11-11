#!/usr/bin/env bash
# Post a short, fixed message to the #alfred Slack channel.
#
# Usage: notify_slack.sh "message"
#
# Safety rules:
# - Never prints or logs the webhook URL (all output is discarded, and the URL
#   is passed to curl via stdin so it never appears in `ps` output).
# - Hook stdin (which may contain code or file contents) is never read.
# - Times out after 5 seconds.
# - Always exits 0 so a Slack outage can never block Claude.

# Never fail the hook, whatever happens below.
trap 'exit 0' EXIT

message="${1:-}"
[ -n "$message" ] || exit 0

command -v op >/dev/null 2>&1 || exit 0
command -v curl >/dev/null 2>&1 || exit 0

url="$(op read 'op://dev_vault/cm2bdbgjqbgirz3nnu5ujt5req/credential' 2>/dev/null)" || exit 0
[ -n "$url" ] || exit 0

# Message is a fixed string passed by settings.json; escape quotes/backslashes anyway.
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
