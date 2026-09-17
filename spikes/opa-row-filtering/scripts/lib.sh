#!/usr/bin/env bash
# Shared setup for every capture script. Sourced, not executed.
set -euo pipefail

SPIKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVIDENCE="$SPIKE_DIR/evidence"
POLICIES="$SPIKE_DIR/policies"
BIN_DIR="${OPA_BIN_DIR:-$SPIKE_DIR/.bin}"
OPA="$BIN_DIR/opa"
OPA_PORT="${OPA_PORT:-8181}"
OPA_URL="http://127.0.0.1:${OPA_PORT}"

mkdir -p "$EVIDENCE" "$BIN_DIR"

ensure_opa() {
	if [[ ! -x "$OPA" ]]; then
		echo "downloading OPA to $OPA" >&2
		curl -sSL -o "$OPA" https://openpolicyagent.org/downloads/latest/opa_linux_amd64_static
		chmod +x "$OPA"
	fi
}

opa_up() {
	ensure_opa
	if curl -sf "$OPA_URL/health" >/dev/null 2>&1; then return 0; fi
	"$OPA" run --server --addr "127.0.0.1:${OPA_PORT}" \
		--log-level error "$POLICIES" >"$EVIDENCE/.opa-server.log" 2>&1 &
	echo $! >"$BIN_DIR/opa.pid"
	for _ in $(seq 1 50); do
		curl -sf "$OPA_URL/health" >/dev/null 2>&1 && return 0
		sleep 0.2
	done
	echo "OPA failed to start; see $EVIDENCE/.opa-server.log" >&2
	return 1
}

opa_down() {
	[[ -f "$BIN_DIR/opa.pid" ]] || return 0
	kill "$(cat "$BIN_DIR/opa.pid")" 2>/dev/null || true
	rm -f "$BIN_DIR/opa.pid"
}

# post <policy-path> <json-body-file>
post() {
	curl -sS -X POST "$OPA_URL/v1/data/$1" \
		-H 'Content-Type: application/json' \
		--data-binary "@$2"
}
