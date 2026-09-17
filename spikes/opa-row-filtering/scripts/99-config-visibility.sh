#!/usr/bin/env bash
# Where should the OPA server address live: a VS property or an Exasol
# CONNECTION object? The deciding question is what an ordinary query user can
# read back. Probes it live with a throwaway user holding only CREATE SESSION
# plus SELECT on an existing virtual schema, then drops it.
#
# Needs a live Exasol with at least one virtual schema already created
# (e.g. after `make test-e2e`). Fails, never skips, when unreachable.
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$SPIKE_DIR/evidence/15-config-visibility.txt"
HOST="${EXASOL_HOST:-127.0.0.1}"
PORT="${EXASOL_PORT:-28563}"
SYS="exasol://sys:${EXASOL_SYS_PASSWORD:-exasol}@$HOST:$PORT/?validateservercertificate=0"
PW='Probe-9137'
USR=OPA_CFG_PROBE
PROBE="exasol://$USR:$PW@$HOST:$PORT/?validateservercertificate=0"
q() { exapump sql --dsn "$1" "$2" 2>&1; }

command -v exapump >/dev/null || { echo "exapump not on PATH" >&2; exit 1; }
VS=$(q "$SYS" "SELECT SCHEMA_NAME FROM SYS.EXA_DBA_VIRTUAL_SCHEMAS ORDER BY 1 LIMIT 1" |
	sed -n '3p' | tr -d '\r')
[ -n "$VS" ] || { echo "no virtual schema on $HOST:$PORT -- run make test-e2e first" >&2; exit 1; }

{
	echo "### Exasol $(q "$SYS" "SELECT PARAM_VALUE FROM SYS.EXA_METADATA WHERE PARAM_NAME='databaseProductVersion'" | sed -n '3p')"
	echo "### probe user: $USR -- CREATE SESSION + SELECT ON SCHEMA $VS, nothing else"
	echo "### (the same privilege shape issue #402 relies on)"
	echo
} >"$OUT"

q "$SYS" "CREATE USER $USR IDENTIFIED BY \"$PW\";
GRANT CREATE SESSION TO $USR;
GRANT SELECT ON SCHEMA $VS TO $USR;" | tail -1 | sed 's/^/### setup: /' >>"$OUT"
trap 'exapump sql --dsn "$SYS" "DROP USER $USR CASCADE" >/dev/null 2>&1 || true' EXIT

{
	echo
	echo "-- 1. the probe DOES see the virtual schemas themselves --"
	q "$PROBE" "SELECT COUNT(*) AS VISIBLE_VS FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS" | sed -n '2,3p' | sed 's/^/   /'
	echo
	echo "-- 2. but reads NOTHING from EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES --"
	q "$PROBE" "SELECT SCHEMA_NAME, PROPERTY_NAME, PROPERTY_VALUE FROM SYS.EXA_ALL_VIRTUAL_SCHEMA_PROPERTIES" | sed -n '1,2p' | sed 's/^/   /'
	echo "   (as SYS the same rows are there: $(q "$SYS" "SELECT COUNT(*) FROM SYS.EXA_DBA_VIRTUAL_SCHEMA_PROPERTIES" | sed -n '3p') rows in EXA_DBA_VIRTUAL_SCHEMA_PROPERTIES)"
	echo
	echo "-- 3. EXA_ALL_CONNECTIONS exposes no address, no user, no password --"
	q "$PROBE" "SELECT * FROM SYS.EXA_ALL_CONNECTIONS" | sed -n '2,4p' | sed 's/^/   /'
	echo "   columns are CONNECTION_NAME, CREATED, CONNECTION_COMMENT only."
	echo "   NOTE: the NAMES are visible even without a grant --"
	echo "     probe sees $(q "$PROBE" "SELECT COUNT(*) FROM SYS.EXA_ALL_CONNECTIONS" | sed -n '3p'), SYS sees $(q "$SYS" "SELECT COUNT(*) FROM SYS.EXA_DBA_CONNECTIONS" | sed -n '3p') -- so a connection NAME is not a secret."
	echo
	echo "-- 4. the probe cannot repoint the schema's configuration --"
	{ q "$PROBE" "ALTER VIRTUAL SCHEMA $VS SET ALLOW_HTTP='true'" || true; } |
		grep -oE 'insufficient privileges[^(]*' | head -1 | sed 's/^/   ALTER ... SET -> /'
	echo
	cat <<'NOTE'
-- conclusion --
Neither store leaks the value to an ordinary query user: VS property values are
invisible to a plain SELECT grantee, and EXA_ALL_CONNECTIONS carries no address
or credential at all. So secrecy of the OPA URL does not decide this. What
decides it is that a CONNECTION already has a credential slot (its password),
is a separately grantable object, and is the pattern CATALOG_CONNECTION already
uses for the catalog endpoint.
NOTE
} >>"$OUT"
echo "wrote $OUT"
