#!/usr/bin/env bash
# Item 3: what identity attributes are actually reachable, verified against a
# LIVE Exasol -- not read off documentation.
#
# Needs the repo's Exasol container:  docker compose up -d exasol
# Fails (never skips) when the DB is unreachable, per the project's E2E contract.
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$SPIKE_DIR/evidence/06-exasol-identity.txt"
PORT="${LH_EXASOL_PORT:-28563}"
DSN="exasol://sys:exasol@127.0.0.1:${PORT}?validateservercertificate=0"

q() { exapump sql -d "$DSN" "$1"; }

{
	echo "### live Exasol: $(q 'SELECT PARAM_VALUE FROM SYS.EXA_METADATA WHERE PARAM_NAME = '"'"'databaseProductVersion'"'"'' | tail -1)"
	echo "### dsn: exasol://sys:***@127.0.0.1:${PORT}?validateservercertificate=0"
	echo
} >"$OUT"

echo "-- setup: a role, a user holding it, and a second role the user does NOT hold" >>"$OUT"
q "CREATE ROLE OPA_EU_STAFF" >/dev/null 2>&1 || true
q "CREATE ROLE OPA_AUDITORS" >/dev/null 2>&1 || true
q "CREATE USER OPA_ALICE IDENTIFIED BY \"pw_Alice1\"" >/dev/null 2>&1 || true
q "GRANT CREATE SESSION TO OPA_ALICE" >/dev/null
q "GRANT OPA_EU_STAFF TO OPA_ALICE" >/dev/null
echo "   created ROLE OPA_EU_STAFF, ROLE OPA_AUDITORS, USER OPA_ALICE (holds only OPA_EU_STAFF)" >>"$OUT"
echo >>"$OUT"

probe() { # probe <label> <sql>
	echo "--------------------------------------------------------------" >>"$OUT"
	echo "Q: $2" >>"$OUT"
	echo "A:" >>"$OUT"
	if out=$(q "$2" 2>&1); then echo "$out" | sed 's/^/   /' >>"$OUT"; else
		echo "   ERROR: $out" | head -3 | sed 's/^/   /' >>"$OUT"; fi
	echo >>"$OUT"
}

echo "=== (1) what the ADAPTER's own session sees (it runs as the VS owner) ===" >>"$OUT"
probe "" "SELECT CURRENT_USER, CURRENT_SESSION, CURRENT_SCHEMA"
probe "" "SELECT * FROM SYS.EXA_USER_ROLE_PRIVS ORDER BY 2"

echo "=== (2) roles of an ARBITRARY user, which is what a policy input needs ===" >>"$OUT"
probe "" "SELECT GRANTEE, GRANTED_ROLE FROM SYS.EXA_DBA_ROLE_PRIVS WHERE GRANTEE = 'OPA_ALICE' ORDER BY 2"
probe "" "SELECT GRANTEE, GRANTED_ROLE FROM SYS.EXA_DBA_ROLE_PRIVS WHERE GRANTEE = 'OPA_ALICE' AND GRANTED_ROLE = 'OPA_AUDITORS'"

echo "=== (2b) role-to-role grants, i.e. whether a policy can get the TRANSITIVE closure ===" >>"$OUT"
q "GRANT OPA_AUDITORS TO OPA_EU_STAFF" >/dev/null 2>&1 || true
probe "" "SELECT * FROM SYS.EXA_ROLE_ROLE_PRIVS ORDER BY 1, 2"
echo "   ^ EXA_ROLE_ROLE_PRIVS shows only role-to-role grants under roles the CURRENT" >>"$OUT"
echo "     session holds, so it is empty here. The same grant IS visible in DBA_ROLE_PRIVS:" >>"$OUT"
echo >>"$OUT"
probe "" "SELECT * FROM SYS.EXA_DBA_ROLE_PRIVS WHERE GRANTEE IN ('OPA_ALICE', 'OPA_EU_STAFF') ORDER BY 1, 2"
echo "   ^ user-to-role AND role-to-role grants come from ONE view, so a policy input" >>"$OUT"
echo "     builder can compute the transitive role closure from a single query." >>"$OUT"
echo >>"$OUT"

echo "=== (3) is there any user ATTRIBUTE store beyond the name and roles? ===" >>"$OUT"
probe "" "SELECT * FROM SYS.EXA_ALL_USERS WHERE USER_NAME = 'OPA_ALICE'"
echo "   ^ the complete EXA_ALL_USERS column list: name, created, consumer group, free-text comment." >>"$OUT"
echo >>"$OUT"
probe "" "SELECT USER_NAME, DISTINGUISHED_NAME, KERBEROS_PRINCIPAL, OPENID_SUBJECT, USER_CONSUMER_GROUP FROM SYS.EXA_DBA_USERS ORDER BY 1"
echo "   ^ EXA_DBA_USERS additionally carries the EXTERNAL identity links (LDAP DN," >>"$OUT"
echo "     Kerberos principal, OIDC subject). Empty here because this container's users" >>"$OUT"
echo "     authenticate by password; on an IdP-backed cluster these are the join key to" >>"$OUT"
echo "     an external attribute source." >>"$OUT"
echo >>"$OUT"

echo "=== (4) NEGATIVE CONTROL: a fabricated view name must ERROR, not return empty ===" >>"$OUT"
probe "" "SELECT * FROM SYS.EXA_USER_ATTRIBUTES"

echo "=== (5) what CURRENT_USER is for the END USER's own session ===" >>"$OUT"
ADSN="exasol://OPA_ALICE:pw_Alice1@127.0.0.1:${PORT}?validateservercertificate=0"
echo "--------------------------------------------------------------" >>"$OUT"
echo "Q: SELECT CURRENT_USER  (connected AS OPA_ALICE)" >>"$OUT"
{ exapump sql -d "$ADSN" "SELECT CURRENT_USER" 2>&1 || true; } | sed "s/^/   /" >>"$OUT"
echo >>"$OUT"
echo "Q: SELECT ROLE_NAME FROM SYS.EXA_USER_ROLE_PRIVS  (connected AS OPA_ALICE)" >>"$OUT"
{ exapump sql -d "$ADSN" "SELECT ROLE_NAME FROM SYS.EXA_USER_ROLE_PRIVS ORDER BY 1" 2>&1 || true; } | sed "s/^/   /" >>"$OUT"
echo >>"$OUT"

echo "=== (6) cleanup ===" >>"$OUT"
for s in "DROP USER OPA_ALICE CASCADE" "DROP ROLE OPA_EU_STAFF CASCADE" "DROP ROLE OPA_AUDITORS CASCADE"; do
	q "$s" >/dev/null 2>&1 && echo "   $s -- ok" >>"$OUT" || echo "   $s -- failed" >>"$OUT"
done

echo "wrote $OUT"
