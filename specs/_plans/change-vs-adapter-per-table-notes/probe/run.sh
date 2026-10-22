#!/usr/bin/env bash
# Re-runs the #425 protocol probe against the local Docker Exasol. Prints each check and the
# virtual tables with their table-level ADAPTER_NOTES after it.
set -euo pipefail
export EXAPUMP_DSN="${EXAPUMP_DSN:-exasol://sys:exasol@localhost:28563?validateservercertificate=0}"
here="$(cd "$(dirname "$0")" && pwd)"
q() { exapump sql "$1" 2>&1 | grep -v 'statement executed' || true; }
tables() { q "SELECT TABLE_NAME, ADAPTER_NOTES FROM EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = 'PVS' ORDER BY 1"; }
q "DROP VIRTUAL SCHEMA IF EXISTS PVS CASCADE"
exapump sql - < "$here/vs-protocol-probe.sql" >/dev/null
q "CREATE VIRTUAL SCHEMA PVS USING PROBE_ADAPTER.ADAPTER WITH SRC_TABLES = 'T1,T2,T3,T4' SRC_SKIP = 'T4'"
echo "== (a) table notes persisted, and sent on single-table, join, and self-join pushdowns"; tables
q "SELECT C1 FROM PVS.T1" | grep -o 'PROBE_PUSHDOWN.*' | head -1
q "SELECT A.C1 FROM PVS.T1 A JOIN PVS.T2 B ON A.C1 = B.C1" | grep -o 'PROBE_PUSHDOWN.*' | head -1
q "SELECT A.C1 FROM PVS.T1 A JOIN PVS.T1 B ON A.C1 = B.C1" | grep -o 'PROBE_PUSHDOWN.*' | head -1
echo "== (b) REFRESH TABLES t2 with a T2-only response: T1 and T3 keep their notes, T2's stamp changes"
q "ALTER VIRTUAL SCHEMA PVS REFRESH TABLES t2"; tables
q "SELECT ADAPTER_NOTES FROM EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'PVS'"
echo "== (c) requested T3 omitted from the response: T3 is dropped"
q "ALTER VIRTUAL SCHEMA PVS SET SRC_SKIP = 'T3'"; q "ALTER VIRTUAL SCHEMA PVS REFRESH TABLES T3"; tables
echo "== (d) REFRESH TABLES T4, which has no virtual table yet: T4 is created"
q "ALTER VIRTUAL SCHEMA PVS REFRESH TABLES T4"; tables
echo "== (e) REFRESH TABLES NOPE, a name neither listed nor declared: statement succeeds, tables unchanged"
q "ALTER VIRTUAL SCHEMA PVS REFRESH TABLES NOPE"; tables
echo "== (f) keys of the last REFRESH TABLES request: REQ_KEYS lacks any table list, INFO_KEYS holds only adapterNotes, name, properties"
q "SELECT ADAPTER_NOTES FROM EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'PVS'"
q "DROP VIRTUAL SCHEMA PVS CASCADE"; q "DROP SCHEMA PROBE_ADAPTER CASCADE"
