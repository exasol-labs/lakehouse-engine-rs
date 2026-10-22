-- Probe adapter for #425: answers createVirtualSchema and refresh from the properties
-- SRC_TABLES (tables the "source" lists) and SRC_SKIP (listed but not loadable), writes a
-- table-level identifier note per table, and fails every pushdown with the involvedTables it saw.
-- Each refresh records the keys of its own request, so a check can show what a refresh carries.
-- setProperties returns no schemaMetadata, so ALTER ... SET changes the "source" without a refresh.
CREATE SCHEMA IF NOT EXISTS PROBE_ADAPTER;
CREATE OR REPLACE PYTHON3 ADAPTER SCRIPT PROBE_ADAPTER.ADAPTER AS
import json
import time

CAPS = ["SELECTLIST_PROJECTION", "FILTER_EXPRESSIONS", "FN_PRED_EQUAL", "LITERAL_EXACTNUMERIC",
        "JOIN", "JOIN_TYPE_INNER", "JOIN_CONDITION_EQUI"]


def names(props, key):
    return [s.strip().upper() for s in (props.get(key) or "").split(",") if s.strip()]


def table(name, stamp):
    return {
        "name": name,
        "adapterNotes": json.dumps({"name": "ns." + name.lower(), "stamp": stamp}),
        "columns": [{"name": "C1", "dataType": {"type": "DECIMAL", "precision": 18, "scale": 0}}],
    }


def adapter_call(request_json):
    req = json.loads(request_json)
    kind = req["type"]
    if kind == "getCapabilities":
        return json.dumps({"type": kind, "capabilities": CAPS})
    if kind in ("dropVirtualSchema", "setProperties"):
        return json.dumps({"type": kind})
    info = req.get("schemaMetadataInfo", {})
    props = info.get("properties", {})
    if kind == "pushdown":
        raise Exception("PROBE_PUSHDOWN " + json.dumps(
            [{"name": t.get("name"), "adapterNotes": t.get("adapterNotes")}
             for t in req.get("involvedTables", [])]))
    stamp = str(time.time())
    loadable = [t for t in names(props, "SRC_TABLES") if t not in names(props, "SRC_SKIP")]
    requested = req.get("requestedTables")
    returned = loadable if requested is None else [t for t in requested if t in loadable]
    resp = {"type": kind, "schemaMetadata": {
        "tables": [table(t, stamp) for t in returned],
        "adapterNotes": json.dumps({"LAST_TYPE": kind, "REQUESTED": requested, "stamp": stamp,
                                  "REQ_KEYS": sorted(req), "INFO_KEYS": sorted(info)})}}
    if requested is not None:
        resp["requestedTables"] = requested
    return json.dumps(resp)
/
