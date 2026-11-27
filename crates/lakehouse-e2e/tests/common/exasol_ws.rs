//! Minimal Exasol WebSocket API v3 client for E2E tests.

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use native_tls::TlsConnector;
use rand::rngs::OsRng;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::pkcs8::DecodePublicKey;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde_json::{Value, json};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, client_tls_with_config};

/// Exasol treats `numBytes` as a soft budget and returns whole rows, so this bounds one
/// response's size, not the result set.
const DEFAULT_FETCH_NUM_BYTES: u64 = 67_108_864;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounds the wait for Exasol's answer to a close.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// The exception text of a response that must be an error. `context` names the statement or
/// case, as SQL or a label.
#[track_caller]
pub fn error_text(response: &Value, context: &str) -> String {
    assert_eq!(
        response["status"].as_str(),
        Some("error"),
        "{context} must fail: {response}"
    );
    response["exception"]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// The first cell of an execute response. A `DECIMAL` result comes back as a JSON string,
/// so a string is parsed too.
pub fn parse_int(v: &Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer value, got: {v:?}"))
}

/// The exported row count and the writer count of one export run in a fresh session.
#[track_caller]
pub fn profiled_export_counts(conn: &mut ExaConn, export_sql: &str) -> (i64, i64) {
    conn.execute("ALTER SESSION SET PROFILE = 'ON'");
    let resp = conn.execute(export_sql);
    let rows = resp["responseData"]["results"][0]["rowCount"]
        .as_i64()
        .unwrap_or_else(|| panic!("expected a rowCount from:\n{export_sql}\n\nResponse: {resp}"));
    conn.execute("ALTER SESSION SET PROFILE = 'OFF'");
    conn.execute("FLUSH STATISTICS");
    let parts = conn.query_columns(
        "SELECT OUT_ROWS FROM EXA_USER_PROFILE_LAST_DAY \
         WHERE SESSION_ID = CURRENT_SESSION AND COMMAND_NAME = 'EXPORT' AND PART_NAME = 'GROUP BY'",
    );
    let [writers] = parts[0].as_slice() else {
        panic!("the export {export_sql} must have one GROUP BY profile part: {parts:?}");
    };
    (rows, parse_int(writers))
}

/// The value of a one-row, one-column integer result, else `None`, so a query that returns
/// extra rows, extra columns, or text cannot pass on its first cell.
pub fn scalar_i64(resp: &Value) -> Option<i64> {
    let result_set = &resp["responseData"]["results"][0]["resultSet"];
    let data_type = &result_set["columns"][0]["dataType"];
    let one_cell =
        result_set["numRows"].as_u64() == Some(1) && result_set["numColumns"].as_u64() == Some(1);
    if !one_cell || data_type["type"] != "DECIMAL" || data_type["scale"] != 0 {
        return None;
    }
    let value = &result_set["data"][0][0];
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
}

/// The `numRows` of an execute response's first result set.
pub fn result_row_count(resp: &Value) -> Option<i64> {
    resp["responseData"]["results"][0]["resultSet"]["numRows"].as_i64()
}

pub struct ExaConn {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    redact_sql: bool,
}

impl ExaConn {
    pub fn connect(host: &str, port: u16, user: &str, password: &str) -> Self {
        Self::connect_inner(host, port, user, password, false)
    }

    /// `execute()` failures omit the SQL and the Exasol response so credential-bearing DDL
    /// cannot leak into test output.
    pub fn connect_redacting(host: &str, port: u16, user: &str, password: &str) -> Self {
        Self::connect_inner(host, port, user, password, true)
    }

    fn connect_inner(host: &str, port: u16, user: &str, password: &str, redact_sql: bool) -> Self {
        let url = format!("wss://{host}:{port}");
        let tls = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()
            .expect("build TLS connector");
        let tcp = connect_tcp(host, port)
            .unwrap_or_else(|e| panic!("TCP connect to Exasol at {host}:{port}: {e}"));
        let connector = tungstenite::Connector::NativeTls(tls);
        let (mut ws, _) = client_tls_with_config(url.as_str(), tcp, None, Some(connector))
            .expect("WebSocket TLS handshake with Exasol");

        ws.send(Message::Text(
            r#"{"command":"login","protocolVersion":3}"#.to_string().into(),
        ))
        .expect("send login initiation");
        let resp = Self::read_json(&mut ws);
        assert_eq!(
            resp["status"].as_str(),
            Some("ok"),
            "Exasol login initiation failed: {resp}"
        );
        let pem = resp["responseData"]["publicKeyPem"]
            .as_str()
            .expect("publicKeyPem in login response");

        let enc_password = encrypt_password(password, pem);
        let creds = json!({
            "command": "login",
            "protocolVersion": 3,
            "username": user,
            "password": enc_password,
            "useCompression": false,
            "clientName": "lakehouse-engine-test",
            "driverName": "lakehouse-engine-test",
            "clientOs": "Linux",
            "clientOsUsername": "ci",
            "clientRuntime": "Rust"
        });
        ws.send(Message::Text(creds.to_string().into()))
            .expect("send credentials");
        let resp = Self::read_json(&mut ws);
        assert_eq!(
            resp["status"].as_str(),
            Some("ok"),
            "Exasol authentication failed: {resp}"
        );
        ExaConn { ws, redact_sql }
    }

    #[track_caller]
    pub fn execute(&mut self, sql: &str) -> Value {
        let resp = self.try_execute(sql);
        self.assert_ok(&resp, sql);
        resp
    }

    /// `context` names the statement, as SQL or a label.
    #[track_caller]
    pub fn assert_ok(&self, resp: &Value, context: &str) {
        if self.redact_sql {
            // The Exasol error response may echo credentials from the SQL back.
            assert_eq!(
                resp["status"].as_str(),
                Some("ok"),
                "Exasol execute failed for SQL — check Exasol error"
            );
        } else {
            assert_eq!(
                resp["status"].as_str(),
                Some("ok"),
                "Exasol execute failed for SQL:\n{context}\n\nError: {resp}"
            );
        }
    }

    pub fn try_execute(&mut self, sql: &str) -> Value {
        self.send_execute(json!({"command": "execute", "sqlText": sql}))
    }

    /// Exasol applies `resultSetMaxRows` to this statement only, never to later ones on the
    /// session. The cap reaches the adapter as a pushdown `limit` on real execution (#314),
    /// though `EXPLAIN VIRTUAL` never shows it, so a test of pushdown or plan shape sends none.
    pub fn try_execute_capped(&mut self, sql: &str, max_rows: u32) -> Value {
        self.send_execute(json!({
            "command": "execute",
            "sqlText": sql,
            "attributes": {"resultSetMaxRows": max_rows}
        }))
    }

    fn send_execute(&mut self, cmd: Value) -> Value {
        self.ws
            .send(Message::Text(cmd.to_string().into()))
            .expect("send execute");
        Self::read_json(&mut self.ws)
    }

    pub fn query_scalar_i64(&mut self, sql: &str) -> i64 {
        let resp = self.execute(sql);
        scalar_i64(&resp)
            .unwrap_or_else(|| panic!("expected i64 scalar from:\n{sql}\n\nResponse: {resp}"))
    }

    pub fn query_row_count(&mut self, sql: &str) -> i64 {
        let resp = self.execute(sql);
        result_row_count(&resp)
            .unwrap_or_else(|| panic!("expected numRows from:\n{sql}\n\nResponse: {resp}"))
    }

    /// A zero-row result still yields one empty column per declared column.
    pub fn query_columns(&mut self, sql: &str) -> Vec<Vec<Value>> {
        let resp = self.execute(sql);
        let result_set = &resp["responseData"]["results"][0]["resultSet"];
        self.fetch_result_columns(result_set)
    }

    pub fn fetch_result_columns(&mut self, result_set: &Value) -> Vec<Vec<Value>> {
        self.fetch_result_columns_with_num_bytes(result_set, DEFAULT_FETCH_NUM_BYTES)
            .0
    }

    /// Returns the columns and the number of `fetch` responses consumed. Any short read
    /// panics, because a quietly short read lets an assertion pass against a prefix.
    pub fn fetch_result_columns_with_num_bytes(
        &mut self,
        result_set: &Value,
        num_bytes: u64,
    ) -> (Vec<Vec<Value>>, usize) {
        // A row-count result has no `resultSet`; reading it as zero columns passes vacuously.
        assert!(
            result_set.is_object(),
            "expected a result set, got {result_set}: the statement returned a row count, \
             or the caller read the wrong result"
        );
        let advertised = result_set["numRows"].as_u64();
        // Exasol omits `data` for a zero-row result, so declared columns are the only shape source.
        let declared_columns = result_set["numColumns"]
            .as_u64()
            .or_else(|| result_set["columns"].as_array().map(|c| c.len() as u64))
            .unwrap_or(0) as usize;
        let mut cols: Vec<Vec<Value>> = result_set["data"]
            .as_array()
            .map(|data| {
                data.iter()
                    .map(|col| col.as_array().cloned().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_else(|| vec![Vec::new(); declared_columns]);
        let mut rows_read = cols.first().map_or(0, |col| col.len() as u64);

        let handle = match result_set["resultSetHandle"].as_u64() {
            Some(h) => h,
            None => {
                if let Some(advertised) = advertised {
                    assert!(
                        advertised == 0 || !cols.is_empty(),
                        "inline result set advertised {advertised} rows but carried no columns"
                    );
                    for (index, col) in cols.iter().enumerate() {
                        assert_eq!(
                            col.len() as u64,
                            advertised,
                            "column {index} of the inline result set carries {} of the \
                             {advertised} rows it advertised",
                            col.len()
                        );
                    }
                }
                return (cols, 0);
            }
        };
        let num_rows = advertised.unwrap_or(0);

        let mut responses = 0usize;
        while rows_read < num_rows {
            let resp = self.fetch_rows(handle, rows_read, num_bytes);
            responses += 1;

            let rows_in_response = resp["responseData"]["numRows"].as_u64().unwrap_or_else(|| {
                panic!(
                    "fetch at startPosition {rows_read} of result set {handle} \
                     returned no responseData.numRows: {resp}"
                )
            });
            if rows_in_response == 0 {
                panic!(
                    "fetch at startPosition {rows_read} of result set {handle} returned \
                     0 rows with {} of {num_rows} rows still outstanding",
                    num_rows - rows_read
                );
            }

            let data = resp["responseData"]["data"].as_array().unwrap_or_else(|| {
                panic!(
                    "fetch at startPosition {rows_read} of result set {handle} returned \
                     no responseData.data array: {resp}"
                )
            });
            if cols.is_empty() {
                cols = vec![Vec::new(); data.len()];
            }
            assert_eq!(
                data.len(),
                cols.len(),
                "fetch response {responses} of result set {handle} changed the column \
                 count mid-read"
            );
            for (col, chunk) in cols.iter_mut().zip(data) {
                col.extend(chunk.as_array().cloned().unwrap_or_default());
            }

            rows_read += rows_in_response;
        }
        self.close_result_set(handle);

        assert!(
            num_rows == 0 || !cols.is_empty(),
            "result set {handle} advertised {num_rows} rows but its {responses} fetch \
             response(s) carried no columns"
        );
        for (index, col) in cols.iter().enumerate() {
            assert_eq!(
                col.len() as u64,
                num_rows,
                "column {index} of result set {handle} accumulated the wrong row count \
                 across {responses} fetch response(s)"
            );
        }
        (cols, responses)
    }

    fn fetch_rows(&mut self, handle: u64, start_position: u64, num_bytes: u64) -> Value {
        let fetch = json!({
            "command": "fetch",
            "resultSetHandle": handle,
            "startPosition": start_position,
            "numBytes": num_bytes
        });
        self.ws
            .send(Message::Text(fetch.to_string().into()))
            .expect("send fetch");
        Self::read_json(&mut self.ws)
    }

    fn close_result_set(&mut self, handle: u64) {
        let close = json!({"command":"closeResultSet","resultSetHandles":[handle]});
        self.ws.send(Message::Text(close.to_string().into())).ok();
        let _ = Self::read_json(&mut self.ws);
    }

    fn read_json(ws: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> Value {
        loop {
            let msg = ws.read().expect("read WebSocket message from Exasol");
            match msg {
                Message::Text(text) => {
                    return serde_json::from_str(text.as_str())
                        .expect("parse Exasol response as JSON");
                }
                Message::Ping(data) => {
                    ws.send(Message::Pong(data)).expect("pong Exasol");
                }
                Message::Pong(_) | Message::Frame(_) => {}
                Message::Binary(_) => panic!("unexpected binary WebSocket message from Exasol"),
                Message::Close(_) => panic!("Exasol WebSocket closed unexpectedly"),
            }
        }
    }
}

impl Drop for ExaConn {
    /// Completes the WebSocket close handshake. On Exasol 2025.1 a session that disconnects
    /// without it makes the next login wait about 5 s.
    fn drop(&mut self) {
        if let MaybeTlsStream::NativeTls(tls) = self.ws.get_ref() {
            let _ = tls.get_ref().set_read_timeout(Some(CLOSE_TIMEOUT));
        }
        let _ = self.ws.close(None);
        let _ = self.ws.flush();
        while self.ws.read().is_ok() {}
    }
}

fn connect_tcp(host: &str, port: u16) -> std::io::Result<TcpStream> {
    let mut last_err = None;
    for addr in (host, port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(tcp) => return Ok(tcp),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| std::io::Error::other("host resolved to no addresses")))
}

fn encrypt_password(password: &str, pem_key: &str) -> String {
    let key = if pem_key.contains("BEGIN RSA PUBLIC KEY") {
        RsaPublicKey::from_pkcs1_pem(pem_key).expect("parse PKCS#1 RSA key from Exasol")
    } else {
        RsaPublicKey::from_public_key_pem(pem_key).expect("parse PKCS#8 RSA key from Exasol")
    };
    let ciphertext = key
        .encrypt(&mut OsRng, Pkcs1v15Encrypt, password.as_bytes())
        .expect("RSA encrypt Exasol password");
    B64.encode(ciphertext)
}
