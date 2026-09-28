//! Minimal Exasol WebSocket API v3 client for E2E tests.

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use native_tls::TlsConnector;
use rand::rngs::OsRng;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::pkcs8::DecodePublicKey;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use serde_json::{Value, json};
use std::net::TcpStream;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, client_tls_with_config};

/// Exasol treats `numBytes` as a soft budget and returns whole rows, so this bounds one
/// response's size, not the result set.
const DEFAULT_FETCH_NUM_BYTES: u64 = 67_108_864;

pub struct ExaConn {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    redact_sql: bool,
    result_set_max_rows: u32,
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
        let tcp = TcpStream::connect(format!("{host}:{port}"))
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
        ExaConn {
            ws,
            redact_sql,
            result_set_max_rows: 0,
        }
    }

    pub fn execute(&mut self, sql: &str) -> Value {
        let cmd = json!({
            "command": "execute",
            "sqlText": sql,
            "attributes": {"resultSetMaxRows": self.result_set_max_rows}
        });
        self.ws
            .send(Message::Text(cmd.to_string().into()))
            .expect("send execute");
        let resp = Self::read_json(&mut self.ws);
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
                "Exasol execute failed for SQL:\n{sql}\n\nError: {resp}"
            );
        }
        resp
    }

    pub fn try_execute(&mut self, sql: &str) -> Value {
        let cmd = json!({
            "command": "execute",
            "sqlText": sql,
            "attributes": {"resultSetMaxRows": self.result_set_max_rows}
        });
        self.ws
            .send(Message::Text(cmd.to_string().into()))
            .expect("send execute");
        Self::read_json(&mut self.ws)
    }

    /// A declared cap reaches the adapter as a pushdown `limit` on real execution (#314),
    /// though `EXPLAIN VIRTUAL` never shows it. A test asserting pushdown or plan shape
    /// must not declare one: it would silently alter the plan under test.
    pub fn capped_result_sets(mut self, max_rows: u32) -> Self {
        self.result_set_max_rows = max_rows;
        self
    }

    /// A `DECIMAL` result comes back as a JSON string, so a string is parsed too.
    pub fn query_scalar_i64(&mut self, sql: &str) -> i64 {
        let resp = self.execute(sql);
        let value = &resp["responseData"]["results"][0]["resultSet"]["data"][0][0];
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("expected i64 scalar from:\n{sql}\n\nResponse: {resp}"))
    }

    pub fn query_row_count(&mut self, sql: &str) -> i64 {
        let resp = self.execute(sql);
        resp["responseData"]["results"][0]["resultSet"]["numRows"]
            .as_i64()
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
    fn drop(&mut self) {
        let _ = self.ws.close(None);
    }
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
