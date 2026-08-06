//! Minimal Exasol WebSocket SQL client for E2E tests.
//!
//! Mirrors the sibling project's tests/common/exasol_ws.rs but is self-contained.
//! Implements just enough of the Exasol WebSocket API v3 to authenticate,
//! execute SQL, and fetch scalar / multi-row results.

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

pub struct ExaConn {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    redact_sql: bool,
    result_set_max_rows: u32,
}

impl ExaConn {
    /// Connect and authenticate to Exasol via WebSocket (TLS, self-signed cert accepted).
    ///
    /// `execute()` failures include the SQL statement and the Exasol response body for
    /// debuggability. Use `connect_redacting` when the SQL may carry credentials.
    pub fn connect(host: &str, port: u16, user: &str, password: &str) -> Self {
        Self::connect_inner(host, port, user, password, false)
    }

    /// Connect in redacting mode: `execute()` failures omit the SQL statement and the
    /// Exasol response body so credential-bearing DDL cannot leak into test output.
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

        // Step 1: initiate login to get server's RSA public key.
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

        // Step 2: encrypt password and complete login.
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
            result_set_max_rows: 10000,
        }
    }

    /// Execute SQL; panics on error. Returns the raw JSON response.
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
            // Redacting mode: the SQL may carry credentials (SigV4, vended keys) and the
            // Exasol error response may echo them back, so surface neither.
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

    /// Execute SQL; returns the raw response WITHOUT asserting status == ok.
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

    /// `0` is Exasol's own documented default meaning "no limit" (WebSocket API v3:
    /// `resultSetMaxRows`, "0 (default) means no limit"). Used only by tests that must
    /// observe row-fetch-time behavior the default 10000-row cap would otherwise mask
    /// (e.g. forcing every join onto the unaccelerated fallback via a pushdown `limit`).
    pub fn unbounded_result_sets(mut self) -> Self {
        self.result_set_max_rows = 0;
        self
    }

    /// Execute SQL and return first column of first row as i64.
    ///
    /// A `DECIMAL` result comes back as a JSON string (e.g. `"3"`), not a JSON
    /// number, so fall back to parsing a string — same tolerant approach as
    /// `parse_int` in the E2E test files.
    pub fn query_scalar_i64(&mut self, sql: &str) -> i64 {
        let resp = self.execute(sql);
        let value = &resp["responseData"]["results"][0]["resultSet"]["data"][0][0];
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("expected i64 scalar from:\n{sql}\n\nResponse: {resp}"))
    }

    /// Execute SQL and return row count from the result set metadata.
    pub fn query_row_count(&mut self, sql: &str) -> i64 {
        let resp = self.execute(sql);
        resp["responseData"]["results"][0]["resultSet"]["numRows"]
            .as_i64()
            .unwrap_or_else(|| panic!("expected numRows from:\n{sql}\n\nResponse: {resp}"))
    }

    /// Execute SQL and return all data as column-major Vec<Vec<Value>>.
    ///
    /// Fetches from a result set handle if necessary (large result sets).
    pub fn query_columns(&mut self, sql: &str) -> Vec<Vec<Value>> {
        let resp = self.execute(sql);
        let result_set = &resp["responseData"]["results"][0]["resultSet"];
        self.fetch_result_columns(result_set)
    }

    /// Fetch all data from a result set (inline or via handle).
    pub fn fetch_result_columns(&mut self, result_set: &Value) -> Vec<Vec<Value>> {
        if let Some(data) = result_set["data"].as_array() {
            return data
                .iter()
                .map(|col| col.as_array().cloned().unwrap_or_default())
                .collect();
        }
        let handle = match result_set["resultSetHandle"].as_u64() {
            Some(h) => h,
            None => return vec![],
        };
        let num_rows = result_set["numRows"].as_u64().unwrap_or(0);
        if num_rows == 0 {
            let close = json!({"command":"closeResultSet","resultSetHandles":[handle]});
            self.ws.send(Message::Text(close.to_string().into())).ok();
            let _ = Self::read_json(&mut self.ws);
            return vec![];
        }
        let fetch = json!({
            "command": "fetch",
            "resultSetHandle": handle,
            "startPosition": 0,
            "numBytes": 67108864
        });
        self.ws
            .send(Message::Text(fetch.to_string().into()))
            .expect("send fetch");
        let fetch_resp = Self::read_json(&mut self.ws);
        let close = json!({"command":"closeResultSet","resultSetHandles":[handle]});
        self.ws.send(Message::Text(close.to_string().into())).ok();
        let _ = Self::read_json(&mut self.ws);
        fetch_resp["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|col| col.as_array().cloned().unwrap_or_default())
            .collect()
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
