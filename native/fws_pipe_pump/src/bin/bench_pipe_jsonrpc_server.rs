use serde_json::{json, Value};
use std::io::{self, BufRead, BufWriter, Write};

fn coerce_i64(value: Option<&Value>, default: i64) -> i64 {
    match value {
        Some(Value::Number(number)) => number.as_i64().unwrap_or(default),
        Some(Value::String(text)) => text.parse::<i64>().unwrap_or(default),
        _ => default,
    }
}

fn payload(size: usize, fill: &str) -> String {
    if size == 0 {
        return String::new();
    }
    let repeat_count = (size / fill.len()) + 1;
    fill.repeat(repeat_count).chars().take(size).collect()
}

fn write_message<W: Write>(writer: &mut W, message: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, message)?;
    writer.write_all(b"\n")
}

fn handle_request<W: Write>(writer: &mut W, request: Value) -> io::Result<()> {
    let request_id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let params = request.get("params").and_then(Value::as_object);

    if method != "bench.echo" {
        write_message(
            writer,
            &json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": format!("unknown method: {method}")},
            }),
        )?;
        writer.flush()?;
        return Ok(());
    }

    let response_bytes =
        coerce_i64(params.and_then(|p| p.get("response_bytes")), 0).max(0) as usize;
    let push_count = coerce_i64(params.and_then(|p| p.get("push_count")), 0).max(0) as usize;
    let push_bytes = coerce_i64(params.and_then(|p| p.get("push_bytes")), 0).max(0) as usize;
    let ordinal = coerce_i64(params.and_then(|p| p.get("ordinal")), 0).max(0);

    for index in 0..push_count {
        write_message(
            writer,
            &json!({
                "jsonrpc": "2.0",
                "method": "bench.push",
                "params": {
                    "request_id": request_id,
                    "ordinal": ordinal,
                    "index": index,
                    "payload": payload(push_bytes, "push_"),
                },
            }),
        )?;
    }

    write_message(
        writer,
        &json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "ok": true,
                "ordinal": ordinal,
                "payload": payload(response_bytes, "resp_"),
            },
        }),
    )?;
    writer.flush()
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut writer = BufWriter::new(io::stdout().lock());

    for line_result in stdin.lock().lines() {
        let line = line_result?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str::<Value>(trimmed) {
            Ok(value) if value.is_object() => value,
            Ok(_) => {
                write_message(
                    &mut writer,
                    &json!({
                        "jsonrpc": "2.0",
                        "id": null,
                        "error": {"code": -32600, "message": "invalid request"},
                    }),
                )?;
                writer.flush()?;
                continue;
            }
            Err(_) => {
                write_message(
                    &mut writer,
                    &json!({
                        "jsonrpc": "2.0",
                        "id": null,
                        "error": {"code": -32700, "message": "parse error"},
                    }),
                )?;
                writer.flush()?;
                continue;
            }
        };
        handle_request(&mut writer, request)?;
    }

    Ok(())
}
