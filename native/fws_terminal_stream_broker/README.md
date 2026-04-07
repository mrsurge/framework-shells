# FWS Native Terminal Pipe Broker

This crate provides the first native broker implementation for the `terminal_testing` terminal-stream prototype.

## Purpose

The broker owns all terminal-session hot-path work in one native process:

1. spawn the inner shell behind a PTY
2. read PTY output
3. write PTY input
4. apply resize operations
5. frame the terminal session over stdio

During the prototype stage it is intended to run under an outer FWS `pipe` shell.

## Wire Contract

The broker intentionally preserves the existing asymmetric contract used by the Node broker.

### Stdin

Stdin accepts JSON-RPC-style notifications, one per line.

Supported methods:

- `terminal.connect`
- `terminal.input`
- `terminal.resize`
- `terminal.destroy`
- `terminal.ping`

Example:

```json
{"jsonrpc":"2.0","method":"terminal.input","params":{"data_b64":"aGVsbG8K","flush":"immediate"}}
```

### Stdout

Stdout emits newline-delimited JSON records.

Frame types:

- `ready`
- `data`
- `closed`
- `pong`

Example:

```json
{"type":"ready","ts":1775587156740,"pid":18684,"shell":["sh","-lc","read line; stty size; printf \"<%s>\" \"$line\""],"cwd":"/tmp"}
{"type":"data","seq":1,"ts":1775587156791,"data_b64":"aGVsbG8NCg=="}
{"type":"closed","seq":4,"ts":1775587156830,"exit_code":0,"reason":"exited"}
```

## Launch Inputs

The broker accepts the same env-driven launch shape as the Node prototype.

Environment variables:

- `TERMINAL_STREAM_CWD`
- `TERMINAL_STREAM_COLS`
- `TERMINAL_STREAM_ROWS`
- `TERMINAL_STREAM_SHELL_CMD_JSON`
- `TERM`

Shell command resolution order:

1. `TERMINAL_STREAM_SHELL_CMD_JSON`
2. argv after `--`
3. fallback to `sh -i`

## Notes

- PTY payload bytes are preserved as raw bytes internally.
- `data_b64` is just the transport envelope for those bytes.
- This binary is intended to replace the Node broker first; FWS-side broker resolution and packaged binary distribution come after that validation.
