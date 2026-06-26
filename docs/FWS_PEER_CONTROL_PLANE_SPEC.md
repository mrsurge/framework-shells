# FWS Peer Control Plane Spec

This document describes the control-plane lane that lets multiple
framework-shells-compatible managers interoperate while retaining local process
ownership.

The current interoperable wire lane is Socket.IO. A later Unix-domain-socket
transport should carry the same message semantics and DTOs; it should not invent
a second control protocol.

## Roles

An FWS runtime can take either or both roles:

- `controller`: hosts the dashboard/control plane and routes live operations.
- `peer`: owns live shell state and connects to a controller.

A controller may also own local shells. In that case it first handles operations
locally, then routes to peers only when local live state is unavailable.

## Current Socket.IO Binding

- Socket.IO path: `/fws_ws/socket.io`
- Namespace: `/fws`
- Peer room: `fws:peers`
- Dashboard room: `fws:dashboard`
- Per-shell log room: `shell:<shell_id>`
- Transport used by Python FWS peers today: websocket-only.

## Authentication

Peer clients connect with a Socket.IO auth object:

```json
{
  "role": "peer",
  "api_token": "<HMAC-derived API token>",
  "runtime_id": "<secret-derived runtime id>",
  "pid": "12345"
}
```

The controller validates `api_token` and `runtime_id` against the shared FWS
secret. `pid` is informational and is not currently authoritative.

## Current Event Names

The current lane uses named Socket.IO events with Socket.IO ack return values:

| Direction | Event | Payload |
|---|---|---|
| browser to controller | `fws_request` | FWS UI JSON-RPC request envelope |
| controller to browser | `fws_notification` | FWS UI JSON-RPC notification envelope |
| controller to peer | `fws_peer_subscriptions` | `{"shell_ids":["..."]}` |
| controller to peer | `fws_peer_request` | peer request DTO; ack returns peer response DTO |
| peer to controller | `fws_peer_notification` | FWS UI JSON-RPC notification envelope |

The browser/dashboard lane is JSON-RPC-shaped. The peer request lane is not
generic JSON-RPC today; it is a named Socket.IO event with a typed request DTO
and a typed ack response.

## Current Peer Request DTO

Only shell input is implemented on the peer request lane today:

```json
{
  "method": "fws.shell.input",
  "params": {
    "shell_id": "fs_...",
    "data": "payload",
    "append_newline": true,
    "eof": false,
    "source": "dashboard"
  }
}
```

Success ack:

```json
{
  "ok": true,
  "data": {
    "shell_id": "fs_...",
    "backend": "pipe",
    "accepted": true,
    "bytes_written": 10,
    "newline_appended": true,
    "eof_sent": false
  }
}
```

Error ack:

```json
{
  "ok": false,
  "code": "not_owner",
  "error": "Live input unavailable for shell fs_..."
}
```

Known error codes:

- `invalid_request`
- `method_not_found`
- `not_found`
- `not_owner`
- `peer_error`
- `write_failed`

## Current Peer Notifications

Peer notifications reuse the dashboard JSON-RPC notification envelope and method
names:

- `fws.shell.created`
- `fws.shell.spawned`
- `fws.shell.updated`
- `fws.shell.exited`
- `fws.shell.removed`
- `fws.logs.chunk`
- `fws.logs.io_metadata`
- `fws.logs.reset`
- `fws.error`

The controller forwards lifecycle notifications to the dashboard room and log
notifications to the per-shell room. Peers only send log notifications for
shells present in the latest `fws_peer_subscriptions` payload.

## Ownership And Routing Semantics

Live operations must target the manager that owns the live shell resources.

For stdin write/EOF:

1. Controller tries local live input first.
2. If local live input is unavailable, controller calls each connected peer with
   `fws_peer_request`.
3. The first peer that returns `{"ok": true}` wins.
4. `not_owner` and `not_found` are fallback errors and do not stop fan-out.
5. Other peer errors may be returned to the caller.

This allows adopted/persisted records to be visible in one manager while live
file descriptors remain owned by another manager.

## Required Symmetric Interoperability

Python FWS and Ferrous must both be able to act as:

- a peer connecting to an existing Python FWS controller.
- a controller accepting existing Python FWS peers.
- a peer connecting to a Ferrous controller.
- a controller accepting Ferrous peers.

Socket.IO is the first required transport because TE2 and Python FWS already use
it. A later UDS peer control plane should preserve these DTOs and semantics.

## Current Ferrous Status

Ferrous now implements both sides of the first Socket.IO lane at MVP level.

`FerrousNativeHost` implements the controller role:

- mounts `/fws_ws/socket.io` with namespace `/fws`.
- accepts websocket-only Socket.IO peers authenticated by shared-secret
  `api_token` and `runtime_id`.
- tracks connected peers in `fws:peers`.
- sends `fws_peer_subscriptions` for active log-shell subscriptions.
- receives `fws_peer_notification` and forwards dashboard/log notifications to
  browser rooms.
- handles browser `fws_request` for dashboard open/refresh, log open/close, and
  shell input.
- routes shell input local-first, then through peer `fws_peer_request` ack
  fan-out when local live input is unavailable.

`FerrousNativePeer` implements the peer-client role:

- connects to a Python or Ferrous controller using the same base URL plus
  `/fws_ws/socket.io`.
- authenticates as `role: "peer"` using shared-secret `api_token` and
  `runtime_id`.
- tracks `fws_peer_subscriptions`.
- handles `fws_peer_request` for `fws.shell.input` by calling the local native
  manager write/EOF primitives.
- returns the required Socket.IO ack response DTO.
- exposes explicit `fws_peer_notification` emission.
- automatically relays native lifecycle events and subscribed output chunks.

Ferrous peer log relay honors controller subscription hints. It subscribes to
native manager output broadcasts and does not independently drain direct
pipe/PTY stdout, so protocol readers keep ownership of stdout bytes.

## Near-Future Peer Requests

Additional peer requests should be added to the same typed request/ack model:

- `fws.shell.terminate`
- `fws.shell.resize`
- `fws.app.shutdown`
- `fws.shutdown`
- `fws.logs.open` / live log subscription state if the transport moves beyond
  controller-broadcast subscription hints.

Each request must return either the same success DTO shape:

```json
{"ok": true, "data": {}}
```

or the same error DTO shape:

```json
{"ok": false, "code": "not_owner", "error": "..."}
```

## Implementation Boundary

The peer protocol is a control plane. It is not a replacement for backend shell
I/O semantics.

- Raw logs remain raw files.
- stdin records remain sidecar metadata only.
- Live pipe/PTY file descriptors remain owned by the spawning manager.
- The control plane routes operations to the owner; it does not duplicate live
  process ownership.
