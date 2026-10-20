# http-fowader-ngrok-clone

A minimal ngrok-style HTTP tunnel. An edge server accepts normal HTTP traffic from browsers/phones and forwards every request over a WebSocket to a CLI client running next to your local dev server, then relays the response back.

```
browser/phone ──HTTP──▶ :8080 ─┬─ /__tunnel → WebSocket (CLI client connects here)
                               └─ /*        → forwarded into the tunnel
CLI client ──▶ localhost:<your-app-port>  (client side)
```

## Build
```
npm install
```
## Run
```
npm run dev                      # edge server (watches for changes)
npm run client -- http 3000      # expose the app on localhost:3000
```

`http 3000` is ngrok-style: "forward the edge server's traffic to my local app on port 3000". Equivalent env form:

```
TUNNEL_TARGET_PORT=3000 npm run client
```

The client auto-reconnects with backoff and multiplexes concurrent requests by frame `id`.

## Client protocol contract

Connect a WebSocket to `ws://<host>:8080/__tunnel`. The server keeps a single **active** client; extra connections become standby and are auto-promoted when the active one drops.

### Request frame (server → client)

```json
{
  "type": "request",
  "id": "b1c0…",
  "method": "GET",
  "path": "/some/path?x=1",
  "headers": { "host": "example.com", "accept": "*/*" },
  "body": "…",
  "encoding": "utf8"
}
```

- `body` + `encoding` are **omitted** when the request has no body.
- `encoding`: `"utf8"` → `body` is the raw text; `"base64"` → `body` is base64 of the raw bytes. The server sends `utf8` whenever the raw bytes are valid UTF-8 (text, JSON, form data) and `base64` for binary (images, gzip, uploads).
- Hop-by-hop headers (`connection`, `transfer-encoding`, `keep-alive`, `upgrade`, `te`, `trailer`, `proxy-*`, `content-length`) are stripped before forwarding.

### Response frame (client → server)

```json
{
  "type": "response",
  "id": "b1c0…",
  "status": 200,
  "headers": { "content-type": "text/html; charset=utf-8" },
  "body": "…",
  "encoding": "utf8"
}
```

- Echo the request `id`. Multiple requests may be in flight at once — multiplex by `id`.
- Choose `encoding` the same way: `utf8` for text bodies, `base64` for binary (or just always `base64` if you don't care about size).
- Omit `body` for empty responses. Unknown or late `id`s are ignored.
- Send frames as JSON text (not binary).

### Behavior

- No client connected → HTTP `502`.
- Client disconnects mid-request → `502` for that request.
- No response within `TUNNEL_TIMEOUT_MS` (default `30000`) → `504`.
- Server pings every 20s; silent clients are terminated.
- A plain HTTP request to `/__tunnel` (not a WS upgrade) is forwarded like any other path.

## Env

| Var                  | Default              | Scope   | Meaning                          |
| -------------------- | -------------------- | ------- | -------------------------------- |
| `HOST`               | `0.0.0.0`            | server  | Bind address                     |
| `PORT`               | `8080`               | server  | HTTP + WS port                   |
| `TUNNEL_TIMEOUT_MS`  | `30000`              | server  | Max wait for a client response   |
| `TUNNEL_TARGET_PORT` | — (or CLI arg)       | client  | Local app port to forward to     |
| `TUNNEL_SERVER_URL`  | `ws://localhost:8080`| client  | Edge server ws url (`--server` flag also works) |

##  Get IP Address, so devices on the same network can access your app

### Linux
```
hostname -I | awk '{print $1}'        # quick
ip -4 addr show wlp3s0                # explicit interface
```