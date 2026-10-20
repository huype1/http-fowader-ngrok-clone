// Edge server: accepts normal HTTP traffic from browsers/phones, pushes each
// request through the tunnel WebSocket to the CLI client, and relays the
// client's response back. Express owns HTTP; ws owns the tunnel endpoint.
import { createServer, type IncomingHttpHeaders } from 'node:http';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { WebSocketServer } from 'ws';
import { TunnelPool } from './pool';
import {
  DEFAULT_HOST,
  DEFAULT_PORT,
  HEARTBEAT_INTERVAL_MS,
  MAX_BODY_BYTES,
  REQUEST_TIMEOUT_MS,
  TUNNEL_WS_PATH,
  WS_MAX_PAYLOAD_BYTES,
  decodeBody,
  encodeBody,
  isHopByHopHeader,
  isValidStatus,
  TunnelError,
  type RequestFrame,
  type ResponseFrame,
  type TunnelFrame,
} from './protocol';

// Runtime configuration, overridable via environment variables.
const HOST = process.env.HOST || DEFAULT_HOST;
const PORT = Number(process.env.PORT) || DEFAULT_PORT;
const TIMEOUT_MS = Number(process.env.TUNNEL_TIMEOUT_MS) || REQUEST_TIMEOUT_MS;

const app = express();
app.disable('x-powered-by'); // don't advertise the framework
// Capture every request body as a raw Buffer (any content type) so it can be
// encoded into a frame; parsing (JSON etc.) is left to the local app.
app.use(express.raw({ type: '*/*', limit: MAX_BODY_BYTES }));

const pool = new TunnelPool();

// Flatten Node's headers (values can be string | string[] | undefined) into a
// plain JSON map for the frame, stripping hop-by-hop headers that must not be
// proxied (connection, transfer-encoding, content-length, ...).
function flattenRequestHeaders(headers: IncomingHttpHeaders): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined || isHopByHopHeader(name)) continue;
    out[name] = Array.isArray(value) ? value.join(', ') : value;
  }
  return out;
}

// Catch-all middleware — this is the tunnel itself: serialize the incoming
// HTTP request into a RequestFrame, await the client's ResponseFrame, then
// write status/headers/body back to the browser. TunnelError carries the
// right status (502 no client/disconnect, 504 timeout); anything else is 502.
app.use(async (req, res) => {
  const raw = req.body;
  const body = Buffer.isBuffer(raw) ? raw : Buffer.alloc(0);
  const encoded = encodeBody(body);

  const frame: RequestFrame = {
    type: 'request',
    id: randomUUID(),
    method: req.method,
    path: req.originalUrl,
    headers: flattenRequestHeaders(req.headers),
    ...encoded,
  };

  try {
    const resp: ResponseFrame = await pool.forward(frame, TIMEOUT_MS);
    const status = isValidStatus(resp.status) ? resp.status : 502;
    for (const [name, value] of Object.entries(resp.headers)) {
      if (isHopByHopHeader(name) || value === undefined) continue;
      res.setHeader(name, value);
    }
    res.status(status).send(decodeBody(resp.body, resp.encoding));
  } catch (err) {
    const status = err instanceof TunnelError ? err.statusCode : 502;
    const message = err instanceof Error ? err.message : 'tunnel error';
    res.status(status).type('text/plain').send(message);
  }
});

const server = createServer(app);

// WebSocket endpoint for CLI clients on the same port as HTTP: only requests
// to TUNNEL_WS_PATH that ask for a WS upgrade land here, everything else
// stays with Express.
const wss = new WebSocketServer({
  server,
  path: TUNNEL_WS_PATH,
  maxPayload: WS_MAX_PAYLOAD_BYTES,
});

// A CLI client connected: register it with the pool and wire its events —
// pong → heartbeat bookkeeping, message → response frames handed to the pool,
// close → pool cleanup (which also fails that client's in-flight requests).
wss.on('connection', (ws) => {
  const clientId = pool.add(ws);
  console.log(`[tunnel] client ${clientId} connected (pool size: ${pool.size})`);

  ws.on('pong', () => pool.markAlive(clientId));

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let frame: TunnelFrame;
    try {
      frame = JSON.parse(data.toString()) as TunnelFrame;
    } catch {
      return;
    }
    if (frame.type === 'response') {
      pool.handleResponse(frame);
    }
  });

  ws.on('error', (err) => {
    console.error(`[tunnel] client ${clientId} error: ${err.message}`);
  });

  ws.on('close', () => {
    pool.remove(clientId);
    console.log(`[tunnel] client ${clientId} disconnected (pool size: ${pool.size})`);
  });
});

// Print listen errors readably instead of crashing with a stack trace —
// most importantly EADDRINUSE when a previous server is still running.
server.on('error', (err) => {
  if ((err as NodeJS.ErrnoException).code === 'EADDRINUSE') {
    console.error(
      `[tunnel] port ${PORT} is already in use — stop the other process (kill $(ss -ltnp 'sport = :${PORT}' | grep -oP 'pid=\\K[0-9]+')) or run with PORT=<other>.`,
    );
  } else {
    console.error(`[tunnel] server error: ${err.message}`);
  }
  process.exit(1);
});

server.listen(PORT, HOST, () => {
  pool.startHeartbeat(HEARTBEAT_INTERVAL_MS);
  console.log(`[tunnel] http edge server listening on http://${HOST}:${PORT}`);
  console.log(`[tunnel] ws endpoint for CLI clients: ws://<host>:${PORT}${TUNNEL_WS_PATH}`);
});

// Graceful shutdown on Ctrl+C / SIGTERM: stop heartbeats, fail pending
// requests, close WS clients, then stop the HTTP listener — bounded by a
// 2s force-exit timer in case sockets refuse to die.
function shutdown(): void {
  console.log('[tunnel] shutting down');
  pool.stop();
  wss.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2_000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
