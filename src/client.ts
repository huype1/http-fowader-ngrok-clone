// CLI client (the "ngrok" command): connects to the edge server over WS,
// receives forwarded request frames, replays them against the local app on
// 127.0.0.1:<port>, and sends the responses back through the tunnel.
import http from 'node:http';
import { WebSocket } from 'ws';
import {
  DEFAULT_TUNNEL_SERVER_URL,
  TUNNEL_WS_PATH,
  WS_MAX_PAYLOAD_BYTES,
  decodeBody,
  encodeBody,
  isHopByHopHeader,
  type RequestFrame,
  type ResponseFrame,
  type TunnelFrame,
} from './protocol';

// Print CLI usage and exit — bad or missing arguments.
function usage(): never {
  console.log(`usage:
  npm run client -- http <targetPort> [options]

example (ngrok-style):
  npm run client -- http 3000

options:
  --server <url>   edge server ws url (default ${DEFAULT_TUNNEL_SERVER_URL})
                   env: TUNNEL_SERVER_URL

env:
  TUNNEL_TARGET_PORT   fallback for <targetPort>
  TUNNEL_SERVER_URL    fallback for --server
`);
  process.exit(1);
}

// CLI argument parsing: ngrok-style positional `http <port>`, optional
// `--server <url>`, with env fallbacks TUNNEL_TARGET_PORT / TUNNEL_SERVER_URL.
const args = process.argv.slice(2);
let targetPort: number | undefined;
let serverUrl = process.env.TUNNEL_SERVER_URL || DEFAULT_TUNNEL_SERVER_URL;

for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (arg === 'http') {
    targetPort = Number(args[++i]);
  } else if (arg === '--server') {
    const next = args[++i];
    if (!next) usage();
    serverUrl = next;
  } else {
    usage();
  }
}

if (!targetPort) targetPort = Number(process.env.TUNNEL_TARGET_PORT) || undefined;
if (!targetPort || !Number.isInteger(targetPort) || targetPort < 1 || targetPort > 65535) usage();
const TARGET_PORT: number = targetPort;

// Normalize the edge-server URL: accept http(s) schemes, and default the path
// to the tunnel endpoint when the URL has none.
let wsUrl: URL;
try {
  wsUrl = new URL(serverUrl);
} catch {
  console.error(`[tunnel] invalid --server url: ${serverUrl}`);
  process.exit(1);
}
if (wsUrl.protocol === 'http:') wsUrl.protocol = 'ws:';
if (wsUrl.protocol === 'https:') wsUrl.protocol = 'wss:';
if (!wsUrl.pathname || wsUrl.pathname === '/') wsUrl.pathname = TUNNEL_WS_PATH;

// Base URL of the local app every tunneled request is replayed against.
const target = `http://127.0.0.1:${TARGET_PORT}`;

// Serialize a frame as JSON text over the socket (no-op if already closed).
function sendFrame(sock: WebSocket, frame: TunnelFrame): void {
  if (sock.readyState !== WebSocket.OPEN) return;
  sock.send(JSON.stringify(frame));
}

// The heart of the client: take one RequestFrame, replay it against the local
// app (target + original path), buffer the response, re-encode the body, and
// send back a ResponseFrame with the same id. Any failure (bad URL, refused
// connection, broken stream) becomes a 502 response so the browser gets an
// immediate error instead of hanging until the server's 504 timeout.
function handleRequest(sock: WebSocket, frame: RequestFrame): void {
  const body = decodeBody(frame.body, frame.encoding);
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(frame.headers)) {
    if (!isHopByHopHeader(name)) headers[name] = value;
  }
  if (body.length > 0) {
    headers['content-length'] = String(body.length);
  } else {
    delete headers['content-length'];
  }

  let req: http.ClientRequest;
  try {
    req = http.request(target + frame.path, { method: frame.method, headers });
  } catch (err) {
    sendFrame(sock, {
      type: 'response',
      id: frame.id,
      status: 502,
      headers: { 'content-type': 'text/plain' },
      body: `tunnel client could not build request for ${target}${frame.path}: ${(err as Error).message}`,
      encoding: 'utf8',
    });
    return;
  }

  // Send at most one response per request id (a stream error can race 'end').
  let replied = false;
  const reply = (resp: ResponseFrame): void => {
    if (replied) return;
    replied = true;
    sendFrame(sock, resp);
  };

  req.on('response', (res) => {
    const chunks: Buffer[] = [];
    res.on('data', (chunk: Buffer) => chunks.push(chunk));
    res.on('error', () => {
      reply({
        type: 'response',
        id: frame.id,
        status: 502,
        headers: { 'content-type': 'text/plain' },
        body: 'local server response stream failed',
        encoding: 'utf8',
      });
      req.destroy();
    });
    res.on('end', () => {
      reply({
        type: 'response',
        id: frame.id,
        status: res.statusCode ?? 502,
        headers: res.headers as Record<string, string | string[]>,
        ...encodeBody(Buffer.concat(chunks)),
      });
    });
  });

  req.on('error', (err) => {
    reply({
      type: 'response',
      id: frame.id,
      status: 502,
      headers: { 'content-type': 'text/plain' },
      body: `tunnel client failed to reach local server at ${target}: ${err.message}`,
      encoding: 'utf8',
    });
  });

  if (body.length > 0) req.write(body);
  req.end();
}

// Consecutive failed reconnects — drives the backoff, reset on successful open.
let attempts = 0;

// Open the tunnel WebSocket and wire its events. Every close schedules the
// next attempt with exponential backoff (1s → 2s → 4s … capped at 10s), so
// the client survives edge-server restarts and network blips.
function connect(): void {
  const sock = new WebSocket(wsUrl, { maxPayload: WS_MAX_PAYLOAD_BYTES });

  sock.on('open', () => {
    attempts = 0;
    console.log(`[tunnel] connected to ${wsUrl.href}`);
    console.log(
      `[tunnel] exposing ${wsUrl.host} -> ${target}  (open http://${wsUrl.hostname}:${wsUrl.port} in a browser)`,
    );
  });

  sock.on('message', (data, isBinary) => {
    if (isBinary) return;
    let frame: TunnelFrame;
    try {
      frame = JSON.parse(data.toString()) as TunnelFrame;
    } catch {
      return;
    }
    if (frame.type === 'request') handleRequest(sock, frame);
  });

  sock.on('error', (err) => {
    console.error(`[tunnel] ws error: ${err.message}`);
  });

  sock.on('close', () => {
    const delay = Math.min(1_000 * 2 ** attempts, 10_000);
    attempts += 1;
    console.log(`[tunnel] disconnected — retrying in ${delay}ms`);
    setTimeout(connect, delay);
  });
}

console.log(`[tunnel] client forwarding ${target} through ${wsUrl.href}`);
connect();
