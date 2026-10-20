// Shared contract for everything that travels over the tunnel WebSocket:
// frame types, size/time limits, and body encoding helpers used by both the
// edge server (server.ts) and the CLI client (client.ts).
import { isUtf8 } from 'node:buffer';

// URL path of the WebSocket endpoint CLI clients connect to.
export const TUNNEL_WS_PATH = '/__tunnel';
// Default edge-server address used by the client when --server/env is absent.
export const DEFAULT_TUNNEL_SERVER_URL = 'ws://localhost:8080';
// Default bind address (all interfaces) so phones on the LAN can reach us.
export const DEFAULT_HOST = '0.0.0.0';
// Default HTTP + WebSocket port.
export const DEFAULT_PORT = 8080;
// How long the server waits for a client response before answering 504.
export const REQUEST_TIMEOUT_MS = 30_000;
// How often the server pings clients to detect dead connections.
export const HEARTBEAT_INTERVAL_MS = 20_000;
// Largest request body the edge server accepts from browsers.
export const MAX_BODY_BYTES = 50 * 1024 * 1024;
// Largest WebSocket message (a JSON frame) accepted in either direction.
export const WS_MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

// How a frame's `body` string is encoded: raw UTF-8 text (cheap for text)
// or base64 (for binary that must survive JSON, e.g. images/gzip).
export type BodyEncoding = 'utf8' | 'base64';

// Server → client: one forwarded HTTP request. `id` ties the response back.
export interface RequestFrame {
  type: 'request';
  id: string;
  method: string;
  path: string; // path + query string, e.g. "/hello?x=1"
  headers: Record<string, string>;
  body?: string; // omitted when there is no body
  encoding?: BodyEncoding; // how `body` is encoded
}

// Client → server: the local app's response for the request with the same `id`.
export interface ResponseFrame {
  type: 'response';
  id: string;
  status: number;
  headers: Record<string, string | string[]>; // string[] for e.g. set-cookie
  body?: string; // omitted when empty
  encoding?: BodyEncoding; // how `body` is encoded
}

// Every message on the wire is one of the two frames above (sent as JSON text).
export type TunnelFrame = RequestFrame | ResponseFrame;

// Error carrying the HTTP status the edge server should return (502/504).
export class TunnelError extends Error {
  readonly statusCode: number;

  constructor(message: string, statusCode: number) {
    super(message);
    this.name = 'TunnelError';
    this.statusCode = statusCode;
  }
}

// Headers a proxy must never forward (RFC 7230) plus content-length, which is
// recomputed for the re-encoded body on each hop.
const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'proxy-connection',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
  'content-length',
]);

// True if the named header must be stripped before proxying in either direction.
export function isHopByHopHeader(name: string): boolean {
  return HOP_BY_HOP_HEADERS.has(name.toLowerCase());
}

// Pick the cheapest lossless encoding: raw UTF-8 text when the bytes are valid
// UTF-8 (text/JSON/forms), base64 otherwise (binary). Empty bodies yield no fields.
export function encodeBody(buf: Buffer): { body?: string; encoding?: BodyEncoding } {
  if (buf.length === 0) return {};
  if (isUtf8(buf)) {
    return { body: buf.toString('utf8'), encoding: 'utf8' };
  }
  return { body: buf.toString('base64'), encoding: 'base64' };
}

// Inverse of encodeBody: rebuild the original bytes from a frame's body.
export function decodeBody(body: string | undefined, encoding: BodyEncoding | undefined): Buffer {
  if (!body) return Buffer.alloc(0);
  return encoding === 'base64' ? Buffer.from(body, 'base64') : Buffer.from(body, 'utf8');
}

// Type guard: only trust a response status that is a real HTTP status code.
export function isValidStatus(status: unknown): status is number {
  return (
    typeof status === 'number' && Number.isInteger(status) && status >= 100 && status <= 599
  );
}
