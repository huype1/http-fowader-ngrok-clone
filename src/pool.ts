// Server-side pool of CLI-client WebSocket connections. Tracks one "active"
// client that all tunneled HTTP requests are pushed to, promotes a standby
// when the active one drops, and correlates response frames back to the
// waiting browser requests by frame id.
import { randomUUID } from 'node:crypto';
import { WebSocket } from 'ws';
import { TunnelError, type RequestFrame, type ResponseFrame } from './protocol';

// One in-flight request forwarded to a client: which client took it, how to
// settle the waiting promise, and the timeout that bounds the wait.
interface PendingRequest {
  clientId: string;
  resolve: (frame: ResponseFrame) => void;
  reject: (err: TunnelError) => void;
  timer: NodeJS.Timeout;
}

export class TunnelPool {
  // id -> socket for every connected client (active + standby).
  private clients = new Map<string, WebSocket>();
  // id -> last heartbeat result; false means "missed a ping, kill it".
  private alive = new Map<string, boolean>();
  // frame id -> waiting HTTP request.
  private pending = new Map<string, PendingRequest>();
  // The client all new requests are sent to (null = nobody connected).
  private activeId: string | null = null;
  // Handle of the periodic ping timer (created by startHeartbeat).
  private heartbeatTimer: NodeJS.Timeout | undefined;

  // Number of connected clients (for logs).
  get size(): number {
    return this.clients.size;
  }

  // True when at least one client exists and requests can be attempted.
  get hasActive(): boolean {
    return this.activeId !== null;
  }

  // Register a new client; it becomes active if the pool was empty.
  // Returns the generated client id used by all other methods.
  add(ws: WebSocket): string {
    const id = randomUUID();
    this.clients.set(id, ws);
    this.alive.set(id, true);
    if (this.activeId === null) this.activeId = id;
    return id;
  }

  // Drop a client (socket closed or terminated). If it was the active one,
  // promote the next connected client; fail the in-flight requests that were
  // sent to the dead client with 502 (they can never be answered now).
  remove(id: string): void {
    if (!this.clients.delete(id)) return;
    this.alive.delete(id);
    if (this.activeId === id) {
      this.activeId = this.clients.keys().next().value ?? null;
    }
    for (const [frameId, entry] of this.pending) {
      if (entry.clientId !== id) continue;
      clearTimeout(entry.timer);
      this.pending.delete(frameId);
      entry.reject(new TunnelError('tunnel client disconnected mid-request', 502));
    }
  }

  // Called on 'pong': the client answered its last heartbeat ping.
  markAlive(id: string): void {
    this.alive.set(id, true);
  }

  // Periodically ping every client and terminate those that missed the
  // previous ping (half-open TCP connections). `unref` so the timer never
  // keeps the process alive on its own.
  startHeartbeat(intervalMs: number): void {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      for (const [id, ws] of this.clients) {
        if (this.alive.get(id) === false) {
          ws.terminate();
          continue;
        }
        this.alive.set(id, true);
        ws.ping();
      }
    }, intervalMs);
    this.heartbeatTimer.unref();
  }

  // Full teardown (SIGINT/SIGTERM): stop pings, fail all pending requests
  // with 502, and terminate every client socket.
  stop(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    for (const [, entry] of this.pending) {
      clearTimeout(entry.timer);
      entry.reject(new TunnelError('tunnel server is shutting down', 502));
    }
    this.pending.clear();
    for (const ws of this.clients.values()) ws.terminate();
    this.clients.clear();
    this.alive.clear();
    this.activeId = null;
  }

  // Send a request frame to the active client and resolve with the matching
  // response frame. Rejects with 502 when no client is connected or the send
  // fails, and with 504 when the client doesn't answer within `timeoutMs`.
  forward(frame: RequestFrame, timeoutMs: number): Promise<ResponseFrame> {
    const ws = this.activeId ? this.clients.get(this.activeId) : undefined;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new TunnelError('no tunnel client connected', 502));
    }

    const clientId = this.activeId;
    return new Promise<ResponseFrame>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(frame.id);
        reject(
          new TunnelError(
            `tunnel client did not respond within ${timeoutMs}ms`,
            504,
          ),
        );
      }, timeoutMs);

      this.pending.set(frame.id, { clientId: clientId!, resolve, reject, timer });

      ws.send(JSON.stringify(frame), (err) => {
        if (err) {
          clearTimeout(timer);
          this.pending.delete(frame.id);
          reject(
            new TunnelError(
              `failed to send request through tunnel: ${err.message}`,
              502,
            ),
          );
        }
      });
    });
  }

  // Settle the pending request that matches this response frame's id.
  // Responses for unknown/late ids (already timed out) are ignored.
  handleResponse(frame: ResponseFrame): void {
    const entry = this.pending.get(frame.id);
    if (!entry) return;
    clearTimeout(entry.timer);
    this.pending.delete(frame.id);
    entry.resolve(frame);
  }
}
