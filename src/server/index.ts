import express from 'express';
import {randomUUID} from 'node:crypto';
import {fileURLToPath} from 'node:url';

function verify(_body: unknown, signature = '') {
  return {valid: Boolean(signature), reason: signature ? 'valid' : 'missing'};
}

export type EventRow = {
  id: number;
  epoch: string;
  workspace: string;
  method: string;
  path: string;
  body: unknown;
  verification: {valid: boolean; reason: string};
};

export type GapReason = 'overflow' | 'epoch';

export type GapNotice = {
  kind: 'gap';
  workspace: string;
  epoch: string;
  after: number | null;
  next: number;
  reason: GapReason;
};

type Cursor = {epoch: string; id: number};

type WorkspaceState = {
  name: string;
  nextId: number;
  buffer: EventRow[];
  listeners: Set<express.Response>;
};

export type AppOptions = {
  bufferLimit?: number;
  heartbeatMs?: number;
};

const DEFAULT_WORKSPACE = 'default';

export function createApp(options: AppOptions = {}) {
  const bufferLimit = options.bufferLimit ?? 100;
  const heartbeatMs = options.heartbeatMs ?? 15_000;
  // A fresh epoch per process/app so events from before a restart can never
  // be mistaken for the current id sequence.
  const epoch = randomUUID();
  const workspaces = new Map<string, WorkspaceState>();

  function workspace(name: string): WorkspaceState {
    let ws = workspaces.get(name);
    if (!ws) {
      ws = {name, nextId: 1, buffer: [], listeners: new Set()};
      workspaces.set(name, ws);
    }
    return ws;
  }

  function writeEvent(res: express.Response, event: EventRow) {
    // The SSE id carries the epoch so a native EventSource reconnect hands it
    // straight back via Last-Event-ID.
    res.write(`id: ${event.epoch}:${event.id}\ndata: ${JSON.stringify(event)}\n\n`);
  }

  function writeGap(res: express.Response, gap: GapNotice) {
    // Deliberately no `id:` line: a gap must not advance the client's cursor,
    // so a subsequent reconnect re-evaluates the same resume position.
    res.write(`event: gap\ndata: ${JSON.stringify(gap)}\n\n`);
  }

  function publish(ws: WorkspaceState, event: EventRow) {
    for (const listener of ws.listeners) writeEvent(listener, event);
  }

  // Accepts the "<epoch>:<id>" token we emit ourselves; a bare number (e.g. a
  // hand-crafted request) is interpreted as belonging to the current epoch.
  function parseCursor(raw: unknown): Cursor | null {
    if (typeof raw !== 'string' || !raw) return null;
    const value = raw.trim();
    const token = /^([^:]*):(\d+)$/.exec(value);
    if (token) return {epoch: token[1], id: Number(token[2])};
    if (/^\d+$/.test(value)) return {epoch, id: Number(value)};
    return null;
  }

  const app = express();
  app.use(express.json({limit: '1mb'}));

  app.get('/api/bootstrap', (_req, res) => {
    const count = [...workspaces.values()].reduce((n, ws) => n + ws.buffer.length, 0);
    res.json({kind: 'webhook', epoch, count, workspaces: [...workspaces.keys()]});
  });

  app.get('/api/events', (req, res) => {
    const name = typeof req.query.workspace === 'string' && req.query.workspace ? req.query.workspace : DEFAULT_WORKSPACE;
    const ws = workspace(name);
    res.json({epoch, workspace: ws.name, events: ws.buffer});
  });

  app.post('/api/capture/*path', (req, res) => {
    const name =
      (typeof req.query.workspace === 'string' && req.query.workspace) ||
      String(req.header('x-workspace') || '') ||
      DEFAULT_WORKSPACE;
    const ws = workspace(name);
    const event: EventRow = {
      id: ws.nextId++,
      epoch,
      workspace: ws.name,
      method: req.method,
      path: req.path,
      body: req.body,
      verification: verify(req.body, String(req.header('x-signature') || '')),
    };
    ws.buffer.push(event);
    if (ws.buffer.length > bufferLimit) ws.buffer.shift();
    publish(ws, event);
    res.status(202).json({id: event.id, epoch});
  });

  app.get('/api/stream', (req, res) => {
    const name = typeof req.query.workspace === 'string' && req.query.workspace ? req.query.workspace : DEFAULT_WORKSPACE;
    const ws = workspace(name);

    res.set({
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    res.flushHeaders();

    // Native EventSource reconnects send Last-Event-ID; the first connect
    // after a snapshot passes ?lastEventId=<epoch>:<id> instead.
    const cursor = parseCursor(req.header('last-event-id') ?? req.query.lastEventId);

    if (cursor) {
      if (cursor.epoch !== epoch) {
        // The client's last event comes from a previous server generation.
        // Its ids cannot be compared with ours, so say so explicitly.
        writeGap(res, {
          kind: 'gap',
          workspace: ws.name,
          epoch,
          after: null,
          next: ws.buffer[0]?.id ?? ws.nextId,
          reason: 'epoch',
        });
      } else {
        const firstBuffered = ws.buffer[0]?.id;
        if (firstBuffered !== undefined && cursor.id < firstBuffered - 1) {
          // Some ids strictly after the cursor have already been evicted:
          // continuity is broken, and we say so instead of replaying as if
          // nothing was missed.
          writeGap(res, {
            kind: 'gap',
            workspace: ws.name,
            epoch,
            after: cursor.id,
            next: firstBuffered,
            reason: 'overflow',
          });
        }
        for (const event of ws.buffer) {
          if (event.id > cursor.id) writeEvent(res, event);
        }
      }
    }

    // Register before publishing can interleave: this whole handler is
    // synchronous for subscribers, so an event captured after this point is
    // delivered live rather than slipping between replay and subscription.
    ws.listeners.add(res);

    const heartbeat = setInterval(() => {
      res.write(': hb\n\n');
    }, heartbeatMs);
    heartbeat.unref();

    res.on('close', () => {
      clearInterval(heartbeat);
      ws.listeners.delete(res);
    });
  });

  app.post('/api/replay', async (req, res) => {
    const ids = Array.isArray(req.body.ids) ? req.body.ids : [];
    const results = await Promise.all(ids.map(async (id: number) => ({id, state: 'succeeded'})));
    res.json({results});
  });

  return app;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  createApp().listen(4174, '127.0.0.1', () => console.log('server http://127.0.0.1:4174'));
}
