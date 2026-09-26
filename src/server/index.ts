import express from 'express';
import {randomUUID} from 'node:crypto';
import {fileURLToPath} from 'node:url';

const BUFFER_CAPACITY = 100;
const WORKSPACE_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/;

export type EventRow = {
  epoch: string;
  seq: number;
  method: string;
  path: string;
  body: unknown;
  verification: {valid: boolean; reason: string};
};

type Cursor = {epoch: string; seq: number};

export type GapPayload = {
  kind: 'gap';
  epoch: string;
  fromEpoch: string | null;
  lastSeen: number | null;
  oldest: number | null;
  reason: 'buffer-overflow' | 'epoch-changed' | 'ahead-of-buffer';
  message: string;
};

function verify(_body: unknown, signature = '') {
  return {valid: Boolean(signature), reason: signature ? 'valid' : 'missing'};
}

function sseFrame(payload: unknown, options: {event?: string; id?: string} = {}) {
  const body = JSON.stringify(payload).replace(/\n/g, '\ndata: ');
  // Only event frames carry an id (epoch:seq): the browser echoes it back as
  // Last-Event-ID on automatic reconnect. Gap/ready frames deliberately carry
  // no id so a stale cursor is never advanced past an unrecoverable hole.
  const idLine = options.id !== undefined ? 'id: ' + options.id + '\n' : '';
  const eventLine = options.event ? 'event: ' + options.event + '\n' : '';
  return idLine + eventLine + 'data: ' + body + '\n\n';
}

class WorkspaceLog {
  readonly epoch: string;
  private readonly capacity: number;
  private buffer: EventRow[] = [];
  private nextSeq = 1;
  private highWaterSeq = 0;
  private listeners = new Set<express.Response>();
  constructor(epoch: string, capacity: number) {
    this.epoch = epoch;
    this.capacity = capacity;
  }

  snapshot() {
    return this.buffer.slice();
  }

  get size() {
    return this.buffer.length;
  }

  publish(partial: Omit<EventRow, 'epoch' | 'seq'>): EventRow {
    const event: EventRow = {...partial, epoch: this.epoch, seq: this.nextSeq++};
    this.highWaterSeq = event.seq;
    this.buffer.push(event);
    while (this.buffer.length > this.capacity) this.buffer.shift();
    const frame = this.eventFrame(event);
    for (const listener of this.listeners) listener.write(frame);
    return event;
  }

  /** Event frames carry id epoch:seq so the browser can resume via Last-Event-ID. */
  private eventFrame(event: EventRow) {
    return sseFrame(event, {id: event.epoch + ':' + event.seq});
  }

  /** Register the listener first, then flush replay frames, so nothing can slip in between. */
  attach(res: express.Response, cursor: Cursor | null) {
    this.listeners.add(res);
    res.on('close', () => this.listeners.delete(res));
    res.on('error', () => this.listeners.delete(res));

    let gap: GapPayload | null = null;
    if (!cursor) {
      for (const event of this.buffer) res.write(this.eventFrame(event));
    } else if (cursor.epoch !== this.epoch) {
      gap = {
        kind: 'gap',
        epoch: this.epoch,
        fromEpoch: cursor.epoch,
        lastSeen: cursor.seq,
        oldest: this.buffer[0]?.seq ?? null,
        reason: 'epoch-changed',
        message: `服务端已重启（事件纪元 ${cursor.epoch} → ${this.epoch}），断线期间的事件无法补发。`,
      };
      // Replay everything buffered in the new epoch; same seq numbers under a
      // different epoch are different events and must all be delivered.
      for (const event of this.buffer) res.write(this.eventFrame(event));
    } else if (this.buffer.length === 0) {
      // Nothing is buffered. Without a high-water mark nothing was ever lost;
      // if events past the cursor were published and evicted, that is a gap.
      if (cursor.seq < this.highWaterSeq) {
        gap = {
          kind: 'gap',
          epoch: this.epoch,
          fromEpoch: cursor.epoch,
          lastSeen: cursor.seq,
          oldest: null,
          reason: 'buffer-overflow',
          message: `事件 #${cursor.seq} 之后的记录已超出重放缓冲区，断线期间的事件丢失。`,
        };
      }
    } else {
      const oldest = this.buffer[0].seq;
      const newest = this.buffer[this.buffer.length - 1].seq;
      if (cursor.seq < oldest - 1) {
        gap = {
          kind: 'gap',
          epoch: this.epoch,
          fromEpoch: cursor.epoch,
          lastSeen: cursor.seq,
          oldest,
          reason: 'buffer-overflow',
          message: `事件 #${cursor.seq} 之后、#${oldest} 之前的记录已超出重放缓冲区，无法补发。`,
        };
      } else if (cursor.seq > newest) {
        gap = {
          kind: 'gap',
          epoch: this.epoch,
          fromEpoch: cursor.epoch,
          lastSeen: cursor.seq,
          oldest,
          reason: 'ahead-of-buffer',
          message: `Last-Event-ID #${cursor.seq} 领先于服务端缓冲（最新 #${newest}）。`,
        };
      }
      // Replay only events strictly after the cursor; an overflow gap is still
      // followed by the part of the buffer that survives.
      for (const event of this.buffer) if (event.seq > cursor.seq) res.write(this.eventFrame(event));
    }
    if (gap) res.write(sseFrame(gap, {event: 'gap'}));
    res.write(sseFrame({epoch: this.epoch, capacity: this.capacity, buffered: this.buffer.length}, {event: 'ready'}));
  }
}

export function parseCursor(header: string | undefined): Cursor | null {
  if (!header) return null;
  const match = /^([\w-]+):(\d+)$/.exec(header.trim());
  if (!match) return null;
  const seq = Number(match[2]);
  if (!Number.isSafeInteger(seq) || seq < 0) return null;
  return {epoch: match[1], seq};
}

export function createApp(options: {epoch?: string; bufferCapacity?: number} = {}) {
  const epoch = options.epoch ?? randomUUID();
  const capacity = options.bufferCapacity ?? BUFFER_CAPACITY;
  const workspaces = new Map<string, WorkspaceLog>();

  const getWorkspace = (name: string) => {
    let log = workspaces.get(name);
    if (!log) {
      log = new WorkspaceLog(epoch, capacity);
      workspaces.set(name, log);
    }
    return log;
  };

  const app = express();
  app.use(express.json({limit: '1mb'}));

  app.param('workspace', (req, res, next, name) => {
    if (!WORKSPACE_PATTERN.test(name)) {
      res.status(400).json({error: 'invalid workspace'});
      return;
    }
    req.workspace = name;
    next();
  });

  app.get('/api/bootstrap', (req, res) => {
    const name = String(req.query.workspace ?? 'default');
    if (!WORKSPACE_PATTERN.test(name)) {
      res.status(400).json({error: 'invalid workspace'});
      return;
    }
    const log = getWorkspace(name);
    res.json({kind: 'webhook', count: log.size, epoch, bufferCapacity: capacity});
  });

  app.get('/api/events', (req, res) => {
    const name = String(req.query.workspace ?? 'default');
    if (!WORKSPACE_PATTERN.test(name)) {
      res.status(400).json({error: 'invalid workspace'});
      return;
    }
    res.json({epoch, events: getWorkspace(name).snapshot()});
  });

  app.post('/api/capture/:workspace/*path', (req, res) => {
    const log = getWorkspace(req.workspace!);
    const event = log.publish({
      method: req.method,
      path: req.path.replace(/^\/api\/capture\/[^/]*/, ''),
      body: req.body,
      verification: verify(req.body, String(req.header('x-signature') || '')),
    });
    res.status(202).json({epoch: event.epoch, seq: event.seq, id: event.seq});
  });

  app.get('/api/stream/:workspace', (req, res) => {
    const log = getWorkspace(req.workspace!);
    const cursor = parseCursor(req.header('last-event-id'));
    res.set({
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-epoch': epoch,
    });
    res.flushHeaders?.();
    log.attach(res, cursor);
  });

  app.post('/api/replay', async (req, res) => {
    const ids = Array.isArray(req.body?.ids) ? req.body.ids : [];
    const results = await Promise.all(
      ids.map(async (id: unknown) => ({id, state: 'succeeded'})),
    );
    res.json({results});
  });

  return app;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  createApp().listen(4174, '127.0.0.1', () => console.log('server http://127.0.0.1:4174'));
}

declare global {
  namespace Express {
    interface Request {
      workspace?: string;
    }
  }
}
