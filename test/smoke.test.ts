import {afterEach, beforeEach, describe, expect, it} from 'vitest';
import http from 'node:http';
import type {AddressInfo} from 'node:net';
import request from 'supertest';
import {createApp} from '../src/server/index';

type ServerHandle = {base: string; close: () => Promise<void>};

function startServer(epoch?: string, bufferCapacity?: number): Promise<ServerHandle> {
  return new Promise((resolve, reject) => {
    const server = createApp({epoch, bufferCapacity}).listen(0, '127.0.0.1');
    server.once('error', reject);
    server.once('listening', () => {
      const {port} = server.address() as AddressInfo;
      resolve({
        base: `http://127.0.0.1:${port}`,
        close: () =>
          new Promise<void>((done, fail) => {
            // SSE sockets stay open on purpose; force them shut on teardown.
            server.closeAllConnections?.();
            server.close(err => (err ? fail(err) : done()));
          }),
      });
    });
  });
}

async function postEvent(base: string, workspace: string, path = '/hook', signature = 'sig') {
  const response = await fetch(`${base}/api/capture/${workspace}${path}`, {
    method: 'POST',
    headers: {'content-type': 'application/json', 'x-signature': signature},
    body: JSON.stringify({hello: Math.random()}),
  });
  return (await response.json()) as {epoch: string; seq: number; id: number};
}

type ParsedFrame = {event?: string; id?: string; data: string};

function parseFrames(buffer: string): ParsedFrame[] {
  return buffer
    .split('\n\n')
    .filter(chunk => chunk.length > 0)
    .map(chunk => {
      const frame: ParsedFrame = {data: ''};
      const dataLines: string[] = [];
      for (const line of chunk.split('\n')) {
        if (line.startsWith('event:')) frame.event = line.slice(6).trim();
        else if (line.startsWith('id:')) frame.id = line.slice(3).trim();
        else if (line.startsWith('data:')) dataLines.push(line.slice(5).replace(/^ /, ''));
      }
      frame.data = dataLines.join('\n');
      return frame;
    });
}

class SseClient {
  private buffer = '';
  frames: ParsedFrame[] = [];
  private waiters: Array<(frames: ParsedFrame[]) => boolean> = [];
  private req: http.ClientRequest;
  responseCode = 0;

  constructor(base: string, workspace: string, lastEventId?: string) {
    const url = new URL(`${base}/api/stream/${workspace}`);
    const headers: Record<string, string> = {Accept: 'text/event-stream'};
    if (lastEventId !== undefined) headers['Last-Event-ID'] = lastEventId;
    this.req = http.request(
      {host: url.hostname, port: url.port, path: url.pathname, headers},
      res => {
      this.responseCode = res.statusCode ?? 0;
      res.setEncoding('utf8');
      res.on('data', (chunk: string) => {
        this.buffer += chunk;
        const complete = this.buffer.split('\n\n');
        this.buffer = complete.pop() ?? '';
        for (const raw of complete) {
          const parsed = parseFrames(raw)[0];
          if (!parsed) continue;
          this.frames.push(parsed);
          // Several frames may share one data chunk, so re-evaluate every
          // pending waiter against the full list after each push. A waiter
          // returns false (and resolves) once its predicate is satisfied and
          // drops itself; unsatisfied waiters return true to stay.
          this.waiters = this.waiters.filter(waiter => waiter(this.frames));
        }
      });
    });
    this.req.on('error', () => {
      /* aborts during teardown */
    });
    this.req.end();
  }

  async waitFor(predicate: (frames: ParsedFrame[]) => boolean, timeoutMs = 2000): Promise<ParsedFrame[]> {
    if (predicate(this.frames)) return this.frames;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out waiting for SSE frame')), timeoutMs);
      // Returns true to keep the waiter registered until the predicate matches;
      // on match it unregisters itself and resolves.
      const waiter = (frames: ParsedFrame[]) => {
        if (!predicate(frames)) return true;
        clearTimeout(timer);
        this.waiters = this.waiters.filter(w => w !== waiter);
        resolve(frames);
        return false;
      };
      this.waiters.push(waiter);
    });
  }

  events(): Array<Record<string, unknown>> {
    return this.frames
      .filter(frame => !frame.event)
      .map(frame => JSON.parse(frame.data));
  }

  gaps(): Array<Record<string, unknown>> {
    return this.frames.filter(frame => frame.event === 'gap').map(frame => JSON.parse(frame.data));
  }

  close() {
    this.req.destroy();
  }
}

function seqs(events: Array<Record<string, unknown>>) {
  return events.map(event => event.seq as number);
}

describe('webhook SSE server', () => {
  let server: ServerHandle;

  beforeEach(async () => {
    server = await startServer('test-epoch', 5);
  });
  afterEach(async () => {
    await server.close();
  });

  it('serves its bootstrap contract', async () => {
    const response = await request(createApp()).get('/api/bootstrap');
    expect(response.status).toBe(200);
    expect(response.body.kind).toBe('webhook');
    expect(response.body.count).toBeTypeOf('number');
  });

  it('assigns a monotonic per-workspace id with an epoch', async () => {
    const first = await postEvent(server.base, 'default');
    const second = await postEvent(server.base, 'default');
    expect(first.epoch).toBe('test-epoch');
    expect(second.seq).toBe(first.seq + 1);

    const events = await (await fetch(server.base + '/api/events?workspace=default')).json();
    expect(seqs(events.events)).toEqual([1, 2]);
    expect(events.epoch).toBe('test-epoch');
  });

  it('replays the whole buffer on first connect and then goes live', async () => {
    await postEvent(server.base, 'default');
    await postEvent(server.base, 'default');
    const client = new SseClient(server.base, 'default');
    await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    expect(seqs(client.events())).toEqual([1, 2]);

    await postEvent(server.base, 'default');
    await client.waitFor(frames => client.events().length >= 3);
    expect(seqs(client.events())).toEqual([1, 2, 3]);
    client.close();
  });

  it('resumes strictly after Last-Event-ID across a disconnect (no dup, no loss)', async () => {
    await postEvent(server.base, 'default'); // 1
    const before = new SseClient(server.base, 'default');
    await before.waitFor(frames => clientEventsCount(frames) >= 1);
    await postEvent(server.base, 'default'); // 2 - seen live
    await before.waitFor(frames => clientEventsCount(frames) >= 2);
    before.close();
    // Events arriving while disconnected.
    await postEvent(server.base, 'default'); // 3
    await postEvent(server.base, 'default'); // 4

    const after = new SseClient(server.base, 'default', 'test-epoch:2');
    await after.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    expect(seqs(after.events())).toEqual([3, 4]);
    expect(after.gaps()).toHaveLength(0);
    // Event frames carry the resumable cursor id; gap/ready frames carry none.
    expect(after.frames.filter(f => !f.event).map(f => f.id)).toEqual(['test-epoch:3', 'test-epoch:4']);
    expect(after.frames.filter(f => f.event === 'ready')[0]?.id).toBeUndefined();
    after.close();
  });

  it('does not resend the last event when the cursor equals the newest id', async () => {
    await postEvent(server.base, 'default');
    const client = new SseClient(server.base, 'default', 'test-epoch:1');
    await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    expect(client.events()).toHaveLength(0);
    expect(client.gaps()).toHaveLength(0);
    client.close();
  });

  it('survives repeated reconnects without duplicating or skipping events', async () => {
    for (let seq = 1; seq <= 3; seq++) await postEvent(server.base, 'default');
    const cursors = ['test-epoch:1', 'test-epoch:2', 'test-epoch:3'];
    for (const cursor of cursors) {
      const client = new SseClient(server.base, 'default', cursor);
      await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
      client.close();
    }
    const client = new SseClient(server.base, 'default', 'test-epoch:1');
    await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    expect(seqs(client.events())).toEqual([2, 3]);
    client.close();
  });

  it('emits an explicit gap when Last-Event-ID is older than the bounded buffer', async () => {
    for (let seq = 1; seq <= 7; seq++) await postEvent(server.base, 'default');
    // Capacity 5: buffer holds 3..7, cursor at 1 means 2 is gone.
    const client = new SseClient(server.base, 'default', 'test-epoch:1');
    await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    const gap = client.gaps()[0];
    expect(gap.reason).toBe('buffer-overflow');
    expect(gap.lastSeen).toBe(1);
    expect(gap.oldest).toBe(3);
    // Surviving tail is still replayed rather than pretending continuity.
    expect(seqs(client.events())).toEqual([3, 4, 5, 6, 7]);
    // Gap frames carry no id: the stale cursor must not be advanced past the hole.
    expect(client.frames.find(frame => frame.event === 'gap')?.id).toBeUndefined();
    // Surviving replayed events still carry their own cursor ids.
    expect(client.frames.filter(f => !f.event).map(f => f.id)).toEqual([
      'test-epoch:3',
      'test-epoch:4',
      'test-epoch:5',
      'test-epoch:6',
      'test-epoch:7',
    ]);
    client.close();
  });

  it('still reports overflow when the whole buffer has rolled past the cursor', async () => {
    for (let seq = 1; seq <= 8; seq++) await postEvent(server.base, 'default');
    // Capacity 5: buffer holds 4..8; events 2,3 after cursor 1 are gone.
    const client = new SseClient(server.base, 'default', 'test-epoch:1');
    await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    expect(client.gaps()[0].reason).toBe('buffer-overflow');
    expect(client.gaps()[0].oldest).toBe(4);
    expect(seqs(client.events())).toEqual([4, 5, 6, 7, 8]);
    client.close();
  });

  it('does not invent a gap for an empty workspace with a fresh cursor', async () => {
    const client = new SseClient(server.base, 'never-used', 'test-epoch:0');
    await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    expect(client.gaps()).toHaveLength(0);
    expect(client.events()).toHaveLength(0);
    client.close();
  });

  it('delivers every live event exactly once to two tabs', async () => {
    const tabA = new SseClient(server.base, 'default');
    const tabB = new SseClient(server.base, 'default');
    await Promise.all([
      tabA.waitFor(frames => frames.some(frame => frame.event === 'ready')),
      tabB.waitFor(frames => frames.some(frame => frame.event === 'ready')),
    ]);
    for (let seq = 1; seq <= 4; seq++) await postEvent(server.base, 'default');
    await Promise.all([
      tabA.waitFor(frames => clientEventsCount(frames) >= 4),
      tabB.waitFor(frames => clientEventsCount(frames) >= 4),
    ]);
    expect(seqs(tabA.events())).toEqual([1, 2, 3, 4]);
    expect(seqs(tabB.events())).toEqual([1, 2, 3, 4]);
    tabA.close();
    tabB.close();
  });

  it('keeps independent monotonic sequences and buffers per workspace', async () => {
    await postEvent(server.base, 'default');
    await postEvent(server.base, 'payments');
    await postEvent(server.base, 'payments');
    const def = new SseClient(server.base, 'default');
    const pay = new SseClient(server.base, 'payments');
    await Promise.all([
      def.waitFor(frames => frames.some(frame => frame.event === 'ready')),
      pay.waitFor(frames => frames.some(frame => frame.event === 'ready')),
    ]);
    expect(seqs(def.events())).toEqual([1]);
    expect(seqs(pay.events())).toEqual([1, 2]);
    def.close();
    pay.close();
  });

  it('rejects invalid workspace names', async () => {
    const response = await request(createApp()).get('/api/stream/bad name!');
    expect(response.status).toBe(400);
  });

  it('ignores a malformed Last-Event-ID and replays the full buffer', async () => {
    await postEvent(server.base, 'default');
    const client = new SseClient(server.base, 'default', 'not-a-cursor');
    await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
    expect(seqs(client.events())).toEqual([1]);
    expect(client.gaps()).toHaveLength(0);
    client.close();
  });

  it('flags a cursor ahead of the buffer instead of silently dropping it', async () => {
    await postEvent(server.base, 'default');
    const client = new SseClient(server.base, 'default', 'test-epoch:99');
    await client.waitFor(frames => frames.some(frame => frame.event === 'gap'));
    expect(client.gaps()[0].reason).toBe('ahead-of-buffer');
    client.close();
  });
});

describe('server restart / epoch change', () => {
  it('sends an epoch-changed gap and replays the new epoch without merging seq numbers', async () => {
    const oldServer = await startServer('epoch-A', 5);
    try {
      await postEvent(oldServer.base, 'default', '/a');
      await postEvent(oldServer.base, 'default', '/b');
      const client = new SseClient(oldServer.base, 'default', 'epoch-A:2');
      await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
      expect(seqs(client.events())).toEqual([]);
      client.close();
    } finally {
      await oldServer.close();
    }

    const newServer = await startServer('epoch-B', 5);
    try {
      await postEvent(newServer.base, 'default', '/after-restart'); // seq 1 again
      const client = new SseClient(newServer.base, 'default', 'epoch-A:2');
      await client.waitFor(frames => frames.some(frame => frame.event === 'ready'));
      const gap = client.gaps()[0];
      expect(gap.reason).toBe('epoch-changed');
      expect(gap.fromEpoch).toBe('epoch-A');
      expect(gap.epoch).toBe('epoch-B');
      const events = client.events();
      expect(events).toHaveLength(1);
      expect(events[0].seq).toBe(1);
      expect(events[0].epoch).toBe('epoch-B');
      expect(client.frames.find(f => !f.event)?.id).toBe('epoch-B:1');
      expect(client.frames.find(f => f.event === 'gap')?.id).toBeUndefined();
      client.close();
    } finally {
      await newServer.close();
    }
  });
});

function clientEventsCount(frames: ParsedFrame[]) {
  // Server sends default events without an "event:" line (SSE spec: default = message).
  return frames.filter(frame => !frame.event).length;
}
