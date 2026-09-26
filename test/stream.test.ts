import {afterEach, describe, expect, it} from 'vitest';
import request from 'supertest';
import http from 'node:http';
import type {AddressInfo} from 'node:net';
import type {Express} from 'express';
import {createApp} from '../src/server/index';

type Frame = {event: string; id: string | null; data: string};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/** Minimal SSE client for tests: collects parsed frames off the wire. */
class SseClient {
  readonly frames: Frame[] = [];
  /** Resolves once response headers arrive, i.e. the server has synchronously
   *  registered this connection as a listener. */
  readonly ready: Promise<void>;
  private waiters: Array<(frames: Frame[]) => boolean> = [];
  private req!: http.ClientRequest;

  constructor(port: number, path: string, headers: Record<string, string> = {}) {
    this.ready = new Promise<void>((resolve, reject) => {
      this.req = http.request({host: '127.0.0.1', port, path, headers}, res => {
        res.setEncoding('utf8');
        let buf = '';
        let event = 'message';
        let id: string | null = null;
        let data: string[] = [];
        res.on('data', (chunk: string) => {
          buf += chunk;
          let idx = buf.indexOf('\n\n');
          while (idx >= 0) {
            const raw = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            for (const line of raw.split('\n')) {
              if (line.startsWith(':')) continue;
              if (line.startsWith('event:')) event = line.slice(6).trim();
              else if (line.startsWith('id:')) id = line.slice(3).trim();
              else if (line.startsWith('data:')) data.push(line.slice(5).trim());
            }
            this.frames.push({event, id, data: data.join('\n')});
            event = 'message';
            id = null;
            data = [];
            this.waiters = this.waiters.filter(w => !w(this.frames));
            idx = buf.indexOf('\n\n');
          }
        });
        resolve();
      });
      this.req.on('error', reject);
      this.req.end();
    });
  }

  waitFor(pred: (frames: Frame[]) => boolean, timeoutMs = 3000): Promise<Frame[]> {
    if (pred(this.frames)) return Promise.resolve(this.frames);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`timed out waiting for frames; got ${JSON.stringify(this.frames)}`)),
        timeoutMs,
      );
      this.waiters.push(frames => {
        if (!pred(frames)) return false;
        clearTimeout(timer);
        resolve(frames);
        return true;
      });
    });
  }

  messages() {
    return this.frames.filter(f => f.event === 'message');
  }

  close() {
    this.req.destroy();
  }
}

const servers: http.Server[] = [];

async function listen(app: Express) {
  const server = http.createServer(app);
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  return (server.address() as AddressInfo).port;
}

afterEach(async () => {
  while (servers.length) {
    const server = servers.pop()!;
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});

async function capture(app: Express, workspace = 'default') {
  const res = await request(app)
    .post(`/api/capture/hook?workspace=${encodeURIComponent(workspace)}`)
    .send({at: Date.now()});
  expect(res.status).toBe(202);
  return res.body as {id: number; epoch: string};
}

describe('sse resume protocol', () => {
  it('replays only events strictly after Last-Event-ID (disconnect boundary)', async () => {
    const app = createApp();
    const port = await listen(app);
    const e1 = await capture(app);
    await capture(app);
    await capture(app);

    const resumed = new SseClient(port, '/api/stream?workspace=default', {
      'last-event-id': `${e1.epoch}:2`,
    });
    await resumed.waitFor(fs => fs.filter(f => f.event === 'message').length >= 1);
    await sleep(100);
    expect(resumed.messages().map(f => f.id)).toEqual([`${e1.epoch}:3`]);
    resumed.close();

    // A cursor that is already at the latest id replays nothing; only future
    // events arrive.
    const caughtUp = new SseClient(port, '/api/stream?workspace=default', {
      'last-event-id': `${e1.epoch}:3`,
    });
    await sleep(100);
    expect(caughtUp.messages()).toHaveLength(0);
    await capture(app);
    await caughtUp.waitFor(fs => fs.some(f => f.event === 'message'));
    await sleep(50);
    expect(caughtUp.messages().map(f => f.id)).toEqual([`${e1.epoch}:4`]);
    caughtUp.close();
  });

  it('repeated reconnects never deliver the same event twice', async () => {
    const app = createApp();
    const port = await listen(app);

    const first = new SseClient(port, '/api/stream?workspace=default');
    await first.ready;
    const e1 = await capture(app);
    await first.waitFor(fs => fs.some(f => f.event === 'message'));
    first.close();

    const e2 = await capture(app);
    const second = new SseClient(port, '/api/stream?workspace=default', {
      'last-event-id': `${e1.epoch}:${e1.id}`,
    });
    await second.waitFor(fs => fs.some(f => f.event === 'message'));
    await sleep(100);
    expect(second.messages().map(f => f.id)).toEqual([`${e2.epoch}:${e2.id}`]);
    second.close();

    const e3 = await capture(app);
    const third = new SseClient(port, '/api/stream?workspace=default', {
      'last-event-id': `${e2.epoch}:${e2.id}`,
    });
    await third.waitFor(fs => fs.some(f => f.event === 'message'));
    await sleep(100);
    expect(third.messages().map(f => f.id)).toEqual([`${e3.epoch}:${e3.id}`]);
    third.close();
  });

  it('sends an explicit gap when the cursor fell out of the bounded replay buffer', async () => {
    const app = createApp({bufferLimit: 3});
    const port = await listen(app);
    let epoch = '';
    for (let i = 0; i < 6; i++) epoch = (await capture(app)).epoch;
    // Buffer now holds ids 4,5,6; ids 2,3 are gone for good.

    const stale = new SseClient(port, '/api/stream?workspace=default', {
      'last-event-id': `${epoch}:1`,
    });
    await stale.waitFor(fs => fs.filter(f => f.event === 'message').length >= 3);
    await sleep(100);
    const [gap, ...rest] = stale.frames;
    expect(gap.event).toBe('gap');
    expect(JSON.parse(gap.data)).toMatchObject({
      kind: 'gap',
      reason: 'overflow',
      after: 1,
      next: 4,
      epoch,
    });
    expect(rest.filter(f => f.event === 'message').map(f => f.id)).toEqual([
      `${epoch}:4`,
      `${epoch}:5`,
      `${epoch}:6`,
    ]);
    stale.close();

    // Cursor 3 is still contiguous with the buffer (next id 4 is buffered):
    // no gap, just the replay.
    const contiguous = new SseClient(port, '/api/stream?workspace=default', {
      'last-event-id': `${epoch}:3`,
    });
    await contiguous.waitFor(fs => fs.filter(f => f.event === 'message').length >= 3);
    await sleep(100);
    expect(contiguous.frames.some(f => f.event === 'gap')).toBe(false);
    expect(contiguous.messages().map(f => f.id)).toEqual([`${epoch}:4`, `${epoch}:5`, `${epoch}:6`]);
    contiguous.close();
  });

  it('fans out to two concurrent tabs on the same workspace', async () => {
    const app = createApp();
    const port = await listen(app);
    const tabA = new SseClient(port, '/api/stream?workspace=default');
    const tabB = new SseClient(port, '/api/stream?workspace=default');
    await Promise.all([tabA.ready, tabB.ready]);

    const e = await capture(app);
    await tabA.waitFor(fs => fs.some(f => f.event === 'message'));
    await tabB.waitFor(fs => fs.some(f => f.event === 'message'));
    expect(tabA.messages().map(f => f.id)).toEqual([`${e.epoch}:${e.id}`]);
    expect(tabB.messages().map(f => f.id)).toEqual([`${e.epoch}:${e.id}`]);
    tabA.close();
    tabB.close();
  });

  it('keeps ids, buffers and streams independent per workspace', async () => {
    const app = createApp();
    const port = await listen(app);
    const a1 = await capture(app, 'alpha');
    const a2 = await capture(app, 'alpha');
    const b1 = await capture(app, 'beta');
    expect([a1.id, a2.id]).toEqual([1, 2]);
    expect(b1.id).toBe(1); // beta runs its own sequence

    const alpha = new SseClient(port, '/api/stream?workspace=alpha', {
      'last-event-id': `${a1.epoch}:1`,
    });
    await alpha.waitFor(fs => fs.some(f => f.event === 'message'));
    await sleep(100);
    expect(alpha.messages().map(f => JSON.parse(f.data).workspace)).toEqual(['alpha']);
    expect(alpha.messages().map(f => JSON.parse(f.data).id)).toEqual([2]);
    alpha.close();

    const beta = await request(app).get('/api/events?workspace=beta');
    expect(beta.body.events.map((e: {id: number}) => e.id)).toEqual([1]);
  });

  it('answers a stale-epoch cursor with a gap instead of silently merging id sequences', async () => {
    const before = createApp();
    const old = await capture(before); // epoch A, id 1

    const restarted = createApp(); // a new process generation
    const port = await listen(restarted);
    const boot = await request(restarted).get('/api/bootstrap');
    expect(boot.body.epoch).not.toBe(old.epoch);

    const client = new SseClient(port, '/api/stream?workspace=default', {
      'last-event-id': `${old.epoch}:${old.id}`,
    });
    await client.ready;
    await client.waitFor(fs => fs.some(f => f.event === 'gap'));
    expect(client.frames[0].event).toBe('gap');
    expect(JSON.parse(client.frames[0].data)).toMatchObject({
      kind: 'gap',
      reason: 'epoch',
      epoch: boot.body.epoch,
    });

    // Ids restart at 1 under the new epoch; the id token keeps them distinct.
    const fresh = await capture(restarted);
    expect(fresh.id).toBe(1);
    await client.waitFor(fs => fs.some(f => f.event === 'message'));
    expect(client.messages()[0].id).toBe(`${boot.body.epoch}:1`);
    expect(client.messages()[0].id).not.toBe(`${old.epoch}:1`);
    client.close();
  });
});
