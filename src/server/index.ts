import express from 'express';
import {fileURLToPath} from 'node:url';

function verify(_body: unknown, signature = '') { return {valid: Boolean(signature), reason: signature ? 'valid' : 'missing'}; }

type EventRow = {id: number; method: string; path: string; body: unknown; verification: {valid: boolean; reason: string}};
const events: EventRow[] = [];
const listeners = new Set<express.Response>();
let nextId = 1;

function publish(event: EventRow) {
  const frame = 'id: ' + event.id + '\ndata: ' + JSON.stringify(event) + '\n\n';
  for (const listener of listeners) listener.write(frame);
}

export function createApp() {
  const app = express();
  app.use(express.json({limit: '1mb'}));
  app.get('/api/bootstrap', (_req, res) => res.json({kind: 'webhook', count: events.length}));
  app.get('/api/events', (_req, res) => res.json(events));
  app.post('/api/capture/*path', (req, res) => {
    const event = {id: nextId++, method: req.method, path: req.path, body: req.body, verification: verify(req.body, String(req.header('x-signature') || ''))};
    events.push(event);
    if (events.length > 100) events.shift();
    publish(event);
    res.status(202).json({id: event.id});
  });
  app.get('/api/stream', (req, res) => {
    res.set({'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive'});
    listeners.add(res);
    req.on('close', () => listeners.delete(res));
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
