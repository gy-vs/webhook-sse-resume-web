import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {openWorkspaceStream, type StreamHandlers} from '../src/client/stream';

type Listener = (event: {data: string}) => void;

class MockEventSource {
  static instances: MockEventSource[] = [];
  static lastUrl: string | null = null;

  url: string;
  readyState = 0;
  onopen: ((event?: unknown) => void) | null = null;
  onmessage: Listener | null = null;
  onerror: (() => void) | null = null;
  private named = new Map<string, Listener>();
  closed = false;

  constructor(url: string) {
    this.url = url;
    MockEventSource.lastUrl = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(name: string, listener: Listener) {
    this.named.set(name, listener);
  }

  close() {
    this.closed = true;
    this.readyState = 2;
  }

  emitOpen() {
    this.readyState = 1;
    this.onopen?.();
  }

  emitMessage(data: unknown) {
    this.onmessage?.({data: JSON.stringify(data)});
  }

  emitNamed(name: string, data: unknown) {
    this.named.get(name)?.({data: JSON.stringify(data)});
  }

  emitError() {
    this.onerror?.();
  }
}

describe('workspace stream lifecycle', () => {
  const originalEventSource = globalThis.EventSource;

  beforeEach(() => {
    MockEventSource.instances = [];
    MockEventSource.lastUrl = null;
    globalThis.EventSource = MockEventSource as unknown as typeof EventSource;
  });

  afterEach(() => {
    globalThis.EventSource = originalEventSource;
    vi.restoreAllMocks();
  });

  function silentHandlers(): StreamHandlers {
    return {
      onEvent: () => {},
      onGap: () => {},
      onReady: () => {},
      onStateChange: () => {},
    };
  }
  it('opens one source per workspace and closes it on teardown (StrictMode safe)', () => {
    const close = openWorkspaceStream('default', silentHandlers());
    expect(MockEventSource.instances).toHaveLength(1);
    expect(MockEventSource.lastUrl).toBe('/api/stream/default');

    close();
    expect(MockEventSource.instances[0].closed).toBe(true);

    // StrictMode remount: new source, old one stays closed.
    const close2 = openWorkspaceStream('default', silentHandlers());
    expect(MockEventSource.instances).toHaveLength(2);
    expect(MockEventSource.instances[0].closed).toBe(true);
    expect(MockEventSource.instances[1].closed).toBe(false);
    close2();
  });

  it('never leaves two live sources when switching workspaces', () => {
    const closeDefault = openWorkspaceStream('default', silentHandlers());
    closeDefault();
    const closePayments = openWorkspaceStream('payments', silentHandlers());
    expect(MockEventSource.lastUrl).toBe('/api/stream/payments');
    const live = MockEventSource.instances.filter(source => !source.closed);
    expect(live).toHaveLength(1);
    expect(live[0].url).toBe('/api/stream/payments');
    closePayments();
  });

  it('routes message / gap / ready frames, flags live vs replay, and ignores malformed data', () => {
    const events: Array<{seq: number; live: boolean}> = [];
    const gaps: unknown[] = [];
    const ready: unknown[] = [];
    const states: string[] = [];
    const close = openWorkspaceStream('default', {
      onEvent: (event, live) => events.push({seq: event.seq, live}),
      onGap: gap => gaps.push(gap),
      onReady: info => ready.push(info),
      onStateChange: state => states.push(state),
    });
    const source = MockEventSource.instances[0];
    source.emitOpen();
    // Before "ready" this is a replayed frame from the handshake buffer.
    source.emitMessage({epoch: 'A', seq: 1, method: 'POST', path: '/x', body: {}, verification: {valid: true, reason: 'valid'}});
    source.emitNamed('ready', {epoch: 'A', buffered: 1});
    // After "ready" events are genuinely live.
    source.emitMessage({epoch: 'A', seq: 2, method: 'POST', path: '/y', body: {}, verification: {valid: true, reason: 'valid'}});
    source.emitMessage({garbage: true});
    source.emitNamed('gap', {kind: 'gap', epoch: 'A', reason: 'buffer-overflow'});
    source.emitNamed('gap', {kind: 'nope'});

    expect(events).toEqual([
      {seq: 1, live: false},
      {seq: 2, live: true},
    ]);
    expect(gaps).toHaveLength(1);
    expect(ready).toHaveLength(1);
    expect(states).toEqual(['connecting', 'open']);

    // After an error the next connection's replay frames are not live again.
    source.emitError();
    expect(states.at(-1)).toBe('reconnecting');
    source.emitMessage({epoch: 'A', seq: 3});
    expect(events.at(-1)).toEqual({seq: 3, live: false});
    close();
    // Frames after close must be dropped.
    source.emitNamed('ready', {epoch: 'A'});
    source.emitMessage({epoch: 'A', seq: 4});
    expect(events).toHaveLength(3);
  });
});
