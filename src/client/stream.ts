import type {ConnectionState, EventRow, GapNotice} from './eventLog';

export type StreamHandlers = {
  // live is false for frames replayed during the (re)connection handshake;
  // only genuinely live frames should bump unread counters.
  onEvent: (event: EventRow, live: boolean) => void;
  onGap: (gap: GapNotice) => void;
  onReady: (info: {epoch: string}) => void;
  onStateChange: (state: ConnectionState) => void;
};

/**
 * Open (and own) exactly one EventSource for one workspace.
 *
 * The returned close function always closes the source, so workspace switches
 * and React StrictMode remounts tear down the old connection immediately
 * instead of stacking two sources that would double-deliver every event.
 *
 * EventSource performs its own automatic reconnect (re-sending Last-Event-ID);
 * we never recreate the source on error, which is what prevents duplicate
 * connections after transient network drops.
 */
export function openWorkspaceStream(workspace: string, handlers: StreamHandlers) {
  const source = new EventSource('/api/stream/' + encodeURIComponent(workspace));
  let closed = false;
  // Frames received before the first "ready" frame of this connection are the
  // server's replay buffer, not new live events.
  let primed = false;

  handlers.onStateChange('connecting');
  source.onopen = () => {
    if (!closed) handlers.onStateChange('open');
  };
  source.onmessage = message => {
    if (closed) return;
    try {
      const data = JSON.parse(message.data);
      if (data && typeof data === 'object' && typeof data.epoch === 'string' && typeof data.seq === 'number') {
        handlers.onEvent(data as EventRow, primed);
      }
    } catch {
      // Ignore malformed frames; one bad event must not kill the subscription.
    }
  };
  source.addEventListener('gap', message => {
    if (closed) return;
    try {
      const data = JSON.parse((message as MessageEvent).data);
      if (data && data.kind === 'gap') handlers.onGap(data as GapNotice);
    } catch {
      // ignore malformed gap
    }
  });
  source.addEventListener('ready', message => {
    if (closed) return;
    try {
      const data = JSON.parse((message as MessageEvent).data);
      if (data && typeof data.epoch === 'string') {
        primed = true;
        handlers.onReady(data);
      }
    } catch {
      // ignore malformed ready
    }
  });
  source.onerror = () => {
    if (closed) return;
    // The browser retries automatically (sending Last-Event-ID); signal that
    // state instead of recreating the source ourselves. Until the next "ready"
    // frame, arriving messages are replay frames rather than live events.
    primed = false;
    handlers.onStateChange('reconnecting');
  };

  return function close() {
    closed = true;
    source.onopen = null;
    source.onmessage = null;
    source.onerror = null;
    source.close();
  };
}
