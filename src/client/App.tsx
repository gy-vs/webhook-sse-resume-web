import {useEffect, useRef, useState} from 'react';
import {AlertTriangle, Radio, RotateCcw, X} from 'lucide-react';
import {
  applyEvent,
  applyGap,
  applySnapshot,
  dismissGap,
  eventKey,
  initialFeed,
  snapshotCursor,
  type EventRow,
  type FeedState,
  type GapNotice,
} from './feed';

type StreamStatus = 'connecting' | 'open' | 'reconnecting';

export default function App() {
  const [workspaces, setWorkspaces] = useState<string[]>(['default']);
  const [workspace, setWorkspace] = useState('default');
  const [unread, setUnread] = useState(0);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/bootstrap')
      .then(r => r.json())
      .then(boot => {
        if (cancelled || !Array.isArray(boot.workspaces)) return;
        setWorkspaces(list => [...new Set([...list, ...boot.workspaces])]);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  // "Unread" counts events that arrived while the tab was hidden; it resets
  // when the user comes back. Dedup in the feed keeps this from inflating.
  useEffect(() => {
    const reset = () => {
      if (!document.hidden) setUnread(0);
    };
    document.addEventListener('visibilitychange', reset);
    return () => document.removeEventListener('visibilitychange', reset);
  }, []);

  useEffect(() => {
    document.title = unread > 0 ? `(${unread}) Webhook Lab` : 'Webhook Lab';
  }, [unread]);

  function addWorkspace() {
    const name = window.prompt('Workspace name?')?.trim();
    if (!name) return;
    setWorkspaces(list => (list.includes(name) ? list : [...list, name]));
    setWorkspace(name);
  }

  return (
    <main className="shell">
      <header className="topbar">
        <Radio size={20} />
        <span className="brand">Webhook Lab</span>
        <small>Live request workspace</small>
        {unread > 0 && <span className="badge">{unread} 未读</span>}
      </header>
      <nav className="tabs">
        {workspaces.map(name => (
          <button
            key={name}
            className={name === workspace ? 'tab active' : 'tab'}
            onClick={() => setWorkspace(name)}
          >
            {name}
          </button>
        ))}
        <button className="tab" onClick={addWorkspace} title="Follow another workspace">
          +
        </button>
      </nav>
      {/* key= remounts on workspace switch, so the old feed state and its
          EventSource are torn down by the effect cleanup below. */}
      <WorkspaceView
        key={workspace}
        workspace={workspace}
        onHiddenEvent={() => setUnread(n => n + 1)}
      />
    </main>
  );
}

function WorkspaceView({workspace, onHiddenEvent}: {workspace: string; onHiddenEvent: () => void}) {
  const [feed, setFeed] = useState<FeedState>(initialFeed);
  const [status, setStatus] = useState<StreamStatus>('connecting');
  const [selected, setSelected] = useState<string | null>(null);
  // Component-level dedup set: shared across StrictMode's double effect run
  // so a briefly duplicated subscription still cannot double-count.
  const seenRef = useRef(new Set<string>());

  useEffect(() => {
    let disposed = false;
    let source: EventSource | undefined;
    const controller = new AbortController();

    function pushEvent(event: EventRow) {
      const key = eventKey(event);
      if (seenRef.current.has(key)) return;
      seenRef.current.add(key);
      setFeed(s => applyEvent(s, event)[0]);
      if (document.hidden) onHiddenEvent();
    }

    async function loadSnapshot(signal?: AbortSignal): Promise<string | null> {
      const res = await fetch(`/api/events?workspace=${encodeURIComponent(workspace)}`, {signal});
      const snap = await res.json();
      if (disposed) return null;
      for (const event of snap.events as EventRow[]) seenRef.current.add(eventKey(event));
      setFeed(s => applySnapshot(s, snap.events));
      return snapshotCursor(snap.events);
    }

    (async () => {
      let cursor: string | null = null;
      try {
        cursor = await loadSnapshot(controller.signal);
      } catch {
        // Offline at mount: the stream below still retries on its own.
      }
      if (disposed) return;
      const params = new URLSearchParams({workspace});
      if (cursor) params.set('lastEventId', cursor);
      source = new EventSource(`/api/stream?${params}`);
      source.onopen = () => setStatus('open');
      // The browser reconnects automatically and replays Last-Event-ID.
      source.onerror = () => setStatus('reconnecting');
      source.onmessage = msg => pushEvent(JSON.parse(msg.data));
      source.addEventListener('gap', msg => {
        const gap: GapNotice = JSON.parse((msg as MessageEvent).data);
        setFeed(s => applyGap(s, gap));
        if (gap.reason === 'epoch') {
          // Server restarted: its new buffer was never streamed to us, so
          // pull it once. Old-epoch rows stay untouched in the list.
          void loadSnapshot().catch(() => {});
        }
      });
    })();

    return () => {
      disposed = true;
      controller.abort();
      source?.close();
    };
  }, [workspace]);

  const active = feed.events.find(event => eventKey(event) === selected) ?? feed.events[0];

  async function replay() {
    if (!active) return;
    await fetch('/api/replay', {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({ids: [active.id]}),
    });
  }

  const epochs = new Set(feed.events.map(event => event.epoch));

  return (
    <section className="workspace">
      <aside className="pane">
        <div className="toolbar">
          <h2 style={{margin: 0}}>Events</h2>
          <span className={`status-pill ${status}`}>
            {status === 'open' ? '已连接' : status === 'reconnecting' ? '重连中…' : '连接中…'}
          </span>
        </div>
        {feed.gap && (
          <div className="gap-banner" role="alert">
            <AlertTriangle size={14} />
            <span>
              {feed.gap.reason === 'epoch'
                ? '服务端已重启：下方旧事件已保留，新事件属于新的 epoch，重启期间的事件可能缺失。'
                : `连接缺口：id ${
                    feed.gap.after === null ? '?' : feed.gap.after + 1
                  }–${feed.gap.next - 1} 的事件已丢失（重放缓冲区溢出），已显示的事件保留不变。`}
            </span>
            <button aria-label="dismiss" onClick={() => setFeed(s => dismissGap(s))}>
              <X size={14} />
            </button>
          </div>
        )}
        <div className="list">
          {feed.events.map(event => (
            <button
              className={active && eventKey(event) === eventKey(active) ? 'active' : ''}
              onClick={() => setSelected(eventKey(event))}
              key={eventKey(event)}
            >
              {event.method} {event.path}
              <br />
              <small>
                event {event.id}
                {epochs.size > 1 && <em className="epoch-tag"> epoch {event.epoch.slice(0, 8)}</em>}
              </small>
            </button>
          ))}
        </div>
      </aside>
      <section className="pane">
        <div className="toolbar">
          <button className="primary" onClick={replay}>
            <RotateCcw size={15} /> Replay
          </button>
        </div>
        <h2>Payload</h2>
        <pre>{JSON.stringify(active?.body ?? {}, null, 2)}</pre>
      </section>
      <section className="pane">
        <h2>Verification</h2>
        <span className="pill">{active?.verification.reason ?? 'waiting'}</span>
        <pre>{JSON.stringify(active, null, 2)}</pre>
      </section>
    </section>
  );
}
