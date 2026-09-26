import {useEffect, useMemo, useRef, useState} from 'react';
import {Radio, RotateCcw, AlertTriangle, Plus} from 'lucide-react';
import {
  applyEvent,
  applyGap,
  applySnapshot,
  dismissGap,
  emptyLog,
  eventKey,
  gapKey,
  gapsForRender,
  type ConnectionState,
  type EventLogState,
  type EventRow,
  type GapNotice,
} from './eventLog';
import {openWorkspaceStream} from './stream';

type WorkspaceView = {
  log: EventLogState;
  status: ConnectionState;
  selected: string | null;
  unread: number;
};

const DEFAULT_WORKSPACES = ['default', 'payments'];

function freshView(): WorkspaceView {
  return {log: emptyLog(), status: 'connecting', selected: null, unread: 0};
}

function gapLabel(gap: GapNotice) {
  if (gap.reason === 'epoch-changed') return '服务端重启';
  if (gap.reason === 'ahead-of-buffer') return '游标超前';
  return '重放缓冲区溢出';
}

export default function App() {
  const [workspaces, setWorkspaces] = useState<string[]>(DEFAULT_WORKSPACES);
  const [workspace, setWorkspace] = useState('default');
  const [newWorkspace, setNewWorkspace] = useState('');
  const [views, setViews] = useState<Record<string, WorkspaceView>>(() => ({
    default: freshView(),
    payments: freshView(),
  }));
  const workspaceRef = useRef(workspace);
  workspaceRef.current = workspace;
  const documentVisibleRef = useRef(typeof document === 'undefined' || document.visibilityState !== 'hidden');

  const view = views[workspace] ?? freshView();

  const patchView = (name: string, patch: (current: WorkspaceView) => WorkspaceView) => {
    setViews(prev => {
      const current = prev[name] ?? freshView();
      const next = patch(current);
      if (next === current) return prev;
      return {...prev, [name]: next};
    });
  };

  // Snapshot + exactly one EventSource per active workspace.
  // Re-runs (including StrictMode remounts and workspace switches) close the
  // previous source first, so no connection is ever left dangling.
  useEffect(() => {
    let cancelled = false;
    fetch('/api/events?workspace=' + encodeURIComponent(workspace))
      .then(response => (response.ok ? response.json() : null))
      .then(payload => {
        if (cancelled || !payload) return;
        const events: EventRow[] = Array.isArray(payload.events) ? payload.events : [];
        patchView(workspace, current => ({...current, log: applySnapshot(current.log, events)}));
      })
      .catch(() => {
        /* stream reconnect covers snapshot failures */
      });

    const close = openWorkspaceStream(workspace, {
      onEvent: (event, live) => {
        patchView(workspaceRef.current, current => {
          const before = current.log.rows.length;
          const log = applyEvent(current.log, event);
          if (log === current.log || log.rows.length === before) return current;
          // Only genuinely live events arriving while the tab is hidden are
          // unread; replayed frames (initial load / reconnect) never are.
          const unread = live && !documentVisibleRef.current ? current.unread + 1 : current.unread;
          return {...current, log, unread};
        });
      },
      onGap: gap => {
        patchView(workspaceRef.current, current => ({
          ...current,
          log: applyGap(current.log, gap),
        }));
      },
      onReady: () => {
        /* status already flips to open via source.onopen */
      },
      onStateChange: status => {
        patchView(workspaceRef.current, current =>
          current.status === status ? current : {...current, status},
        );
      },
    });

    return () => {
      cancelled = true;
      close();
    };
  }, [workspace]);

  // Reset unread when the tab becomes visible (per workspace).
  useEffect(() => {
    const onVisibility = () => {
      documentVisibleRef.current = document.visibilityState !== 'hidden';
      if (document.visibilityState !== 'hidden') {
        setViews(prev => {
          const current = prev[workspaceRef.current];
          if (!current || current.unread === 0) return prev;
          return {...prev, [workspaceRef.current]: {...current, unread: 0}};
        });
      }
    };
    document.addEventListener('visibilitychange', onVisibility);
    return () => document.removeEventListener('visibilitychange', onVisibility);
  }, []);

  useEffect(() => {
    const total = Object.values(views).reduce((sum, item) => sum + item.unread, 0);
    document.title = total > 0 ? `(${total}) Webhook Lab` : 'Webhook Lab';
  }, [views]);

  function switchWorkspace(next: string) {
    if (next === workspace) return;
    setWorkspace(next);
    setViews(prev => {
      const current = prev[next];
      if (!current || current.unread === 0) return prev;
      return {...prev, [next]: {...current, unread: 0}};
    });
  }

  function addWorkspace() {
    const name = newWorkspace.trim().toLowerCase();
    if (!name || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/.test(name) || workspaces.includes(name)) {
      setNewWorkspace('');
      return;
    }
    setWorkspaces(prev => [...prev, name]);
    setViews(prev => (prev[name] ? prev : {...prev, [name]: freshView()}));
    setNewWorkspace('');
    switchWorkspace(name);
  }

  const rowsDescending = useMemo(() => view.log.rows.slice().reverse(), [view.log.rows]);
  const {banners, inline} = useMemo(() => gapsForRender(view.log), [view.log]);
  const inlineByBoundary = useMemo(() => {
    const map = new Map<string | null, GapNotice[]>();
    for (const item of inline) {
      const list = map.get(item.boundaryKey) ?? [];
      list.push(item.gap);
      map.set(item.boundaryKey, list);
    }
    return map;
  }, [inline]);

  const active: EventRow | undefined =
    view.log.rows.find(event => eventKey(event) === view.selected) ?? view.log.rows[view.log.rows.length - 1];

  async function replay() {
    if (!active) return;
    await fetch('/api/replay', {
      method: 'POST',
      headers: {'content-type': 'application/json'},
      body: JSON.stringify({ids: [active.seq]}),
    });
  }

  const statusText =
    view.status === 'open' ? '已连接' : view.status === 'reconnecting' ? '断线重连中…' : '连接中…';

  return (
    <main className="shell">
      <header className="topbar">
        <Radio size={20} />
        <span className="brand">Webhook Lab</span>
        <small>实时请求工作区</small>
        <nav className="workspaces">
          {workspaces.map(name => (
            <button
              key={name}
              className={'ws-tab' + (name === workspace ? ' active' : '')}
              onClick={() => switchWorkspace(name)}
            >
              {name}
              {views[name]?.unread ? <span className="unread">{views[name].unread}</span> : null}
            </button>
          ))}
          <span className="ws-add">
            <input
              value={newWorkspace}
              onChange={event => setNewWorkspace(event.target.value)}
              onKeyDown={event => {
                if (event.key === 'Enter') addWorkspace();
              }}
              placeholder="新工作区"
              aria-label="新工作区名称"
            />
            <button className="ws-add-btn" onClick={addWorkspace} title="加入工作区">
              <Plus size={14} />
            </button>
          </span>
        </nav>
        <span className={'conn conn-' + view.status}>{statusText}</span>
      </header>
      <section className="workspace">
        <aside className="pane">
          <h2>
            事件
            <small className="epoch">epoch: {active?.epoch.slice(0, 8) ?? '—'}</small>
          </h2>
          {banners.map(gap => (
            <div className="gap-banner" key={'banner-' + gapKey(gap)}>
              <AlertTriangle size={15} />
              <div>
                <strong>{gapLabel(gap)}</strong>
                <span>{gap.message}</span>
              </div>
              <button
                className="gap-close"
                onClick={() => patchView(workspace, current => ({...current, log: dismissGap(current.log, gapKey(gap))}))}
              >
                ×
              </button>
            </div>
          ))}
          <div className="list">
            {rowsDescending.flatMap(event => [
              <button
                className={active && eventKey(active) === eventKey(event) ? 'active' : ''}
                onClick={() => patchView(workspace, current => ({...current, selected: eventKey(event)}))}
                key={'event-' + eventKey(event)}
              >
                {event.method} {event.path}
                <br />
                <small>
                  event {event.seq} · {event.epoch.slice(0, 8)}
                </small>
              </button>,
              ...(inlineByBoundary.get(eventKey(event)) ?? []).map(gap => (
                <div className="gap-divider" key={'divider-' + gapKey(gap)} title={gap.message}>
                  <AlertTriangle size={13} />
                  <span>
                    缺口：{gapLabel(gap)}（{gap.oldest == null ? '?' : '#' + gap.oldest} 起可续传）
                  </span>
                </div>
              )),
            ])}
            {view.log.rows.length === 0 && banners.length === 0 && (
              <p className="empty">等待 {workspace} 工作区的 webhook…</p>
            )}
          </div>
        </aside>
        <section className="pane">
          <div className="toolbar">
            <button className="primary" onClick={replay} disabled={!active}>
              <RotateCcw size={15} /> 重放
            </button>
          </div>
          <h2>Payload</h2>
          <pre>{JSON.stringify(active?.body ?? {}, null, 2)}</pre>
        </section>
        <section className="pane">
          <h2>校验</h2>
          <span className="pill">{active?.verification.reason ?? 'waiting'}</span>
          <pre>{JSON.stringify(active ?? null, null, 2)}</pre>
        </section>
      </section>
    </main>
  );
}
