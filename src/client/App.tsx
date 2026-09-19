import {useEffect, useState} from 'react';
import {Radio, RotateCcw} from 'lucide-react';

type EventRow = {id: number; method: string; path: string; body: unknown; verification: {valid: boolean; reason: string}};

export default function App() {
  const [events, setEvents] = useState<EventRow[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  useEffect(() => { fetch('/api/events').then(r => r.json()).then(setEvents); }, []);
  useEffect(() => {
    const source = new EventSource('/api/stream');
    source.onmessage = event => setEvents(rows => [JSON.parse(event.data), ...rows]);
  }, []);
  const active = events.find(event => event.id === selected) ?? events[0];
  async function replay() {
    if (!active) return;
    await fetch('/api/replay', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({ids: [active.id]})});
  }
  return <main className="shell">
    <header className="topbar"><Radio size={20}/><span className="brand">Webhook Lab</span><small>Live request workspace</small></header>
    <section className="workspace">
      <aside className="pane"><h2>Events</h2><div className="list">{events.map(event => <button className={event.id === active?.id ? 'active' : ''} onClick={() => setSelected(event.id)} key={event.id}>{event.method} {event.path}<br/><small>event {event.id}</small></button>)}</div></aside>
      <section className="pane"><div className="toolbar"><button className="primary" onClick={replay}><RotateCcw size={15}/> Replay</button></div><h2>Payload</h2><pre>{JSON.stringify(active?.body ?? {}, null, 2)}</pre></section>
      <section className="pane"><h2>Verification</h2><span className="pill">{active?.verification.reason ?? 'waiting'}</span><pre>{JSON.stringify(active, null, 2)}</pre></section>
    </section>
  </main>;
}
