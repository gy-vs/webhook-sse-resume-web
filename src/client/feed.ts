export type EventRow = {
  id: number;
  epoch: string;
  workspace: string;
  method: string;
  path: string;
  body: unknown;
  verification: {valid: boolean; reason: string};
};

export type GapNotice = {
  kind: 'gap';
  workspace: string;
  epoch: string;
  after: number | null;
  next: number;
  reason: 'overflow' | 'epoch';
};

export type FeedState = {
  /** Newest first. */
  events: EventRow[];
  /** Identity is epoch + id: the same numeric id from two epochs is two events. */
  seen: Set<string>;
  /** Epoch of the most recently applied event. */
  epoch: string | null;
  /** Sticky until dismissed; the event list is never cleared by a gap. */
  gap: GapNotice | null;
};

export function eventKey(event: Pick<EventRow, 'epoch' | 'id'>): string {
  return `${event.epoch}:${event.id}`;
}

export function initialFeed(): FeedState {
  return {events: [], seen: new Set(), epoch: null, gap: null};
}

/**
 * Applies one event. Returns [nextState, added]; `added` is false for
 * duplicates (reconnect replays, double subscriptions), which callers use to
 * keep unread counters honest.
 */
export function applyEvent(state: FeedState, event: EventRow): [FeedState, boolean] {
  const key = eventKey(event);
  if (state.seen.has(key)) return [state, false];
  const seen = new Set(state.seen);
  seen.add(key);
  return [{...state, events: [event, ...state.events], seen, epoch: event.epoch}, true];
}

/** Merges a snapshot (oldest first, as served) without duplicating live arrivals. */
export function applySnapshot(state: FeedState, events: EventRow[]): FeedState {
  let next = state;
  for (const event of events) [next] = applyEvent(next, event);
  return next;
}

/**
 * Records a gap. Existing events are kept as-is; an epoch gap only switches
 * the current epoch marker so old-epoch rows stay distinct from new ones.
 */
export function applyGap(state: FeedState, gap: GapNotice): FeedState {
  return {...state, gap, epoch: gap.epoch};
}

export function dismissGap(state: FeedState): FeedState {
  return {...state, gap: null};
}

/** Resume token for the stream request, taken from the newest snapshot row. */
export function snapshotCursor(events: EventRow[]): string | null {
  const last = events[events.length - 1];
  return last ? eventKey(last) : null;
}
