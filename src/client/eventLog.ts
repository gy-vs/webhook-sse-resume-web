// Pure event-log state for one workspace.
//
// Events are identified by (epoch, seq): a server restart starts a new epoch
// and reuses seq numbers, so seq alone must never be used as an identity.

export type EventRow = {
  epoch: string;
  seq: number;
  method: string;
  path: string;
  body: unknown;
  verification: {valid: boolean; reason: string};
};

export type GapNotice = {
  epoch: string;
  fromEpoch: string | null;
  lastSeen: number | null;
  oldest: number | null;
  reason: 'buffer-overflow' | 'epoch-changed' | 'ahead-of-buffer';
  message: string;
};

export type EventLogState = {
  // Ascending by discovery order (oldest first); UI renders in reverse.
  rows: EventRow[];
  gaps: GapNotice[];
  epochOrder: string[];
};

export type ConnectionState = 'connecting' | 'open' | 'reconnecting';

export function eventKey(event: {epoch: string; seq: number}) {
  return event.epoch + ':' + event.seq;
}

export function gapKey(gap: GapNotice) {
  return [gap.reason, gap.epoch, gap.fromEpoch ?? '-', gap.lastSeen ?? '-'].join('|');
}

export function emptyLog(): EventLogState {
  return {rows: [], gaps: [], epochOrder: []};
}

/**
 * Merge a server event into the log. Duplicates (same epoch+seq, including
 * replayed events delivered again on reconnect) are ignored. Events from a
 * new epoch are appended as a new segment so same-numbered ids from different
 * epochs never merge.
 */
export function applyEvent(state: EventLogState, event: EventRow): EventLogState {
  if (!event || typeof event.epoch !== 'string' || !Number.isFinite(event.seq)) return state;

  if (state.rows.some(row => row.epoch === event.epoch && row.seq === event.seq)) {
    return state;
  }

  const rows = state.rows.slice();
  const epochOrder = state.epochOrder.includes(event.epoch)
    ? state.epochOrder
    : [...state.epochOrder, event.epoch];

  const epochIndex = (epoch: string) => epochOrder.indexOf(epoch);
  let lo = 0;
  let hi = rows.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    const row = rows[mid];
    const rowRank = epochIndex(row.epoch);
    const eventRank = epochIndex(event.epoch);
    if (rowRank < eventRank || (rowRank === eventRank && row.seq < event.seq)) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  rows.splice(lo, 0, event);
  return {...state, rows, epochOrder};
}

export function applySnapshot(state: EventLogState, events: EventRow[]): EventLogState {
  let next = state;
  // Snapshot is oldest-first; insert in order so merge stays cheap and stable.
  for (const event of events) next = applyEvent(next, event);
  return next;
}

/** Record a server-reported gap. Identical gaps (e.g. repeated reconnects) are deduped. */
export function applyGap(state: EventLogState, gap: GapNotice): EventLogState {
  if (!gap || typeof gap.epoch !== 'string') return state;
  const key = gapKey(gap);
  if (state.gaps.some(existing => gapKey(existing) === key)) return state;
  const epochOrder = state.epochOrder.includes(gap.epoch)
    ? state.epochOrder
    : [...state.epochOrder, gap.epoch];
  return {...state, gaps: [...state.gaps, gap], epochOrder};
}

export function dismissGap(state: EventLogState, key: string): EventLogState {
  if (!state.gaps.some(gap => gapKey(gap) === key)) return state;
  return {...state, gaps: state.gaps.filter(gap => gapKey(gap) !== key)};
}

export type RenderGap = {
  boundaryKey: string | null; // divider goes after this row key; null = top of list
  gap: GapNotice;
};

/**
 * Locate where each gap sits relative to the rows the user can already see.
 * Gaps pointing before the first surviving row (or into an unknown epoch that
 * arrived first) render at the top; others render inline between segments.
 */
export function gapsForRender(state: EventLogState): {banners: GapNotice[]; inline: RenderGap[]} {
  const banners: GapNotice[] = [];
  const inline: RenderGap[] = [];

  for (const gap of state.gaps) {
    const newEpochIndex = state.epochOrder.indexOf(gap.epoch);
    // First row belonging to the new epoch.
    const firstNew = state.rows.find(row => row.epoch === gap.epoch);
    if (gap.fromEpoch && gap.fromEpoch !== gap.epoch) {
      const fromIndex = state.epochOrder.indexOf(gap.fromEpoch);
      const oldRows = state.rows.filter(row => row.epoch === gap.fromEpoch);
      // Epoch boundary divider needs both sides on screen, and the epochs must
      // be adjacent in the order we discovered them.
      if (firstNew && oldRows.length > 0 && fromIndex !== -1 && fromIndex + 1 === newEpochIndex) {
        inline.push({boundaryKey: eventKey(oldRows[oldRows.length - 1]), gap});
      } else {
        banners.push(gap);
      }
      continue;
    }
    // Same-epoch overflow: find the last row strictly older than the oldest
    // surviving buffer entry (i.e. the last row before the missing range).
    let boundary: EventRow | null = null;
    for (const row of state.rows) {
      if (row.epoch === gap.epoch && (gap.oldest == null || row.seq < gap.oldest)) boundary = row;
    }
    if (boundary && firstNew && eventKey(boundary) !== eventKey(firstNew)) {
      inline.push({boundaryKey: eventKey(boundary), gap});
    } else {
      banners.push(gap);
    }
  }
  return {banners, inline};
}
