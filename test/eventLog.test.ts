import {describe, expect, it} from 'vitest';
import {
  applyEvent,
  applyGap,
  applySnapshot,
  dismissGap,
  emptyLog,
  eventKey,
  gapKey,
  gapsForRender,
} from '../src/client/eventLog';
import type {EventRow, GapNotice} from '../src/client/eventLog';

function event(epoch: string, seq: number, path = '/hook'): EventRow {
  return {epoch, seq, method: 'POST', path, body: {seq}, verification: {valid: true, reason: 'valid'}};
}

function overflowGap(epoch: string, lastSeen: number, oldest: number): GapNotice {
  return {
    epoch,
    fromEpoch: epoch,
    lastSeen,
    oldest,
    reason: 'buffer-overflow',
    message: 'overflow',
  };
}

describe('client event log', () => {
  it('dedupes by (epoch, seq), including replayed duplicates after reconnect', () => {
    let log = applySnapshot(emptyLog(), [event('A', 1), event('A', 2)]);
    // Replay on reconnect delivers the same rows again.
    log = applyEvent(log, event('A', 1));
    log = applyEvent(log, event('A', 2));
    log = applyEvent(log, event('A', 3));
    expect(log.rows.map(eventKey)).toEqual(['A:1', 'A:2', 'A:3']);
  });

  it('keeps same seq numbers from different epochs as distinct rows', () => {
    let log = applyEvent(emptyLog(), event('A', 1));
    log = applyEvent(log, event('B', 1));
    expect(log.rows).toHaveLength(2);
    expect(log.rows.map(row => row.epoch)).toEqual(['A', 'B']);
    expect(new Set(log.rows.map(eventKey)).size).toBe(2);
  });

  it('orders late replayed events inside their epoch segment', () => {
    // Live rows 4,5 arrived first; reconnect replay then fills 2,3.
    let log = applyEvent(emptyLog(), event('A', 4));
    log = applyEvent(log, event('A', 5));
    log = applyEvent(log, event('A', 2));
    log = applyEvent(log, event('A', 3));
    expect(log.rows.map(row => row.seq)).toEqual([2, 3, 4, 5]);
  });

  it('does not clear displayed rows when a gap arrives and places the gap inline', () => {
    // User saw 1,2 before disconnect; after overflow replay brings 4,5 with
    // oldest=4 and a gap at lastSeen=2.
    let log = applySnapshot(emptyLog(), [event('A', 1), event('A', 2)]);
    log = applyEvent(log, event('A', 4));
    log = applyEvent(log, event('A', 5));
    log = applyGap(log, overflowGap('A', 2, 4));

    expect(log.rows.map(row => row.seq)).toEqual([1, 2, 4, 5]);
    const {banners, inline} = gapsForRender(log);
    expect(banners).toHaveLength(0);
    expect(inline).toHaveLength(1);
    expect(inline[0].boundaryKey).toBe('A:2');
  });

  it('shows an epoch-changed gap between the two epoch segments', () => {
    let log = applyEvent(emptyLog(), event('A', 1));
    log = applyEvent(log, event('A', 2));
    const gap: GapNotice = {
      epoch: 'B',
      fromEpoch: 'A',
      lastSeen: 2,
      oldest: 1,
      reason: 'epoch-changed',
      message: 'restart',
    };
    log = applyGap(log, gap);
    log = applyEvent(log, event('B', 1));

    const {banners, inline} = gapsForRender(log);
    expect(banners).toHaveLength(0);
    expect(inline).toHaveLength(1);
    expect(inline[0].boundaryKey).toBe('A:2');
  });

  it('dedupes repeated identical gaps from repeated reconnects', () => {
    let log = applyGap(emptyLog(), overflowGap('A', 1, 3));
    log = applyGap(log, overflowGap('A', 1, 3));
    expect(log.gaps).toHaveLength(1);
    log = dismissGap(log, gapKey(overflowGap('A', 1, 3)));
    expect(log.gaps).toHaveLength(0);
  });

  it('uses a top banner when no boundary rows exist for the gap', () => {
    const log = applyGap(emptyLog(), overflowGap('A', 1, 3));
    const {banners, inline} = gapsForRender(log);
    expect(banners).toHaveLength(1);
    expect(inline).toHaveLength(0);
  });

  it('snapshot never duplicates live rows that raced it', () => {
    let log = applyEvent(emptyLog(), event('A', 1));
    log = applySnapshot(log, [event('A', 1), event('A', 2)]);
    expect(log.rows.map(eventKey)).toEqual(['A:1', 'A:2']);
  });
});
