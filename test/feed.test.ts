import {describe, expect, it} from 'vitest';
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
} from '../src/client/feed';

function row(epoch: string, id: number): EventRow {
  return {
    id,
    epoch,
    workspace: 'default',
    method: 'POST',
    path: '/api/capture/hook',
    body: {id},
    verification: {valid: true, reason: 'valid'},
  };
}

describe('feed reducer', () => {
  it('dedupes by epoch+id so replays neither double-show nor double-count', () => {
    const [s1, addedFirst] = applyEvent(initialFeed(), row('e1', 1));
    const [s2, addedAgain] = applyEvent(s1, row('e1', 1));
    expect(addedFirst).toBe(true);
    expect(addedAgain).toBe(false);
    expect(s2).toBe(s1); // duplicates are a no-op
    expect(s2.events).toHaveLength(1);
  });

  it('keeps same-numbered events from different epochs as distinct rows', () => {
    const [s1] = applyEvent(initialFeed(), row('epoch-a', 1));
    const [s2, added] = applyEvent(s1, row('epoch-b', 1));
    expect(added).toBe(true);
    expect(s2.events.map(eventKey)).toEqual(['epoch-b:1', 'epoch-a:1']);
  });

  it('merges snapshots with live arrivals, newest first, without duplicates', () => {
    let s: FeedState = applySnapshot(initialFeed(), [row('e', 1), row('e', 2), row('e', 3)]);
    expect(s.events.map(e => e.id)).toEqual([3, 2, 1]);
    [s] = applyEvent(s, row('e', 4));
    // Refetch overlapping the live tail (e.g. after an epoch-gap resnapshot).
    s = applySnapshot(s, [row('e', 1), row('e', 2), row('e', 3), row('e', 4)]);
    expect(s.events.map(e => e.id)).toEqual([4, 3, 2, 1]);
  });

  it('a gap keeps the displayed events and only annotates the feed', () => {
    let s: FeedState = applySnapshot(initialFeed(), [row('e', 4), row('e', 5), row('e', 6)]);
    s = applyGap(s, {kind: 'gap', workspace: 'default', epoch: 'e', after: 1, next: 4, reason: 'overflow'});
    expect(s.events.map(e => e.id)).toEqual([6, 5, 4]);
    expect(s.gap?.reason).toBe('overflow');
    s = dismissGap(s);
    expect(s.gap).toBeNull();
    expect(s.events).toHaveLength(3);
  });

  it('an epoch gap keeps old rows and lets the new epoch restart from id 1', () => {
    const [s1] = applyEvent(initialFeed(), row('old', 3));
    let s: FeedState = applyGap(s1, {
      kind: 'gap',
      workspace: 'default',
      epoch: 'new',
      after: null,
      next: 1,
      reason: 'epoch',
    });
    expect(s.epoch).toBe('new');
    expect(s.events).toHaveLength(1);
    let added = false;
    [s, added] = applyEvent(s, row('new', 1));
    expect(added).toBe(true);
    expect(s.events.map(eventKey)).toEqual(['new:1', 'old:3']);
  });

  it('derives the resume cursor from the newest snapshot row', () => {
    expect(snapshotCursor([])).toBeNull();
    expect(snapshotCursor([row('e', 7), row('e', 8)])).toBe('e:8');
  });
});
