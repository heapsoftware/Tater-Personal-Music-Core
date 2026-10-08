# Feature request: stop (or detach) a single member of a live Sendspin stream

## Summary

`Tater v1.3.0` moves satellite playback to Sendspin: one host-side timeline
(`tater_voice/sendspin_playback.py`) streams timestamped PCM to every member of
a multi-player group under a single `stream_id`. Music Cores can only stop that
stream as a whole — `sendspin_playback.stop_live_stream(stream_id)` and
`stop_live_streams_for_targets(selectors)` both tear down the entire stream.
There is no API to detach one member (one satellite, one room) from a live
stream and leave the rest of it playing.

## Why a core needs this

Personal Music Core runs one queue per Person, and a Person's queue can play on
a multi-room group. When someone else's music takes over one room of a playing
group ("release targets": the Follow-Me transfer path in
`_release_targets_to`), that core wants to stop exactly the stolen rooms and
keep its music playing on the rest.

That was possible with the retired per-satellite `media.session` transport
(each member had its own session id and could be stopped individually). On a
Sendspin host the only available stop is by `stream_id`, so stopping one room
today also silences every other room sharing the stream; the affected queues
go to paused and stay silent until their owner explicitly resumes them
(resuming restarts them correctly on their remaining targets).

Same problem for per-room volume: `set_live_stream_volumes(stream_id, volumes)`
can scale a member to 0 without detaching it, which is a workable substitute
for volume, but a muted member still consumes its share of the timeline and
needs follow-up care when the stream advances.

## Requested host change

In `tater_voice/sendspin_playback.py`, add a per-member operation on an active
stream, e.g.:

```python
async def detach_member_from_stream(
    stream_id: str,
    selectors: Iterable[str],
) -> Dict[str, Any]:
    """Close the given members' Sendspin peers (and any bridge routes owned
    by only those members) and remove them from the stream's ownership map,
    while the stream keeps playing for the remaining members."""
```

Minimal viable alternative: `stop_live_streams_for_targets(selectors)` gaining
an `only_members: bool = False` kwarg, where `True` rewrites the remaining
members' peer set instead of cancelling the stream task (rebalance
`volume_percent`/`delay_ms` handling is not needed for detach — the timeline
timestamps are per-frame absolute already).

## Notes

- Cores can already stop whole streams by recorded id and by current ownership
  (`stop_live_streams_for_targets`), and pause/resume/next/prev keep working
  through stop-plus-replay from the saved position; only per-room stopping is
  missing.
- Stereo pairs: a pair's two member satellites share one upstream and one
  logical selector, so a detach that splits a pair leaves one orphan channel.
  Pairs should detach as a unit (both members), or the API should reject
  detaching half of a saved pair.