# Feature request: Now Playing card should offer group-wide volume and mute controls

## Summary

The dashboard **Now Playing** card (player bar rendered by the Tater host) offers no
group-wide volume or mute controls. When music plays to a group of speakers (several
satellites, a native Sonos group, or a stereo pair plus satellites), the user has to set
volume on each speaker individually and mute each one individually.

Personal Music Core already applies **one volume to every target in the queue's group**
(`_set_target_volume` sends the same absolute level to each member — a group at
12/10/7/15 set to 10 ends up at 10/10/10/10), and the core's own **Music Player** card on
the Personal Music tab now carries 🔇 All / 🔊 All buttons (v3.1.0). But the dashboard
**Now Playing** card — the one most people see while music is playing — is host-rendered
and has no equivalent controls, and the host dashboard cannot reach the core's card
actions directly.

## Why it matters

- "Set all speakers to 70%" is the natural request when a group is playing; today the
  dashboard's volume control either acts on one speaker or does not exist at all.
- Mute-all is a one-tap need ("phone rings", "someone's asleep") that currently requires
  stopping playback or muting speakers one by one.
- Voice can do this in Personal Music Core (`personal_music_control` with `action:
  "volume" | "mute_all" | "unmute_all"`, v3.1.0), but guests using the dashboard have no
  voice path and no visible control.

## Requested change

On the host's Now Playing card / player bar:

1. **Group volume**: a volume control that sends one absolute level to *every* speaker
   currently playing the music (the whole group), not just a single target. A "Sync all"
   semantics — setting the group to 10 makes every member 10.
2. **Mute all / Unmute all**: two explicit buttons. One action mutes every member of the
   current group; the other unmutes every member (restoring the pre-mute level). A muted
   state should be visible on the card (the core exposes `player.muted` /
   `player.volume_percent` via `get_client_music_state` and the tab data).
3. If the host already has a per-speaker volume model, expose the group value as the
   "All" control the way the Personal Music tab's player card does, so the two stay
   consistent.

## Impact

- Host file(s): the dashboard player bar / Now Playing card renderer in
  `tateros_static/ui/tater-ui.js` (and wherever host player-bar actions are dispatched to
  cores or to the built-in music core).
- No core-side change is required if the host can address the existing core actions; the
  core's `music_ui_set_volume` / `music_ui_mute_all` / `music_ui_unmute_all` tab actions
  already implement the exact semantics (absolute group volume, mute-with-remember,
  unmute-restore).