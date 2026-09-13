# Feature request: Music tab should show success messages from core tab actions

## Summary

The Music tab UI (`MusicCoreApp.run()` in `tateros_static/ui/tater-ui.js`) discards the
`message` field of every **successful** core tab action. Only errors are surfaced — the
`run()` helper clears the toast, posts the action, refetches the tab on success, and only
assigns the toast on a thrown error.

## Why it matters

Cores have no other channel to tell the user an action worked. Concretely, in the Personal
Music Core: clicking **Test Emby Connection** on a Person link card returns
`{"ok": true, "message": "Zoe's Emby connection works — zoe signed in to http://…"}`
and the user sees *nothing at all*. A passing test and a silently ignored click look
identical, so the button gets pressed repeatedly.

(Personal Music Core 2.2.1 works around this by recording the last test result in Redis and
showing it as a summary row on the card after the refetch, but the generic problem remains
for every core action that has nothing to re-render.)

## Requested change

When a tab action returns a dict with a non-empty `message` (and `ok` not false), show that
message as a success toast/notice, the same way error details are shown today. Example shape
the host already produces elsewhere: `V(message, "success")` in the Cores app.

## Impact

- Host file(s): `tateros_static/ui/tater-ui.js` (Music tab `run()` helper; the POST wrapper
  `bc()` currently throws away the parsed response body).
- No core-side or API changes needed; the `{"ok": true, "message": "…"}` contract already
  exists.