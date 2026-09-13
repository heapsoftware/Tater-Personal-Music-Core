# Feature request: Music tab should preserve unsaved form values across the post-action refetch

## Summary

After any core tab action succeeds, the Music tab refetches the whole tab payload
(`await y()` in `MusicCoreApp.run()`). The settings cards remount from the fresh payload, so
every field resets to the saved server value and **anything the user typed but had not saved
yet is silently erased**.

## Why it matters

Cards that mix saved settings with a non-saving action lose the user's input. Concretely, in
the Personal Music Core: a Person link card has both a **Save/Link** button and a **Test Emby
Connection** button. The test is intentionally save-free ("test these credentials without
saving them"), but after clicking it the refetch empties the server URL, username, password,
and library name the user just typed — so the next click reports "Enter the Emby server URL
to test."

(Personal Music Core 2.2.1 works around this by caching the last-tested form values in Redis
and prefilling the cards from them on the next payload build, but that only covers the one
action the core knows about; any other button on a form card still wipes edits.)

## Requested change

Keep the per-card form state (`SettingsCard`'s local `values` ref, plus its "touched" set)
across payload refetches — e.g. key the form state by item id outside the component, or have
the refetch merge untouched field values back into the new payload. Alternatively, skip the
full refetch for actions whose result indicates no data changed.

## Impact

- Host file(s): `tateros_static/ui/tater-ui.js` (`SettingsCard` state lifetime, or
  `MusicCoreApp.run()`'s success-path refetch).
- No core-side or API changes needed.