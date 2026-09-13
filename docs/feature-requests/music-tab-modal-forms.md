# Feature request: Music tab settings cards should support modal popups (Manage/Edit)

## Summary

Core tabs with `ui.kind == "settings_manager"` and `ui.appearance == "music_library"` are rendered
by `MusicCoreApp` (`tateros_static/ui/tater-ui.js`), whose settings-card component has no popup
support: fields render inline (or behind the "Connection settings" `<details>`), and every card
shows all of its actions at once. Cores cannot open a form in a modal.

The generic core renderer (`renderCoreSettingsManager` in `tateros_static/app.js`) already supports
this — `ui.item_fields_popup: true` plus per-item `settings_label` / `settings_title` renders a
button that opens the item's fields (and its `save_action`) in a modal. The Jarvis Screen core's
Cards/Layouts/Screens tabs use exactly this ("Manage" button → modal with fields and Save), and it
works well. The Vue music renderer just never grew the same hooks.

## Why it matters

The Personal Music Core's People tab is cluttered because each Person link needs ~10 form fields
(Emby URL/user/password/API key, share folder, playback-conflict mode, follow-me settings) on
permanent display. A compact card (name + sync state + Edit/Remove) with the form in a modal opened
by Edit is the layout the generic renderer enables for free — the music UI only needs the same two
hooks.

(Personal Music Core 2.3.0 emulates this core-side: an Edit action stores the target Person in Redis
and the next refetch swaps that Person's compact card for the full form. It works, but it costs a
round trip, loses the modal affordance, and the host already ships a modal component — `tm-modal` —
in the same UI.)

## Requested change

In `MusicCoreApp`'s settings-card component (`tater-ui.js`):

- Honor `ui.item_fields_popup` / `ui.item_fields_popup_label` (tab-level default) and per-item
  `fields_popup`, `settings_label`, `settings_title` the same way `renderCoreSettingsManager` does.
- When enabled, items with fields render a button (label from `settings_label`, e.g. "Manage" or
  "Edit") that opens the existing `tm-modal` component containing the item's fields and its
  `save_action` submit button (`save_label` as caption), closing on Save/Cancel.
- Items without fields (compact tiles with only `actions`) keep rendering their action buttons
  inline, as today.

Example payload shape (already produced by the Jarvis Screen core, so this adds no new contract):

```json
{
  "ui": {
    "kind": "settings_manager",
    "appearance": "music_library",
    "item_fields_popup": true,
    "item_forms": [
      {
        "id": "person:abc",
        "title": "Zoe",
        "settings_label": "Edit",
        "settings_title": "Edit Zoe's music link",
        "fields": [ "…" ],
        "save_action": "music_person_link_save",
        "actions": [ "…" ]
      }
    ]
  }
}
```

## Impact

- Host file(s): `tateros_static/ui/tater-ui.js` (settings-card component of `MusicCoreApp`).
- No core-side or API changes needed; the payload contract already exists in the generic renderer.