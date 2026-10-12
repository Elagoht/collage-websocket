# elagoht/websocket

A collage plugin that carries [collage-live](https://github.com/Elagoht/collage-live)'s
pushed fragments over a WebSocket instead of an event stream.

```
collage plugin add elagoht/websocket
```

adds collage-live too, and lists `websocket.New(websocket.Options{})` before
`live.New(live.Options{})`:

```go
app, err := collage.New(&collage.Config{
	Plugins: []collage.Plugin{websocket.New(websocket.Options{}), live.New(live.Options{})},
})
```

The plugin finds the application's collage-live plugin when the application is
built, wherever it is listed, and live then does not serve its event stream;
without one, `collage.New` fails with `websocket: no collage-live plugin to carry;
add elagoht/live`. `collage plugin add` lists it before live, which is the
recommended place.

Requires collage v0.61.1 and collage-live v0.5.1 or later.

Nothing else changes. The layout still includes `{{liveClient}}`, which now tells
the client to connect here; elements still say `data-collage-push`; each message is
collage-live's JSON, one text frame per fragment. collage-live stops serving its
event stream.

## Should you use it?

Usually not. Pushing fragments is one-way, which is what an event stream is for: it
needs no dependency, no upgrade through a proxy, and reconnects by itself. This
plugin is for deployments where streams are what breaks — a proxy that buffers them,
a platform that limits them. It is the one piece of the live stack with a
dependency ([github.com/coder/websocket](https://github.com/coder/websocket)), which
is why it is a plugin of its own.

## Options

The push settings are collage-live's, and this plugin reads them from live:

- the WebSocket is served at live's `prefix` plus `ws/`: `/_live/ws/` by default,
  `/push/ws/` with `"prefix": "/push/"`;
- an idle connection is pinged every live `keepAlive` (25s by default);
- a connection is closed after live's `maxStreamAge`, and the client reopens it
  with the cookies it holds then;
- with live's `noStream`, nothing is pushed over the WebSocket either: the client
  only polls, and a connection is refused with 404 and the site's not-found page.

The plugin's own option:

| Option | JSON | Default | |
| --- | --- | --- | --- |
| `OriginPatterns` | `originPatterns` | the site's own origin | Hosts besides the site's own whose pages may connect, as github.com/coder/websocket matches them |

```json
{
  "elagoht/live": { "prefix": "/push/", "keepAlive": "20s", "maxStreamAge": "1h" },
  "elagoht/websocket": { "originPatterns": ["admin.example.com"] }
}
```

A connection carries the reader's cookies, so by default only a page from the site
itself may open one: a page on another origin is refused with 403. `OriginPatterns`
names others that may.

The client opens the WebSocket from collage-live's shared worker, so every tab of
the browser shares one connection, as with the event stream.

A URL that is not a fragment path is refused with 400 before the upgrade. The
connection is closed with "going away" when the application shuts down; the client
reconnects, and falls back to polling after three failed attempts.

## Changed in v0.4.1

- With live's `noStream`, a connection is answered with the site's not-found
  page (404), where it was a plain-text 404. Requires collage v0.61.2.

## Changed in v0.4.0

| Was | Now |
| --- | --- |
| `websocket.New(lv)`, `websocket.New(nil)`, `websocket.NewWith(lv, opts)` | `websocket.New(websocket.Options{})`; live is always found in the application |
| `Options.Path`, `websocket.DefaultPath` | removed: the WebSocket is served at live's `prefix` + `ws/` |
| `Options.Ping` | removed: live's `keepAlive` |
| configuration ignored | `"elagoht/websocket"` in plugins-config.json is read: `originPatterns`; `path` or `ping` there stops the application with `unknown key` |

Fixed: live's `maxStreamAge` closes the connection on time, where it could stay
open up to a keep-alive (25s) longer; with live's `noStream` a connection is
refused with 404, where it was a logged 500. The plugin still replaces live's
event stream wherever it is listed, before or after live. Requires collage
v0.61.1 and collage-live v0.5.1, whose event stream also closes on time at
`maxStreamAge`.

## Changed in v0.3.0

- `New(nil)` finds the application's collage-live plugin in `Configure`, through
  collage v0.58.0's `collage.FindPlugin`, so `collage plugin add` can write it.
  No live plugin is the error `websocket: no collage-live plugin to carry; add
  elagoht/live`. `New(lv)` works as before.
- collage.json `requires` elagoht/live and has `order` −1, so `collage plugin add`
  adds live and lists this plugin before it.
- Built against collage v0.58.0. Requires it.

## Changed in v0.2.4

- Built against collage v0.50.0, whose render values are typed keys, and
  collage-live v0.4.2. Requires both.

## Changed in v0.2.3

- Built against collage v0.49.0, whose fragment data is a typed `collage.Data`,
  and collage-live v0.4.1. Requires both.

## Changed in v0.2.2

- `collage.json`: the plugin described to editors — its template functions,
  snippets and configuration schema — for the Collage Snippets & Highlighter
  extension and any tool reading it.

## Changed in v0.2.1

- Requires collage-live v0.2.1, which fixes a tab coming back from the
  back-forward cache receiving no pushes, and makes a form whose action redirects
  navigate in one request. Requires collage v0.24.0.

## Changed in v0.2.0

- Follows collage-live v0.2.0: watches carry the ETag the client holds, and
  `maxStreamAge` closes the connection for the client to reopen.
