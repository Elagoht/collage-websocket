# elagoht/websocket

A collage plugin that carries [collage-live](https://github.com/Elagoht/collage-live)'s
pushed fragments over a WebSocket instead of an event stream.

```go
lv := live.New()
app, err := collage.New(&collage.Config{
	Plugins: []collage.Plugin{lv, websocket.New(lv)},
})
```

Requires collage v0.50.0 and collage-live v0.4.2 or later. Register collage-live as
well, before this plugin.

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

```go
websocket.NewWith(lv, websocket.Options{
	Path:           "/_live/ws/",   // where it is served
	Ping:           25 * time.Second,
	OriginPatterns: []string{"admin.example.com"},
})
```

A connection carries the reader's cookies, so by default only a page from the site
itself may open one: a page on another origin is refused with 403. `OriginPatterns`
names others that may.

The client opens the WebSocket from collage-live's shared worker, so every tab of
the browser shares one connection, as with the event stream. collage-live's
`maxStreamAge` applies here too: the connection is closed after that long, and the
client reopens it with the cookies it holds then.

A URL that is not a fragment path is refused with 400 before the upgrade. The
connection is pinged when idle, and closed with "going away" when the application
shuts down; the client reconnects, and falls back to polling after three failed
attempts.

## Changes

### v0.2.4

- Built against collage v0.50.0, whose render values are typed keys, and
  collage-live v0.4.2. Requires both.

### v0.2.3

- Built against collage v0.49.0, whose fragment data is a typed `collage.Data`,
  and collage-live v0.4.1. Requires both.

### v0.2.2

- `collage.json`: the plugin described to editors — its template functions,
  snippets and configuration schema — for the Collage Snippets & Highlighter
  extension and any tool reading it.

### v0.2.1

- Requires collage-live v0.2.1, which fixes a tab coming back from the
  back-forward cache receiving no pushes, and makes a form whose action redirects
  navigate in one request. Requires collage v0.24.0.

### v0.2.0

- Follows collage-live v0.2.0: watches carry the ETag the client holds, and
  `maxStreamAge` closes the connection for the client to reopen.
