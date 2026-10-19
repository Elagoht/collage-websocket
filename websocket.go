// Package websocket is a collage plugin that carries collage-live's pushed
// fragments over a WebSocket instead of an event stream.
//
//	app, err := collage.New(&collage.Config{
//		Plugins: []collage.Plugin{client.New(), websocket.New(websocket.Options{}), live.New(live.Options{})},
//	})
//
// The plugin finds the application's live plugin in Configure, wherever it is
// listed, and replaces live's event stream. It serves the WebSocket at live's
// prefix plus "ws/" ("/_live/ws/" by default), pinging an idle connection every
// live keepAlive and closing one after live's maxStreamAge: the push settings
// are live's, read from it in Init. collage plugin add lists it before live.
//
// Nothing else changes: the layout still includes {{liveClient}}, which now tells
// the client to connect here, and elements still say data-collage-push. The
// messages are collage-live's, one JSON text frame each.
//
// An event stream is usually the better choice. Pushing fragments is one-way,
// which is what a stream is; it needs no dependency, no upgrade through a proxy,
// and reconnects by itself. This plugin is for the deployments where streams are
// what breaks — a proxy that buffers them, a platform that limits them.
package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	live "github.com/Elagoht/collage-live"
	"github.com/Elagoht/collage/pkg/collage"
	"github.com/coder/websocket"
)

// Name is the plugin's name, and the key its configuration is found under.
const Name = "elagoht/websocket"

// Options configures the plugin. Everything has a default; the push settings
// (prefix, keepAlive, maxStreamAge, noStream) are collage-live's.
type Options struct {
	// OriginPatterns are the hosts, besides the site's own, whose pages may
	// connect — as github.com/coder/websocket matches them. A connection carries
	// the reader's cookies, so a page on another origin must not be able to open
	// one on the reader's behalf; the default allows only the site itself.
	OriginPatterns []string `json:"originPatterns"`
}

// Plugin serves the WebSocket.
type Plugin struct {
	opts Options
	live *live.Plugin
	host collage.Host
	log  *slog.Logger
	// path, keepAlive and maxAge are live's, read in Init.
	path      string
	keepAlive time.Duration
	maxAge    time.Duration
}

// New returns the plugin with opts as its starting point, which the
// application's own configuration is then decoded over. The application must
// have collage-live too; without it, it is not built.
func New(opts Options) *Plugin { return &Plugin{opts: opts} }

func (p *Plugin) Name() string    { return Name }
func (p *Plugin) Version() string { return "0.4.2" }

// errNoLive is the error for an application without the live plugin.
var errNoLive = errors.New("websocket: no collage-live plugin to carry; add elagoht/live")

// Configure reads the configuration and finds the application's live plugin.
func (p *Plugin) Configure(_ context.Context, host collage.ConfigHost) error {
	opts, err := collage.PluginConfig(host, p.opts)
	if err != nil {
		return err
	}
	p.opts = opts
	lv, ok := collage.FindPlugin[*live.Plugin](host)
	if !ok || lv == nil {
		return errNoLive
	}
	// Reserve the transport now, before any plugin's Init, so live never serves
	// its event stream wherever it is listed. The path is set in Init, once
	// live's prefix is final.
	lv.UseTransport(live.Transport{Kind: "ws"})
	p.live = lv
	return nil
}

// Init reads live's push settings, final now that every plugin's Configure has
// run, points live's client at the WebSocket's path and serves it.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	if p.live == nil {
		return errNoLive
	}
	p.path = p.live.Prefix() + "ws/"
	p.keepAlive = p.live.KeepAlive()
	p.maxAge = p.live.MaxStreamAge()
	p.host, p.log = host, host.Logger()
	p.live.UseTransport(live.Transport{Kind: "ws", Path: p.path})
	return host.Handle(p.path, http.HandlerFunc(p.serve))
}

// Shutdown releases nothing. The connections end when collage-live closes its
// subscriptions, which it does before the server stops.
func (p *Plugin) Shutdown(context.Context) error { return nil }

// serve upgrades one page's connection and pushes what its subscription yields.
//
// The subscription is made before the upgrade, so a URL that is not a fragment
// path is answered with a plain 400 a developer can read, and the forgery cookie a
// pushed form needs goes out on the upgrade response.
func (p *Plugin) serve(w http.ResponseWriter, r *http.Request) {
	sub, err := p.live.Subscribe(r, live.ParseWatches(r.URL.Query()["f"]))
	switch {
	case errors.Is(err, live.ErrClosed):
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	case errors.Is(err, live.ErrNoStream):
		// live's noStream turns pushing off over any transport; the client is not
		// pointed here, and the event stream answers 404 too: the site's own
		// not-found page, as for any path it does not serve.
		p.host.ServeStatus(w, r, http.StatusNotFound)
		return
	case errors.Is(err, collage.ErrUnknownFragmentPath), errors.Is(err, live.ErrNoFragments), errors.Is(err, live.ErrTooManyFragments):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	case err != nil:
		p.log.Error("websocket: subscribe", "err", err)
		http.Error(w, "unavailable", http.StatusInternalServerError)
		return
	}
	defer sub.Close()
	if cookie := sub.Cookie(); cookie != nil {
		http.SetCookie(w, cookie)
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: p.opts.OriginPatterns})
	if err != nil {
		// Accept has answered already.
		return
	}
	// Nothing is read from the client; CloseRead answers its pings and closes, and
	// ends ctx when the client goes away.
	ctx := conn.CloseRead(r.Context())

	send := func(msgs []live.Message) error {
		for _, msg := range msgs {
			data, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			write, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = conn.Write(write, websocket.MessageText, data)
			cancel()
			if err != nil {
				return err
			}
		}
		return nil
	}

	if err := send(sub.Initial()); err != nil {
		return
	}
	// deadline is when the connection is closed for the client to reconnect with
	// the cookies it holds then; see live.Options.MaxStreamAge. Each wait ends at
	// it, so the connection does not outlive it by up to a keep-alive.
	var deadline time.Time
	if p.maxAge > 0 {
		deadline = time.Now().Add(p.maxAge)
	}
	expired := func() bool { return !deadline.IsZero() && !time.Now().Before(deadline) }
	for {
		if expired() {
			conn.Close(websocket.StatusNormalClosure, "reconnect")
			return
		}
		timeout := p.keepAlive
		if !deadline.IsZero() {
			timeout = min(timeout, time.Until(deadline))
		}
		wait, cancel := context.WithTimeout(ctx, timeout)
		msgs, err := sub.Wait(wait)
		cancel()
		switch {
		case err == nil:
			if send(msgs) != nil {
				return
			}
		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
			if expired() {
				continue
			}
			ping, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := conn.Ping(ping)
			cancel()
			if err != nil {
				return
			}
		case errors.Is(err, live.ErrClosed):
			conn.Close(websocket.StatusGoingAway, "shutting down")
			return
		default:
			conn.Close(websocket.StatusNormalClosure, "")
			return
		}
	}
}
