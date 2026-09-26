// Package websocket is a collage plugin that carries collage-live's pushed
// fragments over a WebSocket instead of an event stream.
//
//	lv := live.New()
//	app, err := collage.New(&collage.Config{
//		Plugins: []collage.Plugin{lv, websocket.New(lv)},
//	})
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
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	live "github.com/Elagoht/collage-live"
	"github.com/Elagoht/collage/pkg/collage"
	"github.com/coder/websocket"
)

// Name is the plugin's name, and the key its configuration is found under.
const Name = "elagoht/websocket"

// DefaultPath is where the WebSocket is served.
const DefaultPath = "/_live/ws/"

// Options configures the plugin. Everything has a default.
type Options struct {
	// Path is where the WebSocket is served. It begins and ends with a slash.
	// Default "/_live/ws/".
	Path string
	// Ping is how often an idle connection is pinged, so a proxy that closes quiet
	// connections leaves it open and a peer that vanished is noticed. Default 25s.
	Ping time.Duration
	// OriginPatterns are the hosts, besides the site's own, whose pages may
	// connect — as github.com/coder/websocket matches them. A connection carries
	// the reader's cookies, so a page on another origin must not be able to open
	// one on the reader's behalf; the default allows only the site itself.
	OriginPatterns []string
}

// Plugin serves the WebSocket.
type Plugin struct {
	live *live.Plugin
	opts Options
	log  *slog.Logger
}

// New returns a plugin carrying lv's pushes, with the default options. lv must be
// registered with the application too, before this plugin.
func New(lv *live.Plugin) *Plugin { return NewWith(lv, Options{}) }

// NewWith returns a plugin carrying lv's pushes with opts.
//
// It tells lv to point the client here straight away, rather than in Configure or
// Init: lv decides in its own Init whether to serve its event stream, and it may
// be initialised first.
func NewWith(lv *live.Plugin, opts Options) *Plugin {
	if opts.Path == "" {
		opts.Path = DefaultPath
	}
	if opts.Ping <= 0 {
		opts.Ping = 25 * time.Second
	}
	if lv != nil {
		lv.UseTransport(live.Transport{Kind: "ws", Path: opts.Path})
	}
	return &Plugin{live: lv, opts: opts}
}

func (p *Plugin) Name() string    { return Name }
func (p *Plugin) Version() string { return "0.2.1" }

// Init serves the WebSocket.
func (p *Plugin) Init(_ context.Context, host collage.Host) error {
	if p.live == nil {
		return errors.New("websocket: no collage-live plugin to carry")
	}
	if !strings.HasPrefix(p.opts.Path, "/") || !strings.HasSuffix(p.opts.Path, "/") {
		return fmt.Errorf("websocket: path %q must begin and end with a slash", p.opts.Path)
	}
	p.log = host.Logger()
	return host.Handle(p.opts.Path, http.HandlerFunc(p.serve))
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
	var expired <-chan time.Time
	if age := p.live.MaxStreamAge(); age > 0 {
		timer := time.NewTimer(age)
		defer timer.Stop()
		expired = timer.C
	}
	for {
		select {
		case <-expired:
			// Closed from here, the client reconnects with the cookies it holds
			// now; see live.Config.MaxStreamAge.
			conn.Close(websocket.StatusNormalClosure, "reconnect")
			return
		default:
		}
		wait, cancel := context.WithTimeout(ctx, p.opts.Ping)
		msgs, err := sub.Wait(wait)
		cancel()
		switch {
		case err == nil:
			if send(msgs) != nil {
				return
			}
		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
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
