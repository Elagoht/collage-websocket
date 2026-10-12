package websocket_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	live "github.com/Elagoht/collage-live"
	ws "github.com/Elagoht/collage-websocket"
	"github.com/Elagoht/collage/pkg/collage"
	"github.com/Elagoht/collage/pkg/collagetest"
	"github.com/coder/websocket"
)

type site struct {
	app    *collage.App
	logs   *collagetest.Logs
	lv     *live.Plugin
	cpu    atomic.Int64
	server *httptest.Server
}

// newSite builds the site as collage plugin add lists the plugins: websocket,
// then live. liveConfig, when given, is live's section of plugins-config.json.
func newSite(t *testing.T, liveConfig ...string) *site {
	t.Helper()
	lv := live.New(live.Options{})
	var options []collagetest.SiteOption
	if len(liveConfig) > 0 {
		options = append(options, collagetest.WithPluginConfig(live.Name, liveConfig[0]))
	}
	return newSiteWith(t, lv, []collage.Plugin{ws.New(ws.Options{}), lv}, options...)
}

// newSiteWith builds the site with plugins, lv among them.
func newSiteWith(t *testing.T, lv *live.Plugin, plugins []collage.Plugin, options ...collagetest.SiteOption) *site {
	t.Helper()
	s := &site{lv: lv}
	cpu := collage.NewFragment("cpu", "cpu.html").WithData(collage.LoadTagged(func(context.Context, *collage.RenderContext) (int64, []string, error) {
		return s.cpu.Load(), []string{"system:cpu"}, nil
	})).Build()
	page := collage.NewFragment("page", "page.html").WithSlotFragment("cpu", cpu).Build()
	built := collagetest.NewSite(t, append([]collagetest.SiteOption{
		collagetest.WithPlugins(plugins...),
		collagetest.WithoutHome(),
		collagetest.WithTemplates(map[string]string{
			"page.html": `<html><head>{{liveClient}}</head><body>{{slot "cpu"}}</body></html>`,
			"cpu.html":  `<p>cpu {{.}}</p>`,
		}),
		collagetest.WithRegister(collage.NewPage("home").WithContent(page).WithPath("en", "/").
			WithFragmentPath("en", "/live/cpu", cpu).Build()),
	}, options...)...)
	s.app, s.logs = built.App, built.Logs
	s.server = httptest.NewServer(s.app.Handler())
	t.Cleanup(s.server.Close)
	return s
}

// dial opens a WebSocket at path on s.
func (s *site) dial(t *testing.T, path string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(s.server.URL, "http")+path, nil)
	if err != nil {
		t.Fatalf("Dial %s: %v", path, err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

// page is the home page's body.
func (s *site) page(t *testing.T) string {
	t.Helper()
	res, err := http.Get(s.server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return string(body)
}

func read(t *testing.T, conn *websocket.Conn) live.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var msg live.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("message %q: %v", data, err)
	}
	return msg
}

// The client is told to connect here, and the event stream is not served.
func TestClientPointsHere(t *testing.T) {
	pointsHere(t, newSite(t))
}

func pointsHere(t *testing.T, s *site) {
	t.Helper()
	if body := s.page(t); !strings.Contains(body, `data-collage-stream="/_live/ws/" data-collage-transport="ws"`) {
		t.Errorf("client does not name the WebSocket:\n%s", body)
	}
	if res, _ := http.Get(s.server.URL + "/_live/stream/?f=/live/cpu"); res.StatusCode != http.StatusNotFound {
		t.Errorf("event stream = %d, want 404: the WebSocket replaces it", res.StatusCode)
	}
}

// The connection sends the current state, then a push per invalidation.
func TestPushesOverTheSocket(t *testing.T) {
	roundTrip(t, newSite(t))
}

// Listed after live, by hand or from the v0.3 docs, the plugin still replaces
// live's event stream: the client points here and the stream is not served.
func TestListedAfterLive(t *testing.T) {
	lv := live.New(live.Options{})
	s := newSiteWith(t, lv, []collage.Plugin{lv, ws.New(ws.Options{})})
	pointsHere(t, s)
	roundTrip(t, s)
}

// Without a live plugin the application is not built, and the error names it.
func TestWithoutLive(t *testing.T) {
	_, err := collagetest.TrySite(t, collagetest.WithPlugins(ws.New(ws.Options{})))
	if err == nil || !strings.Contains(err.Error(), "websocket: no collage-live plugin to carry; add elagoht/live") {
		t.Fatalf("TrySite = %v, want the missing live plugin named", err)
	}
}

// The WebSocket is served under live's prefix, wherever that is moved.
func TestFollowsLivePrefix(t *testing.T) {
	s := newSite(t, `{"prefix":"/push/"}`)
	if body := s.page(t); !strings.Contains(body, `data-collage-stream="/push/ws/" data-collage-transport="ws"`) {
		t.Errorf("client does not name /push/ws/:\n%s", body)
	}
	s.cpu.Store(4)
	if msg := read(t, s.dial(t, "/push/ws/?f=/live/cpu")); msg.HTML != "<p>cpu 4</p>" {
		t.Errorf("initial = %+v", msg)
	}
}

// The plugin reads its own section of plugins-config.json.
func TestReadsItsConfiguration(t *testing.T) {
	lv := live.New(live.Options{})
	s := newSiteWith(t, lv, []collage.Plugin{ws.New(ws.Options{}), lv},
		collagetest.WithPluginConfig(ws.Name, `{"originPatterns":["evil.example"]}`))
	header := http.Header{"Origin": []string{"https://evil.example"}}
	url := "ws" + strings.TrimPrefix(s.server.URL, "http") + "/_live/ws/?f=/live/cpu"
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("an origin originPatterns names was refused: %v", err)
	}
	conn.CloseNow()
}

// path and ping were the plugin's own settings for what live's prefix and
// keepAlive say; written in configuration, they stop the application.
func TestRemovedKeys(t *testing.T) {
	for _, config := range []string{`{"path":"/ws/"}`, `{"ping":"10s"}`} {
		lv := live.New(live.Options{})
		_, err := collagetest.TrySite(t, collagetest.WithPlugins(ws.New(ws.Options{}), lv),
			collagetest.WithPluginConfig(ws.Name, config))
		if err == nil || !strings.Contains(err.Error(), "unknown key") {
			t.Errorf("%s: err = %v, want unknown key", config, err)
		}
	}
}

// With live's noStream, nothing is pushed over the WebSocket either: the client
// is not pointed at it, and a connection is refused with the site's 404 page,
// not a logged 500.
func TestNoStream(t *testing.T) {
	lv := live.New(live.Options{})
	s := newSiteWith(t, lv, []collage.Plugin{ws.New(ws.Options{}), lv},
		collagetest.WithPluginConfig(live.Name, `{"noStream":true}`),
		collagetest.WithTemplates(map[string]string{"missing.html": `<h1>Nothing here</h1>`}),
		collagetest.WithSetup(func(app *collage.App) error {
			return app.RegisterNotFoundPage(collage.NewPage("missing").WithContent(collage.NewFragment("missing", "missing.html").Build()).Build())
		}),
	)
	if body := s.page(t); strings.Contains(body, "data-collage-stream") {
		t.Errorf("client is pointed at a push connection:\n%s", body)
	}
	url := "ws" + strings.TrimPrefix(s.server.URL, "http") + "/_live/ws/?f=/live/cpu"
	if _, res, err := websocket.Dial(context.Background(), url, nil); err == nil || res == nil || res.StatusCode != http.StatusNotFound {
		status := 0
		if res != nil {
			status = res.StatusCode
		}
		t.Errorf("dial = %v (status %d), want 404", err, status)
	}
	res, err := http.Get(s.server.URL + "/_live/ws/?f=/live/cpu")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound || !strings.Contains(string(body), "<h1>Nothing here</h1>") {
		t.Errorf("GET = %d %q, want 404 with the site's not-found page", res.StatusCode, body)
	}
	if s.logs.Has(slog.LevelError) {
		t.Errorf("logged an error:\n%s", s.logs)
	}
}

// A connection is closed when live's maxStreamAge has passed, not at the next
// keep-alive after it.
func TestMaxStreamAgeOnTime(t *testing.T) {
	s := newSite(t, `{"maxStreamAge":"200ms","keepAlive":"1h"}`)
	conn := s.dial(t, "/_live/ws/?f=/live/cpu")
	read(t, conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Errorf("after maxStreamAge: %v, want a normal closure within 2s", err)
	}
}

func roundTrip(t *testing.T, s *site) {
	t.Helper()
	s.cpu.Store(3)
	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.server.URL, "http")+"/_live/ws/?f=/live/cpu", nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.CloseNow()
	if msg := read(t, conn); msg.URL != "/live/cpu" || msg.HTML != "<p>cpu 3</p>" {
		t.Fatalf("initial = %+v", msg)
	}
	s.cpu.Store(9)
	_ = s.app.InvalidateTags(ctx, "system:cpu")
	if msg := read(t, conn); msg.HTML != "<p>cpu 9</p>" {
		t.Errorf("pushed %+v", msg)
	}

	// Shutting down closes it, going away.
	s.lv.CloseStreams()
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, _, err := conn.Read(rctx); websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Errorf("after CloseStreams: %v, want going away", err)
	}
}

// A URL that is not a fragment path is refused before the upgrade, and so is a
// page on another origin, which would otherwise open a connection carrying the
// reader's cookies.
func TestRefusals(t *testing.T) {
	s := newSite(t)
	ctx := context.Background()
	base := "ws" + strings.TrimPrefix(s.server.URL, "http") + "/_live/ws/"
	if _, res, err := websocket.Dial(ctx, base+"?f=/nope", nil); err == nil || res == nil || res.StatusCode != http.StatusBadRequest {
		t.Errorf("unknown fragment: err %v", err)
	}
	header := http.Header{"Origin": []string{"https://evil.example"}}
	if _, res, err := websocket.Dial(ctx, base+"?f=/live/cpu", &websocket.DialOptions{HTTPHeader: header}); err == nil || res == nil || res.StatusCode != http.StatusForbidden {
		t.Errorf("cross-origin: err %v", err)
	}
}
