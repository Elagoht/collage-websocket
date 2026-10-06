package websocket_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	live "github.com/Elagoht/collage-live"
	ws "github.com/Elagoht/collage-websocket"
	"github.com/Elagoht/collage/pkg/collage"
	"github.com/coder/websocket"
)

type site struct {
	app    *collage.App
	lv     *live.Plugin
	cpu    atomic.Int64
	server *httptest.Server
}

func newSite(t *testing.T) *site {
	t.Helper()
	s := &site{lv: live.New()}
	app, err := collage.New(&collage.Config{
		Server: collage.ServerConfig{Host: "localhost", Port: 3000},
		Template: collage.TemplateConfig{FS: fstest.MapFS{
			"t/page.html": {Data: []byte(`<html><head>{{liveClient}}</head><body>{{slot "cpu"}}</body></html>`)},
			"t/cpu.html":  {Data: []byte(`<p>cpu {{.}}</p>`)},
		}, Root: "t"},
		Plugins: []collage.Plugin{s.lv, ws.New(s.lv)},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cpu := collage.NewFragment("cpu", "cpu.html").WithData(collage.DataHandler(func(context.Context, *collage.RenderContext) (int64, []string, error) {
		return s.cpu.Load(), []string{"system:cpu"}, nil
	})).Build()
	page := collage.NewFragment("page", "page.html").WithSlotFragment("cpu", cpu).Build()
	if err := app.RegisterPage(collage.NewPage("home").WithContent(page).WithPath("en", "/").
		WithFragmentPath("en", "/live/cpu", cpu).Build()); err != nil {
		t.Fatal(err)
	}
	s.app = app
	s.server = httptest.NewServer(app.Handler())
	t.Cleanup(s.server.Close)
	return s
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
	s := newSite(t)
	res, err := http.Get(s.server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), `data-collage-stream="/_live/ws/" data-collage-transport="ws"`) {
		t.Errorf("client does not name the WebSocket:\n%s", body)
	}
	if res, _ := http.Get(s.server.URL + "/_live/stream/?f=/live/cpu"); res.StatusCode != http.StatusNotFound {
		t.Errorf("event stream = %d, want 404: the WebSocket replaces it", res.StatusCode)
	}
}

// The connection sends the current state, then a push per invalidation.
func TestPushesOverTheSocket(t *testing.T) {
	s := newSite(t)
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
