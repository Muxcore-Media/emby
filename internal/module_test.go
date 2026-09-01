package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	embyv1 "github.com/Muxcore-Media/emby/proto/embyv1"
	playbackv1 "github.com/Muxcore-Media/playback-contract/proto/playbackv1"
)

func TestEmbySessionPollPublishesEvents(t *testing.T) {
	sample := `[{
		"Id": "sess-1",
		"UserId": "u1",
		"UserName": "alice",
		"Client": "Emby Web",
		"DeviceName": "Chrome",
		"RemoteEndPoint": "10.0.0.2:1234",
		"NowPlayingItem": {"Id": "item-1", "Name": "Movie", "Type": "Movie", "Path": "/media/movie.mkv", "RunTimeTicks": 72000000000},
		"PlayState": {"PositionTicks": 600000000, "IsPaused": false, "PlayMethod": "Transcode"},
		"TranscodingInfo": {}
	}]`

	active := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Sessions":
			w.Header().Set("Content-Type", "application/json")
			if active {
				_, _ = w.Write([]byte(sample))
			} else {
				_, _ = w.Write([]byte(`[]`))
			}
		case "/System/Info":
			_, _ = w.Write([]byte(`{"Id":"server-1"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var mu sync.Mutex
	var events []string
	var lastPayload []byte
	testPublishHook = func(_ context.Context, eventType, _ string, payload []byte) error {
		mu.Lock()
		events = append(events, eventType)
		lastPayload = append([]byte(nil), payload...)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { testPublishHook = nil })

	m := NewModule(Config{
		BaseURL:         srv.URL,
		Token:           "tok",
		SessionsPollSec: 30,
		GRPCAddr:        "127.0.0.1:0",
		HTTPAddr:        "127.0.0.1:0",
		DataDir:         t.TempDir(),
	})
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	m.pollSessionsOnce(ctx)
	mu.Lock()
	if len(events) != 1 || events[0] != playbackevents.EventPlaybackStarted {
		t.Fatalf("events: %v", events)
	}
	msg, err := playbackv1.UnmarshalSessionEvent(lastPayload)
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if msg.GetDurationSeconds() != 7200 {
		t.Fatalf("duration_seconds: %d", msg.GetDurationSeconds())
	}

	m.pollSessionsOnce(ctx)
	mu.Lock()
	if len(events) != 2 || events[1] != playbackevents.EventPlaybackProgress {
		t.Fatalf("progress: %v", events)
	}
	mu.Unlock()

	active = false
	m.pollSessionsOnce(ctx)
	mu.Lock()
	if len(events) != 3 || events[2] != playbackevents.EventPlaybackStopped {
		t.Fatalf("stop: %v", events)
	}
	mu.Unlock()
}

func TestEmbySSEEventIngest(t *testing.T) {
	var mu sync.Mutex
	var lastType string
	testPublishHook = func(_ context.Context, eventType, _ string, payload []byte) error {
		mu.Lock()
		lastType = eventType
		mu.Unlock()
		msg, err := playbackv1.UnmarshalSessionEvent(payload)
		if err != nil {
			t.Errorf("unmarshal: %v", err)
			return nil
		}
		if msg.GetExternalSessionId() != "s1" || msg.GetItemId() != "i1" {
			t.Errorf("payload: %+v", msg)
		}
		return nil
	}
	t.Cleanup(func() { testPublishHook = nil })

	m := NewModule(Config{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0", SSESecret: "s3cret", DataDir: t.TempDir()})
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	body, _ := json.Marshal(map[string]any{
		"state":         "playing",
		"sessionId":     "s1",
		"itemId":        "i1",
		"userId":        "u1",
		"positionTicks": 100000000,
	})
	req := httptest.NewRequest(http.MethodPost, "/emby/sse/events", bytes.NewReader(body))
	req.Header.Set("X-Emby-SSE-Secret", "s3cret")
	w := httptest.NewRecorder()
	m.handleSSEEvent(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	mu.Lock()
	if lastType != playbackevents.EventPlaybackStarted {
		t.Fatalf("type %q", lastType)
	}
	mu.Unlock()
}

func TestEmbySSEAuthRequired(t *testing.T) {
	m := NewModule(Config{SSESecret: "s3cret", DataDir: t.TempDir()})
	req := httptest.NewRequest(http.MethodPost, "/emby/sse/events", strings.NewReader(`{"state":"playing"}`))
	w := httptest.NewRecorder()
	m.handleSSEEvent(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", w.Code)
	}
}

func TestHealthConfiguredAndEmpty(t *testing.T) {
	ctx := context.Background()
	unconfigured := NewModule(Config{DataDir: t.TempDir()})
	if err := unconfigured.Health(ctx); err == nil {
		t.Fatal("expected unconfigured health error")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/System/Info" {
			_, _ = w.Write([]byte(`{"Id":"srv"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	m := NewModule(Config{BaseURL: srv.URL, Token: "tok", DataDir: t.TempDir()})
	if err := m.Health(ctx); err != nil {
		t.Fatalf("configured health: %v", err)
	}
}

func TestTerminateSessionEmptyID(t *testing.T) {
	m := NewModule(Config{DataDir: t.TempDir()})
	resp, err := m.TerminateSession(context.Background(), &embyv1.TerminateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetOk() || resp.GetError() != "session_id required" {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestSettingsDurable(t *testing.T) {
	dir := t.TempDir()
	m := NewModule(Config{DataDir: dir})
	if err := m.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.updateSetting("emby_url", "http://emby:8096"); err != nil {
		t.Fatal(err)
	}
	if err := m.updateSetting("emby_sse_secret", "secret"); err != nil {
		t.Fatal(err)
	}
	m2 := NewModule(Config{DataDir: dir})
	if err := m2.loadDurable(); err != nil {
		t.Fatal(err)
	}
	if m2.baseURL != "http://emby:8096" || m2.sseSecret != "secret" {
		t.Fatalf("loaded url=%q secret=%q", m2.baseURL, m2.sseSecret)
	}
}

func TestTerminateSession(t *testing.T) {
	var stopped string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/Sessions/s1/Playing/Stop" {
			stopped = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	m := NewModule(Config{BaseURL: srv.URL, Token: "tok", GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0", DataDir: t.TempDir()})
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	resp, err := m.TerminateSession(ctx, &embyv1.TerminateSessionRequest{SessionId: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetOk() || stopped == "" {
		t.Fatalf("resp=%+v stopped=%q", resp, stopped)
	}
}
