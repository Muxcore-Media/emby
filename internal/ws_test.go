package internal

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
)

func TestEmbyWSURL(t *testing.T) {
	u, err := embyWSURL("http://emby:8096/emby", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if !stringsContains(u, "ws://emby:8096/emby/embywebsocket") || !stringsContains(u, "api_key=secret") {
		t.Fatalf("url: %s", u)
	}
}

func stringsContains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestEmbyWebSocketSessionsMessage(t *testing.T) {
	var mu sync.Mutex
	var events []string
	testPublishHook = func(_ context.Context, eventType, _ string, _ []byte) error {
		mu.Lock()
		events = append(events, eventType)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { testPublishHook = nil })

	m := NewModule(Config{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	sample := []embySession{{
		Id:       "sess-ws",
		UserId:   "u1",
		UserName: "alice",
		NowPlayingItem: &embyItem{ID: "item-1", Name: "Movie", Type: "Movie"},
		PlayState:      &struct {
			PositionTicks int64  `json:"PositionTicks"`
			IsPaused      bool   `json:"IsPaused"`
			PlayMethod    string `json:"PlayMethod"`
		}{PositionTicks: 600000000, PlayMethod: "DirectPlay"},
	}}
	data, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	msg := embyWSMessage{MessageType: "Sessions", Data: data}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	m.handleWSMessage(raw)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 || events[0] != playbackevents.EventPlaybackStarted {
		t.Fatalf("events: %v", events)
	}
}
