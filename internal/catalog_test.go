package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
)

func TestSyncLibraryCatalogPerFolder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/Library/VirtualFolders":
			_, _ = w.Write([]byte(`[{"Name":"TV","ItemId":"lib-tv"},{"Name":"Movies","ItemId":"lib-movies"}]`))
		case strings.HasPrefix(r.URL.Path, "/Items") && r.URL.Query().Get("ParentId") == "lib-tv":
			_, _ = w.Write([]byte(`{"Items":[{"Id":"ep-1","Name":"Pilot","Type":"Episode","ParentId":"season-1","ProviderIds":{"Imdb":"tt123","Tmdb":"99","Tvdb":"1001"}}]}`))
		case strings.HasPrefix(r.URL.Path, "/Items") && r.URL.Query().Get("ParentId") == "lib-movies":
			_, _ = w.Write([]byte(`{"Items":[{"Id":"mv-1","Name":"Film","Type":"Movie","Path":"/media/film.mkv"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var mu sync.Mutex
	var payloads []map[string]any
	testPublishHook = func(_ context.Context, eventType, _ string, payload []byte) error {
		if eventType != playbackevents.EventPlaybackLibraryItem {
			return nil
		}
		var raw map[string]any
		_ = json.Unmarshal(payload, &raw)
		mu.Lock()
		payloads = append(payloads, raw)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { testPublishHook = nil })

	m := NewModule(Config{BaseURL: srv.URL, Token: "tok", DataDir: t.TempDir()})
	n, err := m.syncLibraryCatalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("published %d items", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 2 {
		t.Fatalf("payloads: %d", len(payloads))
	}
	var ep map[string]any
	for _, p := range payloads {
		if p["item_id"] == "ep-1" {
			ep = p
		}
	}
	if ep == nil {
		t.Fatalf("missing episode payload: %+v", payloads)
	}
	if ep["library_name"] != "TV" {
		t.Fatalf("library_name: %v", ep["library_name"])
	}
	if ep["imdb_id"] != "tt123" {
		t.Fatalf("imdb_id: %v", ep["imdb_id"])
	}
}

func TestHandlePluginLibrarySSE(t *testing.T) {
	var mu sync.Mutex
	var lastAction, lastID string
	testPublishHook = func(_ context.Context, eventType, _ string, payload []byte) error {
		if eventType != playbackevents.EventPlaybackLibraryItem {
			return nil
		}
		var raw map[string]any
		_ = json.Unmarshal(payload, &raw)
		mu.Lock()
		lastAction, _ = raw["action"].(string)
		lastID, _ = raw["item_id"].(string)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { testPublishHook = nil })

	m := NewModule(Config{DataDir: t.TempDir()})
	m.handlePluginLibrarySSE(context.Background(), "library.item.added", `{"itemId":"x1","title":"Show","itemType":"Series"}`)
	mu.Lock()
	if lastAction != "upsert" || lastID != "x1" {
		t.Fatalf("upsert: action=%q id=%q", lastAction, lastID)
	}
	mu.Unlock()

	m.handlePluginLibrarySSE(context.Background(), "library.item.removed", `{"itemId":"x1"}`)
	mu.Lock()
	if lastAction != "removed" || lastID != "x1" {
		t.Fatalf("removed: action=%q id=%q", lastAction, lastID)
	}
	mu.Unlock()
}
