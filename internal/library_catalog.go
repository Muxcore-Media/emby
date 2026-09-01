package internal

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	playbackv1 "github.com/Muxcore-Media/playback-contract/proto/playbackv1"
)

type libraryCatalogPayload struct {
	Action          string `json:"action"`
	ServerID        string `json:"server_id"`
	ServerType      string `json:"server_type"`
	ItemID          string `json:"item_id"`
	MediaType       string `json:"media_type,omitempty"`
	MuxcoreID       string `json:"muxcore_id,omitempty"`
	Title           string `json:"title,omitempty"`
	MediaPath       string `json:"media_path,omitempty"`
	LibraryName     string `json:"library_name,omitempty"`
	VideoResolution string `json:"video_resolution,omitempty"`
	ParentID        string `json:"parent_id,omitempty"`
	FileSizeBytes   int64  `json:"file_size_bytes,omitempty"`
	ImdbID          string `json:"imdb_id,omitempty"`
	TmdbID          int64  `json:"tmdb_id,omitempty"`
	TvdbID          int64  `json:"tvdb_id,omitempty"`
}

func (m *Module) publishLibraryCatalogEvent(ctx context.Context, action string, item libraryCatalogPayload) {
	item.Action = action
	item.ServerID = m.id
	item.ServerType = "emby"
	if item.ItemID == "" {
		return
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return
	}
	if err := m.publishEvent(ctx, playbackevents.EventPlaybackLibraryItem, payload); err != nil {
		_ = err
	}
}

func (m *Module) publishCatalogFromEmbyItem(ctx context.Context, libraryName string, it embyItem) {
	if it.ID == "" {
		return
	}
	imdb, tmdb, tvdb := externalIDsFromProviderMap(it.ProviderIds)
	m.publishLibraryCatalogEvent(ctx, "upsert", libraryCatalogPayload{
		ItemID:          it.ID,
		MediaType:       it.Type,
		Title:           it.Name,
		MediaPath:       it.Path,
		LibraryName:     libraryName,
		FileSizeBytes:   it.Size,
		MuxcoreID:       "emby:" + it.ID,
		VideoResolution: playbackv1.NormalizeStreamResolution(it.Height, it.Width, ""),
		ParentID:        it.ParentId,
		ImdbID:          imdb,
		TmdbID:          tmdb,
		TvdbID:          tvdb,
	})
}

func (m *Module) syncLibraryCatalog(ctx context.Context) (int, error) {
	if !m.configured() {
		return 0, nil
	}
	folders, err := m.listEmbyVirtualFolders(ctx)
	if err != nil {
		return 0, err
	}
	published := 0
	if len(folders) == 0 {
		items, err := m.listEmbyItemsForFolder(ctx, "")
		if err != nil {
			return 0, err
		}
		for _, it := range items {
			m.publishCatalogFromEmbyItem(ctx, "", it)
			published++
		}
		return published, nil
	}
	for _, folder := range folders {
		if folder.ItemId == "" {
			continue
		}
		items, err := m.listEmbyItemsForFolder(ctx, folder.ItemId)
		if err != nil {
			return published, err
		}
		for _, it := range items {
			m.publishCatalogFromEmbyItem(ctx, folder.Name, it)
			published++
		}
	}
	return published, nil
}

func (m *Module) catalogSyncIntervalSec() int {
	m.mu.RLock()
	sec := m.catalogSyncSec
	m.mu.RUnlock()
	if sec > 0 {
		return sec
	}
	return 6 * 3600
}

func (m *Module) catalogSyncLoop(ctx context.Context) {
	interval := time.Duration(m.catalogSyncIntervalSec()) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	run := func() {
		if !m.configured() {
			return
		}
		n, err := m.syncLibraryCatalog(ctx)
		if err != nil {
			slog.Debug("emby: library catalog sync failed", "error", err)
			return
		}
		if n > 0 {
			slog.Info("emby: library catalog synced", "items", n)
		}
	}
	run()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			run()
		}
	}
}

func (m *Module) handlePluginLibrarySSE(ctx context.Context, eventName, data string) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(data)), &raw); err != nil {
		return
	}
	action := "upsert"
	if strings.Contains(strings.ToLower(eventName), "removed") {
		action = "removed"
	}
	itemID := stringField(raw, "itemId", "item_id", "ItemId")
	if itemID == "" {
		return
	}
	m.publishLibraryCatalogEvent(ctx, action, libraryCatalogPayload{
		ItemID:        itemID,
		MediaType:     stringField(raw, "itemType", "item_type", "ItemType"),
		Title:         stringField(raw, "title", "Title", "Name"),
		MediaPath:     stringField(raw, "path", "Path", "mediaPath"),
		FileSizeBytes: int64Field(raw, "size", "Size", "file_size_bytes", "fileSizeBytes"),
		MuxcoreID:     "emby:" + itemID,
		ParentID:      stringField(raw, "parentId", "parent_id", "ParentId"),
		VideoResolution: playbackv1.NormalizeStreamResolution(
			int(int64Field(raw, "height", "Height", "video_height", "videoHeight")),
			int(int64Field(raw, "width", "Width", "video_width", "videoWidth")),
			stringField(raw, "video_resolution", "videoResolution"),
		),
	})
}
