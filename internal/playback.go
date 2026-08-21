package internal

import (
	"context"
	"log/slog"
	"strings"
	"time"

	playbackevents "github.com/Muxcore-Media/contracts-playback/events"
	playbackv1 "github.com/Muxcore-Media/playback-contract/proto/playbackv1"
)

type playbackEventPayload struct {
	ItemID          string `json:"item_id"`
	EmbyItemID      string `json:"emby_item_id"`
	UserID          string `json:"user_id"`
	UserName        string `json:"user_name"`
	SessionID       string `json:"session_id"`
	PositionSeconds int64  `json:"position_seconds"`
	DurationSeconds int64  `json:"duration_seconds"`
	Title           string `json:"title"`
	MediaType       string `json:"media_type"`
	MediaPath       string `json:"media_path,omitempty"`
	ServerType      string `json:"server_type"`
	IsPaused        bool   `json:"is_paused,omitempty"`
	IsTranscode     bool   `json:"is_transcode,omitempty"`
	PlayMethod      string `json:"play_method,omitempty"`
	Platform        string `json:"platform,omitempty"`
	Device          string `json:"device,omitempty"`
	Player          string `json:"player,omitempty"`
	IPAddress       string `json:"ip_address,omitempty"`
	StreamResolution string `json:"stream_resolution,omitempty"`
	VideoHeight     int    `json:"video_height,omitempty"`
	VideoWidth      int    `json:"video_width,omitempty"`
}

func (m *Module) pollSessionsLoop() {
	for {
		m.mu.RLock()
		sec := m.sessionsPollSec
		m.mu.RUnlock()
		wait := time.Second
		if sec > 0 && m.configured() {
			wait = time.Duration(sec) * time.Second
			m.pollSessionsOnce()
		}
		select {
		case <-m.stopCh:
			return
		case <-time.After(wait):
		}
	}
}

func (m *Module) pollSessionsOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sessions, err := m.listSessions(ctx)
	if err != nil {
		slog.Debug("emby: sessions poll failed", "error", err)
		return
	}
	m.mu.Lock()
	m.lastActive = len(sessions)
	m.mu.Unlock()

	active := map[string]bool{}
	for _, s := range sessions {
		if s.NowPlayingItem == nil {
			continue
		}
		key := s.Id
		if key == "" {
			key = s.UserId + ":" + s.NowPlayingItem.ID
		}
		active[key] = true
		ev := sessionToEvent(s)
		m.mu.Lock()
		prev := m.sessionSeen[key]
		m.sessionSeen[key] = sessionState(s)
		m.mu.Unlock()
		if prev == "" {
			m.publishPlayback(ctx, playbackevents.EventPlaybackStarted, ev)
		} else {
			m.publishPlayback(ctx, playbackevents.EventPlaybackProgress, ev)
		}
	}
	m.mu.Lock()
	for key := range m.sessionSeen {
		if !active[key] {
			delete(m.sessionSeen, key)
			m.mu.Unlock()
			m.publishPlayback(ctx, playbackevents.EventPlaybackStopped, playbackEventPayload{
				SessionID:  key,
				ServerType: "emby",
			})
			m.mu.Lock()
		}
	}
	m.mu.Unlock()
}

func sessionState(s embySession) string {
	if s.PlayState != nil && s.PlayState.IsPaused {
		return "paused"
	}
	return "playing"
}

func sessionToEvent(s embySession) playbackEventPayload {
	item := s.NowPlayingItem
	ev := playbackEventPayload{
		ItemID:     item.ID,
		EmbyItemID: item.ID,
		UserID:     s.UserId,
		UserName:   s.UserName,
		SessionID:  s.Id,
		Title:      item.Name,
		MediaType:  item.Type,
		MediaPath:  item.Path,
		ServerType: "emby",
		Platform:   s.Client,
		Device:     s.DeviceName,
		Player:     firstNonEmpty(s.AppName, s.Client),
		IPAddress:  remoteIP(s.RemoteEndPoint),
	}
	if s.PlayState != nil {
		ev.PositionSeconds = ticksToSeconds(s.PlayState.PositionTicks)
		ev.IsPaused = s.PlayState.IsPaused
		ev.IsTranscode = strings.EqualFold(strings.TrimSpace(s.PlayState.PlayMethod), "Transcode") || s.TranscodingInfo != nil
		ev.PlayMethod = strings.TrimSpace(s.PlayState.PlayMethod)
	}
	ev.StreamResolution = streamResolutionFromEmbySession(s)
	return ev
}

func (m *Module) publishPlayback(ctx context.Context, eventType string, ev playbackEventPayload) {
	data, err := playbackv1.MarshalSessionEvent(m.sessionInputFromPayload(eventType, ev))
	if err != nil {
		return
	}
	if err := m.publishEvent(ctx, eventType, data); err != nil {
		slog.Debug("emby: publish playback failed", "type", eventType, "error", err)
	}
}

func (m *Module) sessionInputFromPayload(eventType string, ev playbackEventPayload) playbackv1.SessionInput {
	itemID := ev.ItemID
	if itemID == "" {
		itemID = ev.EmbyItemID
	}
	serverType := ev.ServerType
	if serverType == "" {
		serverType = "emby"
	}
	return playbackv1.SessionInput{
		EventType:         eventType,
		SourceModule:      m.id,
		ServerID:          m.id,
		ServerType:        serverType,
		ExternalSessionID: ev.SessionID,
		UserID:            ev.UserID,
		UserName:          ev.UserName,
		ItemID:            itemID,
		Title:             ev.Title,
		MediaType:         ev.MediaType,
		PositionSeconds:   ev.PositionSeconds,
		DurationSeconds:   ev.DurationSeconds,
		IsPaused:          ev.IsPaused,
		IsTranscode:       ev.IsTranscode,
		PlayMethod:        ev.PlayMethod,
		Platform:          ev.Platform,
		Device:            ev.Device,
		Player:            ev.Player,
		IPAddress:         ev.IPAddress,
		MediaPath:         ev.MediaPath,
		StreamResolution:  streamResolutionFromPayload(ev),
	}
}

func (m *Module) publishPlaybackEvent(ctx context.Context, eventType string, ev playbackEventPayload) {
	m.publishPlayback(ctx, eventType, ev)
}
