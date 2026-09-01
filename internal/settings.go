package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/Muxcore-Media/core/pkg/contracts"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	embyv1 "github.com/Muxcore-Media/emby/proto/embyv1"
)

func (m *Module) Settings() []contracts.SettingDef {
	return m.settingsDefs()
}

func (m *Module) UpdateSetting(key, value string) error {
	return m.updateSetting(key, value)
}

func (m *Module) settingsDefs() []contracts.SettingDef {
	m.mu.RLock()
	baseURL := m.baseURL
	token := m.token
	secret := m.sseSecret
	poll := m.sessionsPollSec
	ws := m.websocketEnabled
	catalog := m.catalogSyncSec
	m.mu.RUnlock()
	masked := modulesdk.MaskSecret(token)
	wsVal := "0"
	if ws {
		wsVal = "1"
	}
	if catalog <= 0 {
		catalog = 6 * 3600
	}
	return []contracts.SettingDef{
		{
			Key:         "emby_url",
			Label:       "Emby Server URL",
			Type:        contracts.SettingTypeString,
			Value:       baseURL,
			Description: "Emby server base URL (EMBY_URL)",
			Required:    true,
			Group:       "Connection",
		},
		{
			Key:         "emby_token",
			Label:       "Emby API Token",
			Type:        contracts.SettingTypeSecret,
			Value:       masked,
			Description: "X-Emby-Token (EMBY_TOKEN)",
			Required:    true,
			Group:       "Connection",
		},
		{
			Key:         "emby_sse_secret",
			Label:       "SSE Ingest Secret",
			Type:        contracts.SettingTypeSecret,
			Value:       modulesdk.MaskSecret(secret),
			Description: "Required for POST /emby/sse/events (X-Emby-SSE-Secret or Authorization: Bearer)",
			Required:    false,
			Group:       "Security",
		},
		{
			Key:         "sessions_poll_seconds",
			Label:       "Sessions Poll Interval",
			Type:        contracts.SettingTypeString,
			Value:       fmt.Sprintf("%d", poll),
			Description: "Poll /Sessions interval in seconds",
			Required:    false,
			Group:       "Playback",
		},
		{
			Key:         "emby_websocket",
			Label:       "WebSocket Sessions",
			Type:        contracts.SettingTypeString,
			Value:       wsVal,
			Description: "1 connects outbound to /embywebsocket (EMBY_WEBSOCKET)",
			Required:    false,
			Group:       "Playback",
		},
		{
			Key:         "emby_catalog_sync_sec",
			Label:       "Library Catalog Sync Interval",
			Type:        contracts.SettingTypeString,
			Value:       fmt.Sprintf("%d", catalog),
			Description: "Full library catalog sync interval in seconds (default 21600)",
			Required:    false,
			Group:       "Library",
		},
	}
}

func (m *Module) updateSetting(key, value string) error {
	switch key {
	case "emby_url", "EMBY_URL":
		m.mu.Lock()
		m.baseURL = trimSlash(value)
		m.mu.Unlock()
	case "emby_token", "EMBY_TOKEN":
		if value == "********" {
			return nil
		}
		m.mu.Lock()
		m.token = strings.TrimSpace(value)
		m.mu.Unlock()
	case "emby_sse_secret", "EMBY_SSE_SECRET":
		if value == "********" {
			return nil
		}
		m.mu.Lock()
		m.sseSecret = strings.TrimSpace(value)
		m.mu.Unlock()
	case "sessions_poll_seconds", "EMBY_SESSIONS_POLL_SEC":
		if n, err := parseInt(value); err == nil && n > 0 {
			m.mu.Lock()
			m.sessionsPollSec = n
			m.mu.Unlock()
		}
	case "emby_websocket", "EMBY_WEBSOCKET":
		m.mu.Lock()
		m.websocketEnabled = envTruthy(value)
		m.mu.Unlock()
	case "emby_catalog_sync_sec", "EMBY_CATALOG_SYNC_SEC":
		if n, err := parseInt(value); err == nil && n > 0 {
			m.mu.Lock()
			m.catalogSyncSec = n
			m.mu.Unlock()
		}
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	return m.persistDurable()
}

func (m *Module) Status(ctx context.Context, _ *embyv1.StatusRequest) (*embyv1.StatusResponse, error) {
	m.mu.RLock()
	base := m.baseURL
	active := int32(m.lastActive) //nolint:gosec // active session count from Emby fits int32 status field
	m.mu.RUnlock()
	return &embyv1.StatusResponse{
		Configured:     m.configured(),
		BaseUrl:        base,
		ActiveSessions: active,
	}, nil
}

func (m *Module) TerminateSession(ctx context.Context, req *embyv1.TerminateSessionRequest) (*embyv1.TerminateSessionResponse, error) {
	if req.GetSessionId() == "" {
		return &embyv1.TerminateSessionResponse{Ok: false, Error: "session_id required"}, nil
	}
	path := fmt.Sprintf("/Sessions/%s/Playing/Stop", req.GetSessionId())
	if err := m.embyPOST(ctx, path); err != nil {
		return &embyv1.TerminateSessionResponse{Ok: false, Error: err.Error()}, nil //nolint:nilerr // application-level failure encoded in response
	}
	return &embyv1.TerminateSessionResponse{Ok: true}, nil
}
