package internal

import (
	"context"
	"fmt"

	"github.com/Muxcore-Media/core/pkg/contracts"
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
	poll := m.sessionsPollSec
	m.mu.RUnlock()
	masked := ""
	if token != "" {
		masked = "••••"
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
			Type:        contracts.SettingTypeString,
			Value:       masked,
			Description: "X-Emby-Token (EMBY_TOKEN)",
			Required:    true,
			Group:       "Connection",
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
	}
}

func (m *Module) updateSetting(key, value string) error {
	switch key {
	case "emby_url", "EMBY_URL":
		m.mu.Lock()
		m.baseURL = trimSlash(value)
		m.mu.Unlock()
	case "emby_token", "EMBY_TOKEN":
		if value != "" && value != "••••" {
			m.mu.Lock()
			m.token = value
			m.mu.Unlock()
		}
	case "sessions_poll_seconds", "EMBY_SESSIONS_POLL_SEC":
		if n, err := parseInt(value); err == nil && n > 0 {
			m.mu.Lock()
			m.sessionsPollSec = n
			m.mu.Unlock()
		}
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	return nil
}

func (m *Module) Status(ctx context.Context, _ *embyv1.StatusRequest) (*embyv1.StatusResponse, error) {
	m.mu.RLock()
	base := m.baseURL
	active := int32(m.lastActive)
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
		return &embyv1.TerminateSessionResponse{Ok: false, Error: err.Error()}, nil
	}
	return &embyv1.TerminateSessionResponse{Ok: true}, nil
}
