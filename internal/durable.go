package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type durableSettings struct {
	BaseURL         string `json:"emby_url"`
	Token           string `json:"emby_token"`
	SSESecret       string `json:"emby_sse_secret"`
	SessionsPollSec int    `json:"sessions_poll_seconds"`
	WebSocket       *bool  `json:"emby_websocket,omitempty"`
	CatalogSyncSec  int    `json:"emby_catalog_sync_sec"`
}

func (m *Module) settingsPath() string {
	return filepath.Join(m.dataDir, "settings.json")
}

func (m *Module) loadDurable() error {
	path := m.settingsPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m.persistDurable()
		}
		return err
	}
	var s durableSettings
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.baseURL == "" && s.BaseURL != "" {
		m.baseURL = trimSlash(s.BaseURL)
	}
	if m.token == "" && s.Token != "" {
		m.token = s.Token
	}
	if m.sseSecret == "" && s.SSESecret != "" {
		m.sseSecret = s.SSESecret
	}
	if s.SessionsPollSec > 0 {
		m.sessionsPollSec = s.SessionsPollSec
	}
	if s.CatalogSyncSec > 0 {
		m.catalogSyncSec = s.CatalogSyncSec
	}
	if s.WebSocket != nil {
		m.websocketEnabled = *s.WebSocket
	}
	return nil
}

func (m *Module) persistDurable() error {
	m.mu.RLock()
	s := durableSettings{
		BaseURL:         m.baseURL,
		Token:           m.token,
		SSESecret:       m.sseSecret,
		SessionsPollSec: m.sessionsPollSec,
		WebSocket:       boolPtr(m.websocketEnabled),
		CatalogSyncSec:  m.catalogSyncSec,
	}
	m.mu.RUnlock()
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.settingsPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.settingsPath())
}

func boolPtr(v bool) *bool {
	return &v
}
