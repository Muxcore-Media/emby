package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func (m *Module) wsEnabled() bool {
	m.mu.RLock()
	enabled := m.websocketEnabled
	m.mu.RUnlock()
	return enabled
}

func (m *Module) wsLoop(ctx context.Context) {
	backoff := time.Second
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}
		if !m.wsEnabled() || !m.configured() {
			select {
			case <-m.stopCh:
				return
			case <-time.After(time.Second):
			}
			continue
		}
		err := m.runWSConnection(ctx)
		m.setWSConnected(false)
		if err != nil {
			slog.Debug("emby: websocket disconnected", "error", err)
		}
		select {
		case <-m.stopCh:
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func embyWSURL(base, token string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported emby url scheme %q", u.Scheme)
	}
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = path + "/embywebsocket"
	q := u.Query()
	q.Set("api_key", token)
	q.Set("deviceId", "muxcore-emby-bridge")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (m *Module) runWSConnection(ctx context.Context) error {
	m.mu.RLock()
	base, token := m.baseURL, m.token
	sec := m.sessionsPollSec
	m.mu.RUnlock()

	wsURL, err := embyWSURL(base, token)
	if err != nil {
		return err
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := dialer.Dial(wsURL, http.Header{"User-Agent": []string{"MuxCore-Emby-Bridge"}})
	if err != nil {
		return err
	}
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close() }()

	ms := sec * 1000
	if ms < 1000 {
		ms = 1500
	}
	startMsg := fmt.Sprintf(`{"MessageType":"SessionsStart","Data":"0,%d"}`, ms)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(startMsg)); err != nil {
		return err
	}
	m.setWSConnected(true)
	slog.Info("emby: websocket connected")

	readWait := time.Duration(sec*3) * time.Second
	if readWait < 90*time.Second {
		readWait = 90 * time.Second
	}

	for {
		select {
		case <-m.stopCh:
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"MessageType":"SessionsStop","Data":""}`))
			return nil
		default:
		}
		if err := conn.SetReadDeadline(time.Now().Add(readWait)); err != nil {
			return err
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		m.handleWSMessage(ctx, msg)
	}
}

type embyWSMessage struct {
	MessageType string          `json:"MessageType"`
	Data        json.RawMessage `json:"Data"`
}

func (m *Module) handleWSMessage(ctx context.Context, raw []byte) {
	var msg embyWSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	switch msg.MessageType {
	case "Sessions":
		var sessions []embySession
		if err := json.Unmarshal(msg.Data, &sessions); err != nil {
			slog.Debug("emby: websocket sessions parse failed", "error", err)
			return
		}
		m.processSessionsSnapshot(ctx, sessions)
	case "PlaybackStarted", "PlaybackStopped", "SessionEnded":
		// Sessions snapshot covers state; direct events are optional noise.
	case "Ping":
		// keep-alive
	default:
		if strings.HasSuffix(msg.MessageType, "Start") || strings.HasSuffix(msg.MessageType, "Stop") {
			return
		}
		slog.Debug("emby: websocket message", "type", msg.MessageType)
	}
}

func (m *Module) setWSConnected(v bool) {
	m.wsMu.Lock()
	m.wsConnected = v
	m.wsMu.Unlock()
}

func (m *Module) wsConnectedNow() bool {
	m.wsMu.RLock()
	defer m.wsMu.RUnlock()
	return m.wsConnected
}

func envTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
