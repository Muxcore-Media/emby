package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type embyItem struct { //nolint:govet // field order matches Emby API JSON grouping
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	Type         string            `json:"Type"`
	Path         string            `json:"Path"`
	Size         int64             `json:"Size"`
	ParentId     string            `json:"ParentId"`
	Width        int               `json:"Width"`
	Height       int               `json:"Height"`
	RunTimeTicks int64             `json:"RunTimeTicks"`
	ProviderIds  map[string]string `json:"ProviderIds"`
}

type embyVirtualFolder struct {
	Name   string `json:"Name"`
	ItemId string `json:"ItemId"`
}

type embySession struct {
	NowPlayingItem *embyItem `json:"NowPlayingItem"`
	PlayState      *struct {
		PlayMethod    string `json:"PlayMethod"`
		PositionTicks int64  `json:"PositionTicks"`
		IsPaused      bool   `json:"IsPaused"`
	} `json:"PlayState"`
	TranscodingInfo *struct {
		Width  int `json:"Width"`
		Height int `json:"Height"`
	} `json:"TranscodingInfo"`
	Id             string `json:"Id"`
	UserId         string `json:"UserId"`
	UserName       string `json:"UserName"`
	Client         string `json:"Client"`
	DeviceName     string `json:"DeviceName"`
	RemoteEndPoint string `json:"RemoteEndPoint"`
	AppName        string `json:"AppName"`
}

func (m *Module) embyGET(ctx context.Context, path string) ([]byte, int, error) {
	m.mu.RLock()
	base, token := m.baseURL, m.token
	m.mu.RUnlock()
	if base == "" || token == "" {
		return nil, 0, fmt.Errorf("emby not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, http.NoBody)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Emby-Token", token)
	resp, err := m.httpCli.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func (m *Module) embyPOST(ctx context.Context, path string) error {
	m.mu.RLock()
	base, token := m.baseURL, m.token
	m.mu.RUnlock()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set("X-Emby-Token", token)
	resp, err := m.httpCli.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("emby POST %s status %d", path, resp.StatusCode)
	}
	return nil
}

func (m *Module) probeSystemInfo(ctx context.Context) error {
	body, code, err := m.embyGET(ctx, "/System/Info")
	if err != nil {
		return err
	}
	if code >= 300 {
		return fmt.Errorf("emby /System/Info status %d", code)
	}
	var info map[string]any
	if err := json.Unmarshal(body, &info); err != nil {
		return fmt.Errorf("emby /System/Info parse: %w", err)
	}
	return nil
}

func (m *Module) listSessions(ctx context.Context) ([]embySession, error) {
	q := url.Values{}
	q.Set("Fields", "Path,Width,Height,RunTimeTicks")
	body, code, err := m.embyGET(ctx, "/Sessions?"+q.Encode())
	if err != nil {
		return nil, err
	}
	if code >= 300 {
		return nil, fmt.Errorf("emby /Sessions status %d", code)
	}
	var sessions []embySession
	if err := json.Unmarshal(body, &sessions); err != nil {
		return nil, fmt.Errorf("emby sessions parse: %w", err)
	}
	return sessions, nil
}

func ticksToSeconds(ticks int64) int64 {
	if ticks <= 0 {
		return 0
	}
	return ticks / 10_000_000
}

func remoteIP(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if i := strings.LastIndex(endpoint, ":"); i > 0 {
		host := endpoint[:i]
		if strings.HasPrefix(host, "[") && strings.Contains(host, "]") {
			return strings.Trim(host, "[]")
		}
		return host
	}
	return endpoint
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

const embyItemsPageSize = 500

func (m *Module) listEmbyItemsForFolder(ctx context.Context, parentID string) ([]embyItem, error) {
	var all []embyItem
	start := 0
	for {
		q := url.Values{}
		if parentID != "" {
			q.Set("ParentId", parentID)
		}
		q.Set("Recursive", "true")
		q.Set("IncludeItemTypes", "Movie,Series,Episode")
		q.Set("Fields", "Path,Size,ParentId,Width,Height,ProviderIds,RunTimeTicks")
		q.Set("StartIndex", fmt.Sprintf("%d", start))
		q.Set("Limit", fmt.Sprintf("%d", embyItemsPageSize))
		body, code, err := m.embyGET(ctx, "/Items?"+q.Encode())
		if err != nil {
			return nil, err
		}
		if code >= 300 {
			return nil, fmt.Errorf("emby /Items status %d", code)
		}
		var raw struct {
			Items []embyItem `json:"Items"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil, err
		}
		if len(raw.Items) == 0 {
			break
		}
		all = append(all, raw.Items...)
		if len(raw.Items) < embyItemsPageSize {
			break
		}
		start += len(raw.Items)
	}
	return all, nil
}

func (m *Module) listEmbyVirtualFolders(ctx context.Context) ([]embyVirtualFolder, error) {
	body, code, err := m.embyGET(ctx, "/Library/VirtualFolders")
	if err != nil {
		return nil, err
	}
	if code >= 300 {
		return nil, fmt.Errorf("emby /Library/VirtualFolders status %d", code)
	}
	var folders []embyVirtualFolder
	if err := json.Unmarshal(body, &folders); err != nil {
		return nil, err
	}
	return folders, nil
}
