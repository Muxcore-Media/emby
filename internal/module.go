package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Muxcore-Media/core/sdk/go/module/meshtls"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/core/sdk/go/client"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	manifest "github.com/Muxcore-Media/emby"
	embyv1 "github.com/Muxcore-Media/emby/proto/embyv1"
)

type Module struct {
	embyv1.UnimplementedEmbyBridgeServiceServer
	httpLis          net.Listener
	grpcLis          net.Listener
	grpcSrv          *grpc.Server
	httpSrv          *http.Server
	runCancel        context.CancelFunc
	sessionSeen      map[string]string
	stopCh           chan struct{}
	meshWake         chan struct{}
	mc               *client.Client
	httpCli          *http.Client
	dataDir          string
	token            string
	httpAddr         string
	grpcAddr         string
	sseSecret        string
	baseURL          string
	id               string
	sessionsPollSec  int
	catalogSyncSec   int
	lastActive       int
	mu               sync.RWMutex
	wsMu             sync.RWMutex
	wsConnected      bool
	websocketEnabled bool
}

type Config struct {
	ID              string
	BaseURL         string
	Token           string
	SSESecret       string
	DataDir         string
	GRPCAddr        string
	HTTPAddr        string
	SessionsPollSec int
	CatalogSyncSec  int
	WebSocket       bool
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "emby"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9477"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8477"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/lib/muxcore-emby"
	}
	if cfg.SessionsPollSec <= 0 {
		cfg.SessionsPollSec = 30
	}
	if cfg.CatalogSyncSec <= 0 {
		cfg.CatalogSyncSec = 6 * 3600
	}
	cfg.WebSocket = envTruthyDefaultTrue(os.Getenv("EMBY_WEBSOCKET"))
	if v := os.Getenv("EMBY_URL"); v != "" {
		cfg.BaseURL = v
	}
	if v := os.Getenv("EMBY_TOKEN"); v != "" {
		cfg.Token = v
	}
	if v := os.Getenv("EMBY_SSE_SECRET"); v != "" {
		cfg.SSESecret = v
	}
	if v := os.Getenv("EMBY_SESSIONS_POLL_SEC"); v != "" {
		if n, err := parseInt(v); err == nil && n > 0 {
			cfg.SessionsPollSec = n
		}
	}
	if v := os.Getenv("EMBY_CATALOG_SYNC_SEC"); v != "" {
		if n, err := parseInt(v); err == nil && n > 0 {
			cfg.CatalogSyncSec = n
		}
	}
	if v := os.Getenv("EMBY_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("EMBY_GRPC_ADDR"); v != "" {
		cfg.GRPCAddr = v
	}
	if v := os.Getenv("EMBY_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	return &Module{
		id:               cfg.ID,
		baseURL:          trimSlash(cfg.BaseURL),
		token:            cfg.Token,
		sseSecret:        cfg.SSESecret,
		sessionsPollSec:  cfg.SessionsPollSec,
		catalogSyncSec:   cfg.CatalogSyncSec,
		websocketEnabled: cfg.WebSocket,
		dataDir:          cfg.DataDir,
		grpcAddr:         cfg.GRPCAddr,
		httpAddr:         cfg.HTTPAddr,
		stopCh:           make(chan struct{}),
		meshWake:         make(chan struct{}, 1),
		sessionSeen:      map[string]string{},
		httpCli:          newGuardedClient(20 * time.Second),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:          m.id,
		Name:        "Emby Playback Bridge",
		Version:     modulesdk.ManifestVersion(manifest.ManifestJSON),
		Roles:       []string{"playback"},
		Description: "Emby session poll and playback mesh events",
		Author:      "MuxCore",
		Capabilities: []string{
			"playback.emby",
			"playback",
			"settings",
		},
		MinCoreVersion: "0.5.0",
		HTTPAddr:       m.httpAddr,
	}
}

func (m *Module) Init(ctx context.Context) error {
	if err := os.MkdirAll(m.dataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir %s: %w", m.dataDir, err)
	}
	if err := m.loadDurable(); err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", m.grpcAddr, err)
	}
	m.grpcLis = lis
	httpLis, err := lc.Listen(ctx, "tcp", m.httpAddr)
	if err != nil {
		_ = lis.Close()
		return fmt.Errorf("listen HTTP %s: %w", m.httpAddr, err)
	}
	m.httpLis = httpLis
	slog.Info("emby bridge initialized", "grpc", m.grpcAddr, "http", m.httpAddr)
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	runCtx, runCancel := context.WithCancel(context.WithoutCancel(ctx))
	m.runCancel = runCancel
	srv, err := meshtls.NewServer()
	if err != nil {
		runCancel()
		return fmt.Errorf("gRPC mesh TLS: %w", err)
	}
	m.grpcSrv = srv
	embyv1.RegisterEmbyBridgeServiceServer(m.grpcSrv, m)
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)
	go func() {
		if err := m.grpcSrv.Serve(m.grpcLis); err != nil {
			slog.Error("emby gRPC error", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := m.Health(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, `{"status":"error","error":%q}`, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("POST /emby/sse/events", m.handleSSEEvent)
	m.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := m.httpSrv.Serve(m.httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("emby HTTP error", "error", err)
		}
	}()

	go m.connectCore()
	go m.wsLoop(runCtx)
	go m.pollSessionsLoop(runCtx)
	go m.catalogSyncLoop(runCtx)
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.runCancel != nil {
		m.runCancel()
	}
	if m.httpSrv != nil {
		_ = m.httpSrv.Shutdown(ctx)
	} else if m.httpLis != nil {
		_ = m.httpLis.Close()
	}
	m.mu.Lock()
	mc := m.mc
	m.mc = nil
	m.mu.Unlock()
	if mc != nil {
		_ = mc.Close()
	}
	slog.Info("emby bridge stopped")
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	if !m.configured() {
		return fmt.Errorf("emby not configured (EMBY_URL / EMBY_TOKEN)")
	}
	return m.probeSystemInfo(ctx)
}

func (m *Module) configured() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.baseURL != "" && m.token != ""
}

func (m *Module) connectCore() {
	addr := os.Getenv("MUXCORE_GRPC_ADDR")
	if addr == "" {
		return
	}
	var opts []client.Option
	if os.Getenv("MUXCORE_INSECURE_DISABLE_TLS") == "true" || os.Getenv("MUXCORE_GRPC_INSECURE") == "true" {
		opts = append(opts, client.WithInsecure())
	}
	backoff := time.Second
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}
		c, err := client.Dial(addr, opts...)
		if err != nil {
			slog.Warn("emby: dial core failed, retrying", "error", err, "backoff", backoff)
			select {
			case <-m.stopCh:
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		m.mu.Lock()
		if m.mc != nil {
			_ = m.mc.Close()
		}
		m.mc = c
		m.mu.Unlock()
		slog.Info("emby: connected to core mesh", "addr", addr)
		backoff = time.Second

		select {
		case <-m.stopCh:
			return
		case <-m.meshWake:
		}

		m.mu.Lock()
		if m.mc == c {
			_ = m.mc.Close()
			m.mc = nil
		}
		m.mu.Unlock()
	}
}

func (m *Module) dropMeshClient(c *client.Client) {
	m.mu.Lock()
	if m.mc == c {
		_ = m.mc.Close()
		m.mc = nil
	}
	m.mu.Unlock()
	select {
	case m.meshWake <- struct{}{}:
	default:
	}
}

func (m *Module) eventClient() *client.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mc
}

var testPublishHook func(ctx context.Context, eventType, source string, payload []byte) error

func (m *Module) publishEvent(ctx context.Context, eventType string, payload []byte) error {
	if testPublishHook != nil {
		return testPublishHook(ctx, eventType, m.id, payload)
	}
	mc := m.eventClient()
	if mc == nil {
		return nil
	}
	err := mc.Events.Publish(ctx, eventType, m.id, payload)
	if err != nil {
		m.dropMeshClient(mc)
	}
	return err
}

func envTruthyDefaultTrue(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return true
	}
	return envTruthy(v)
}

func trimSlash(s string) string {
	for len(s) > 1 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscan(s, &n)
	return n, err
}
