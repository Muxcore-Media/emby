package internal

import (
	"net/http"
	"strings"
	"time"

	"github.com/Muxcore-Media/core/sdk/go/module/netguard"
)

// embyGuardOptions is the Integration profile for the operator's Emby server.
// A household server lives on the LAN or loopback. Link-local, cloud metadata,
// and non-HTTP schemes stay refused.
func embyGuardOptions(timeout time.Duration) netguard.Options {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	return netguard.Options{
		AllowPrivate:  true,
		AllowLoopback: true,
		Timeout:       timeout,
	}
}

func newGuardedClient(timeout time.Duration) *http.Client {
	return netguard.NewClient(netguard.Integration, embyGuardOptions(timeout))
}

// guardOutboundURL checks raw before a request. An empty URL is left to the
// caller (unset configuration).
func guardOutboundURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return netguard.ValidateURL(raw, netguard.Integration, embyGuardOptions(20*time.Second))
}
