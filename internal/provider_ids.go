package internal

import (
	"strconv"
	"strings"
)

func externalIDsFromProviderMap(ids map[string]string) (imdb string, tmdb, tvdb int64) {
	if len(ids) == 0 {
		return "", 0, 0
	}
	imdb = strings.TrimSpace(ids["Imdb"])
	if imdb == "" {
		imdb = strings.TrimSpace(ids["imdb"])
	}
	if imdb != "" && !strings.HasPrefix(strings.ToLower(imdb), "tt") {
		imdb = "tt" + imdb
	}
	tmdb = providerIDInt(ids, "Tmdb", "tmdb")
	tvdb = providerIDInt(ids, "Tvdb", "tvdb")
	return imdb, tmdb, tvdb
}

func providerIDInt(ids map[string]string, keys ...string) int64 {
	for _, k := range keys {
		raw := strings.TrimSpace(ids[k])
		if raw == "" {
			continue
		}
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return n
		}
	}
	return 0
}
