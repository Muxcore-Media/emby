package internal

import "testing"

func TestExternalIDsFromProviderMap(t *testing.T) {
	imdb, tmdb, tvdb := externalIDsFromProviderMap(map[string]string{
		"Imdb": "123",
		"Tmdb": "42",
		"Tvdb": "7",
	})
	if imdb != "tt123" || tmdb != 42 || tvdb != 7 {
		t.Fatalf("ids: imdb=%q tmdb=%d tvdb=%d", imdb, tmdb, tvdb)
	}
}
