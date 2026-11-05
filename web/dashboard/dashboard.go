// Package dashboard serves the built-in web dashboard: plain HTML, CSS and
// JavaScript embedded in the binary, with no build step and no external
// requests. It talks to the REST API (/v1/...) and /v1/ws.
package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var files embed.FS

// Handler serves the dashboard's static files (index.html at "/").
func Handler() http.Handler {
	sub, err := fs.Sub(files, "static")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServerFS(sub)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Content-Security-Policy", "default-src 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; style-src 'self'; script-src 'self'")
		h.Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})
}
