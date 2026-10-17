package httpapi

import (
	"log/slog"
	"net/http"
	"runtime/debug"
)

// RecoverMiddleware shields the server from panics in downstream handlers: a
// panic is logged together with its stack trace and converted into a 500
// response instead of crashing the process.
func RecoverMiddleware(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			// net/http uses ErrAbortHandler to silently abort a response;
			// preserve that behaviour rather than logging it as a panic.
			if rec == http.ErrAbortHandler {
				panic(rec)
			}

			log.Error("panic recovered",
				"err", rec,
				"method", r.Method,
				"path", r.URL.Path,
				"stack", string(debug.Stack()),
			)

			writeError(w, http.StatusInternalServerError, ErrInternal)
		}()

		next.ServeHTTP(w, r)
	})
}
