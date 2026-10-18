package config

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger builds the process-wide JSON logger according to the configured
// log level. Unrecognized levels fall back to INFO.
func (c Config) NewLogger() *slog.Logger {
	var level slog.LevelVar
	level.Set(slog.LevelInfo)

	switch strings.ToUpper(c.Log.Level) {
	case "DEBUG":
		level.Set(slog.LevelDebug)
	case "WARN":
		level.Set(slog.LevelWarn)
	case "ERROR":
		level.Set(slog.LevelError)
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &level}))
}
