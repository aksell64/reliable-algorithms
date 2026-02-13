package logger

import (
	"fmt"
	"os"
	"reliable/types"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func Disable() {
	logger = zerolog.Nop()
}

func init() {
	logger = newLogger()
}

func newLogger() zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "15:04:05",
		PartsOrder: []string{"time", "level", "message"},
	}).With().Timestamp().Logger()
}

func Logger() *zerolog.Logger {
	return &logger
}

func New() zerolog.Logger {
	return newLogger()
}

func NewNodeLogger(pid types.ProcessID) zerolog.Logger {
	return logger.With().
		Str("PID", pid.String()).
		Logger()
}

func Panicfmt(msg string, args ...any) {
	plog := newLogger()
	plog.Panic().Msg(fmt.Sprintf(msg, args...))
}

type Scope struct {
	Key string
	Val string
}

func NewNodeScopeLogger(pid types.ProcessID, scopes ...Scope) zerolog.Logger {
	l := NewNodeLogger(pid)

	for _, s := range scopes {
		l = l.With().Str(s.Key, s.Val).Logger()
	}
	return l
}
