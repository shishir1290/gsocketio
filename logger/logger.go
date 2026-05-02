// Package logger provides a levelled logger for gsocketio.
// FIX L-01: log level can be changed at runtime via GSOCKETIO_LOG_LEVEL env var.
//   Accepted values: DEBUG, INFO, WARN, ERROR, SILENT
package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
)

// Level controls which messages are emitted.
type Level int32

const (
	LevelDebug  Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelSilent
)

var (
	// current holds the active log level as an atomic int32.
	current int32 = int32(LevelInfo)

	std = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)
)

func init() {
	// FIX L-01: honour GSOCKETIO_LOG_LEVEL at startup.
	if val := os.Getenv("GSOCKETIO_LOG_LEVEL"); val != "" {
		if lvl, ok := parseLevel(val); ok {
			atomic.StoreInt32(&current, int32(lvl))
		}
	}
}

// parseLevel converts a string to a Level.
func parseLevel(s string) (Level, bool) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return LevelDebug, true
	case "INFO":
		return LevelInfo, true
	case "WARN", "WARNING":
		return LevelWarn, true
	case "ERROR":
		return LevelError, true
	case "SILENT", "OFF", "NONE":
		return LevelSilent, true
	}
	return LevelInfo, false
}

// SetLevel changes the global log level at runtime.
func SetLevel(l Level) { atomic.StoreInt32(&current, int32(l)) }

// GetLevel returns the current log level.
func GetLevel() Level { return Level(atomic.LoadInt32(&current)) }

func emit(level Level, tag, msg string) {
	if level < Level(atomic.LoadInt32(&current)) {
		return
	}
	std.Output(3, tag+msg) //nolint:errcheck
}

// Debug logs a debug-level message.
func Debug(format string, args ...interface{}) {
	if LevelDebug < Level(atomic.LoadInt32(&current)) {
		return
	}
	emit(LevelDebug, "[DEBUG] ", fmt.Sprintf(format, args...))
}

// Info logs an info-level message.
func Info(format string, args ...interface{}) {
	emit(LevelInfo, "[INFO]  ", fmt.Sprintf(format, args...))
}

// Warn logs a warning-level message.
func Warn(format string, args ...interface{}) {
	emit(LevelWarn, "[WARN]  ", fmt.Sprintf(format, args...))
}

// Error logs an error-level message.
func Error(format string, args ...interface{}) {
	emit(LevelError, "[ERROR] ", fmt.Sprintf(format, args...))
}
