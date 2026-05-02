// Package logger provides a simple levelled logger for gsocketio.
// No external dependencies — uses only the standard library.
package logger

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"unsafe"
)

// Level controls which messages are emitted.
type Level int32

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelSilent
)

var (
	// current holds the active log level (atomic for lock-free reads).
	current int32 = int32(LevelInfo)

	std = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)
)

// SetLevel changes the global log level.
func SetLevel(l Level) {
	atomic.StoreInt32(&current, int32(l))
}

// GetLevel returns the current log level.
func GetLevel() Level {
	return Level(atomic.LoadInt32(&current))
}

// SetOutput redirects log output (useful for testing).
func SetOutput(l *log.Logger) {
	(*(**log.Logger)(unsafe.Pointer(&std))) = l
}

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
