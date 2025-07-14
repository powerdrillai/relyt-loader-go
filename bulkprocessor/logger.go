package bulkprocessor

import (
	"fmt"
	"log"
	"os"
)

// LogLevel represents the logging level
type LogLevel int

const (
	DEFAULT LogLevel = iota
	DEBUG
	LOG
	WARNING
	ERROR
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case DEFAULT:
		return "DEFAULT"
	case DEBUG:
		return "DEBUG"
	case LOG:
		return "LOG"
	case WARNING:
		return "WARNING"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger represents a logger instance
type Logger struct {
	level  LogLevel
	logger *log.Logger
}

// Global logger instance
var globalLogger = &Logger{
	level:  LOG,
	logger: log.New(os.Stdout, "", log.LstdFlags),
}

// SetLogLevel sets the global log level
func SetLogLevel(level LogLevel) {
	globalLogger.level = level
}

func Debug(format string, args ...interface{}) {
	logMessage(DEBUG, format, args...)
}

// Log logs a message at LOG level
func Log(format string, args ...interface{}) {
	logMessage(LOG, format, args...)
}

// Warning logs a message at WARNING level
func Warning(format string, args ...interface{}) {
	logMessage(WARNING, format, args...)
}

// Error logs a message at ERROR level
func Error(format string, args ...interface{}) {
	logMessage(ERROR, format, args...)
}

// logMessage logs a message if the level is enabled
func logMessage(level LogLevel, format string, args ...interface{}) {
	currentLevel := globalLogger.level
	logger := globalLogger.logger

	if level < currentLevel {
		return
	}

	message := fmt.Sprintf(format, args...)
	logger.Printf("[%s] %s", level.String(), message)
}

// Logf is an alias for Log for consistency with standard log package
func Logf(format string, args ...interface{}) {
	Log(format, args...)
}

// Warningf is an alias for Warning for consistency with standard log package
func Warningf(format string, args ...interface{}) {
	Warning(format, args...)
}

// Errorf is an alias for Error for consistency with standard log package
func Errorf(format string, args ...interface{}) {
	Error(format, args...)
}
