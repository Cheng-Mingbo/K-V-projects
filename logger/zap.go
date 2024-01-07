package logger

import (
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	logger, _ = zap.NewDevelopment()
	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {

		}
	}(logger)
}

// Info 封装了 zap 的 Info 方法
func Info(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

// Error 封装了 zap 的 Error 方法
func Error(msg string, fields ...zap.Field) {
	logger.Error(msg, fields...)
}

// Debug 封装了 zap 的 Debug 方法
func Debug(msg string, fields ...zap.Field) {
	logger.Debug(msg, fields...)
}

// Warn 封装了 zap 的 Warn 方法
func Warn(msg string, fields ...zap.Field) {
	logger.Warn(msg, fields...)
}

// Fatal 封装了 zap 的 Fatal 方法
func Fatal(msg string, fields ...zap.Field) {
	logger.Fatal(msg, fields...)
}

// Panic 封装了 zap 的 Panic 方法
func Panic(msg string, fields ...zap.Field) {
	logger.Panic(msg, fields...)
}
