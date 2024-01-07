package main

import (
	"KV-Project/logger"
	"go.uber.org/zap"
)

func main() {
	logger.Info("hello world")
	str := "hello world"
	logger.Info("hello world", zap.String("str", str), zap.Int("int", 1))
	logger.Debug("hello world")
}
