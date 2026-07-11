package logger

import (
	"log"

	"go.uber.org/zap"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func MustNew(env string) *zap.Logger {

	var (
		logger *zap.Logger
		err    error
	)

	switch env {
	case envLocal:
		logger, err = zap.NewDevelopment()

	case envProd:
		logger, err = zap.NewProduction()
	default: // If env config is invalid, set prod settings by default due to security
		logger = zap.NewExample()

	}

	if err != nil {
		log.Panicf("initialize logger error: %v", err)
	}

	return logger
}