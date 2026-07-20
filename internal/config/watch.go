package config

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/fsnotify/fsnotify"
	yaml "github.com/goccy/go-yaml"
	"go.uber.org/zap"
)

func (p *Provider) StartWatch(ctx context.Context, path string, logger *zap.Logger) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	err = watcher.Add(path)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			// интересуют Write / Create / Rename
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				p.reload(path, logger)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			logger.Error("failed to watch config", zap.Error(err))
		}
	}
}

func (p *Provider) reload(path string, logger *zap.Logger) {
	cfg, err := LoadByPath(path)
	if err != nil {
		logger.Error("failed to load config", zap.Error(err))
		return
	}
	loadEnv(cfg)
	if err := cfg.validateEnv(); err != nil {
		logger.Error("failed to validate config", zap.Error(err))
		return
	}
	p.Swap(cfg)
	logger.Info("config reloaded", zap.String("mode", cfg.ProcessingMode.Mode))
}

func LoadByPath(path string) (*Config, error) {

	configFile, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("config file does not exist")
		}
		return nil, fmt.Errorf("failed to read config: %w", err)

	}
	var cfg Config

	if err := yaml.Unmarshal(configFile, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}
