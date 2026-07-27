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

func (p *Provider) StartWatch(ctx context.Context, path string, logger *zap.Logger, onReload func(*Config)) error {
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
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				p.reload(path, logger, onReload)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			logger.Error("failed to watch config", zap.Error(err))
		}
	}
}

func (p *Provider) reload(path string, logger *zap.Logger, onReload func(*Config)) {
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

	old := p.Get()
	changes := configChanges(old, cfg)

	p.Swap(cfg)
	if onReload != nil {
		onReload(cfg)
	}

	if len(changes) == 0 {
		return
	}
	logger.Info("config reloaded", changes...)
}

func configChanges(old, new *Config) []zap.Field {
	if old == nil {
		return nil
	}

	var fields []zap.Field
	add := func(name string, from, to any) {
		fields = append(fields, zap.Any(name, map[string]any{"from": from, "to": to}))
	}

	if old.Env != new.Env {
		add("env", old.Env, new.Env)
	}
	if old.ProcessingMode.Mode != new.ProcessingMode.Mode {
		add("processing_mode", old.ProcessingMode.Mode, new.ProcessingMode.Mode)
	}
	if old.HttpServer.ReadTimeout != new.HttpServer.ReadTimeout {
		add("read_timeout", old.HttpServer.ReadTimeout.String(), new.HttpServer.ReadTimeout.String())
	}
	if old.HttpServer.WriteTimeout != new.HttpServer.WriteTimeout {
		add("write_timeout", old.HttpServer.WriteTimeout.String(), new.HttpServer.WriteTimeout.String())
	}
	if old.HttpServer.IdleTimeout != new.HttpServer.IdleTimeout {
		add("idle_timeout", old.HttpServer.IdleTimeout.String(), new.HttpServer.IdleTimeout.String())
	}
	if old.HttpServer.RequestTimeout != new.HttpServer.RequestTimeout {
		add("request_timeout", old.HttpServer.RequestTimeout.String(), new.HttpServer.RequestTimeout.String())
	}
	if old.HttpServer.GracefulShutdownTimeout != new.HttpServer.GracefulShutdownTimeout {
		add("graceful_shutdown_timeout", old.HttpServer.GracefulShutdownTimeout.String(), new.HttpServer.GracefulShutdownTimeout.String())
	}

	return fields
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
