package config

import (
	"sync/atomic"
)

type Provider struct {
	cfg atomic.Pointer[Config]
}

func NewProvider(cfg *Config) *Provider {
	p := &Provider{}
	p.cfg.Store(cfg)
	return p
}

func (p *Provider) Get() *Config {
	return p.cfg.Load()
}

func (p *Provider) Swap(cfg *Config) {
	p.cfg.Store(cfg)
}
