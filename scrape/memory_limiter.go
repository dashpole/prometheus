// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scrape

import (
	"log/slog"
	"sync"

	"github.com/prometheus/prometheus/config"
)

// MemoryLimiter is a component that dictates if a target scrape should be aborted
// due to the Prometheus server exceeding a memory threshold.
type MemoryLimiter interface {
	// TargetScrapeAllowed returns true if the memory limits are not exceeded, and therefore
	// the target's scrape should not be aborted.
	TargetScrapeAllowed(hash uint64, lastScrapeSize int) bool
}

// scrapeMemoryLimiter implements MemoryLimiter based on configuration.
type scrapeMemoryLimiter struct {
	logger *slog.Logger
	config *config.ScrapeMemoryLimiterConfig

	// mu protects config updates.
	mu sync.RWMutex
}

func newScrapeMemoryLimiter(cfg *config.ScrapeMemoryLimiterConfig, logger *slog.Logger) *scrapeMemoryLimiter {
	return &scrapeMemoryLimiter{
		logger: logger,
		config: cfg,
	}
}

func (l *scrapeMemoryLimiter) ApplyConfig(cfg *config.ScrapeMemoryLimiterConfig) {
	l.mu.Lock()
	l.config = cfg
	l.mu.Unlock()
}

func (l *scrapeMemoryLimiter) TargetScrapeAllowed(hash uint64, lastScrapeSize int) bool {
	// TODO: For now this is a no-op that always allows scrapes.
	// The tracking features will be implemented later.
	return true
}
