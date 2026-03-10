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
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/pbnjay/memory"

	"github.com/prometheus/prometheus/config"
)

// MemoryLimiter is a component that dictates if a target scrape should be aborted
// due to the Prometheus server exceeding a memory threshold.
type MemoryLimiter interface {
	// TargetScrapeAllowed returns true if the memory limits are not exceeded, and therefore
	// the target's scrape should not be aborted.
	TargetScrapeAllowed() bool
}

// scrapeMemoryLimiter implements MemoryLimiter based on configuration.
type scrapeMemoryLimiter struct {
	logger *slog.Logger
	config *config.ScrapeMemoryLimiterConfig

	// mu protects config and cached state.
	mu sync.RWMutex

	lastCheck   time.Time
	isOverLimit bool

	// Functions for reading memory stats, overrideable for testing.
	readMemStats func(*runtime.MemStats)
	totalMemory  func() uint64
}

func newScrapeMemoryLimiter(cfg *config.ScrapeMemoryLimiterConfig, logger *slog.Logger) *scrapeMemoryLimiter {
	return &scrapeMemoryLimiter{
		logger:       logger,
		config:       cfg,
		readMemStats: runtime.ReadMemStats,
		totalMemory:  memory.TotalMemory,
	}
}

func (l *scrapeMemoryLimiter) ApplyConfig(cfg *config.ScrapeMemoryLimiterConfig) {
	l.mu.Lock()
	l.config = cfg
	l.mu.Unlock()
}

func (l *scrapeMemoryLimiter) TargetScrapeAllowed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.config == nil {
		return true
	}

	now := time.Now()
	if time.Duration(l.config.CheckInterval) != 0 && now.Sub(l.lastCheck) < time.Duration(l.config.CheckInterval) {
		return !l.isOverLimit
	}

	var m runtime.MemStats
	l.readMemStats(&m)

	l.lastCheck = now
	l.isOverLimit = false

	if l.config.LimitMiB > 0 {
		allocMiB := float64(m.Alloc) / 1024 / 1024
		if allocMiB >= float64(l.config.LimitMiB) {
			l.isOverLimit = true
			return false
		}
	}

	if l.config.LimitPercentage > 0 {
		totalMem := l.totalMemory()
		if totalMem > 0 {
			allocPercentage := (float64(m.Alloc) / float64(totalMem)) * 100.0
			if math.Round(allocPercentage) >= float64(l.config.LimitPercentage) {
				l.isOverLimit = true
				return false
			}
		}
	}

	return true
}
