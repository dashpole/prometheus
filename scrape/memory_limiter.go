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
	"math/rand"
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
	TargetScrapeAllowed(hash uint64, lastScrapeSize int) bool
}

// scrapeMemoryLimiter implements MemoryLimiter based on configuration.
type scrapeMemoryLimiter struct {
	logger *slog.Logger
	config *config.ScrapeMemoryLimiterConfig

	// mu protects config and cached state.
	mu sync.RWMutex

	lastCheck       time.Time
	allocMiB        float64
	allocPercentage float64

	tokenBucket   *tokenBucketStrategy
	drr           *drrStrategy
	probabilistic *probabilisticStrategy

	maxScrapeSize    int
	consecutiveSkips map[uint64]int

	// Functions for reading memory stats, overrideable for testing.
	readMemStats func(*runtime.MemStats)
	totalMemory  func() uint64
	randFloat    func() float64
}

func newScrapeMemoryLimiter(cfg *config.ScrapeMemoryLimiterConfig, logger *slog.Logger) *scrapeMemoryLimiter {
	return &scrapeMemoryLimiter{
		logger:           logger,
		config:           cfg,
		readMemStats:     runtime.ReadMemStats,
		totalMemory:      memory.TotalMemory,
		randFloat:        rand.Float64,
		consecutiveSkips: make(map[uint64]int),
		tokenBucket:      &tokenBucketStrategy{},
		drr: &drrStrategy{
			targetLastQuantum: make(map[uint64]float64),
			targetDeficit:     make(map[uint64]float64),
		},
		probabilistic: &probabilisticStrategy{},
	}
}

func (l *scrapeMemoryLimiter) ApplyConfig(cfg *config.ScrapeMemoryLimiterConfig) {
	l.mu.Lock()
	l.config = cfg
	l.mu.Unlock()
}

func (l *scrapeMemoryLimiter) TargetScrapeAllowed(hash uint64, lastScrapeSize int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.config == nil {
		return true
	}

	if l.consecutiveSkips == nil {
		l.consecutiveSkips = make(map[uint64]int)
	}

	if lastScrapeSize > l.maxScrapeSize {
		l.maxScrapeSize = lastScrapeSize
	}

	now := time.Now()
	if time.Duration(l.config.CheckInterval) == 0 || now.Sub(l.lastCheck) >= time.Duration(l.config.CheckInterval) {
		var m runtime.MemStats
		l.readMemStats(&m)

		l.lastCheck = now
		l.allocMiB = float64(m.Alloc) / 1024 / 1024

		totalMem := l.totalMemory()
		if totalMem > 0 {
			l.allocPercentage = (float64(m.Alloc) / float64(totalMem)) * 100.0
		} else {
			l.allocPercentage = 0
		}
	}

	dropReqMiB := float64(0)
	if l.config.LimitMiB > 0 {
		softLimit := float64(l.config.LimitMiB - l.config.SpikeLimitMiB)
		hardLimit := float64(l.config.LimitMiB)
		if l.allocMiB >= hardLimit {
			dropReqMiB = 1.0
		} else if l.allocMiB > softLimit {
			dropReqMiB = (l.allocMiB - softLimit) / (hardLimit - softLimit)
		}
	}

	dropReqPct := float64(0)
	if l.config.LimitPercentage > 0 {
		softLimit := float64(l.config.LimitPercentage - l.config.SpikeLimitPercentage)
		hardLimit := float64(l.config.LimitPercentage)
		if l.allocPercentage >= hardLimit {
			dropReqPct = 1.0
		} else if l.allocPercentage > softLimit {
			dropReqPct = (l.allocPercentage - softLimit) / (hardLimit - softLimit)
		}
	}

	pressure := dropReqMiB
	if dropReqPct > pressure {
		pressure = dropReqPct
	}

	if pressure <= 0 {
		l.consecutiveSkips[hash] = 0
		return true
	}

	strategy := l.config.Strategy
	if strategy == "" {
		strategy = "probabilistic"
	}

	if strategy == "token_bucket" {
		return l.tokenBucket.targetScrapeAllowed(hash, pressure, lastScrapeSize, now, l.config, l.consecutiveSkips)
	}

	if strategy == "deficit_round_robin" {
		return l.drr.targetScrapeAllowed(hash, pressure, lastScrapeSize, now, l.config, l.consecutiveSkips)
	}

	return l.probabilistic.targetScrapeAllowed(hash, pressure, lastScrapeSize, l.maxScrapeSize, l.randFloat, l.consecutiveSkips)
}

// tokenBucketStrategy implements Strategy B: Token Bucket (Cost-Based)
type tokenBucketStrategy struct {
	lastTokenUpdate time.Time
	tokens          float64
}

func (s *tokenBucketStrategy) targetScrapeAllowed(hash uint64, pressure float64, lastScrapeSize int, now time.Time, config *config.ScrapeMemoryLimiterConfig, consecutiveSkips map[uint64]int) bool {
	// Initialize maxTokens to 10% of limits as a reasonable bucket size
	maxTokens := float64(50 * 1024 * 1024)
	if config.LimitMiB > 0 {
		maxTokens = float64(config.LimitMiB) * 1024 * 102.4 // 10% of MiB limit
	}

	if s.lastTokenUpdate.IsZero() {
		s.lastTokenUpdate = now
		s.tokens = maxTokens
	}

	elapsed := now.Sub(s.lastTokenUpdate).Seconds()
	if elapsed > 0 {
		rate := maxTokens * (1.0 - pressure)
		s.tokens += rate * elapsed
		if s.tokens > maxTokens {
			s.tokens = maxTokens
		}
		s.lastTokenUpdate = now
	}

	cost := float64(lastScrapeSize)
	if cost == 0 {
		cost = 1000 // default minimum cost
	}

	// IOU logic: If we have consecutive skips, we discount the cost by 50% per skip to effectively queue large targets
	discount := float64(1.0)
	if skips, ok := consecutiveSkips[hash]; ok {
		discount = math.Pow(0.5, float64(skips))
	}
	cost = cost * discount

	if pressure == 1.0 {
		consecutiveSkips[hash]++
		return false // Must drop to prevent OOM
	}

	if s.tokens >= cost {
		s.tokens -= cost
		consecutiveSkips[hash] = 0
		return true
	}

	consecutiveSkips[hash]++
	return false
}

// drrStrategy implements Strategy C: Deficit Round Robin
type drrStrategy struct {
	globalQuantum     float64
	lastUpdate        time.Time
	targetLastQuantum map[uint64]float64
	targetDeficit     map[uint64]float64
}

func (s *drrStrategy) targetScrapeAllowed(hash uint64, pressure float64, lastScrapeSize int, now time.Time, config *config.ScrapeMemoryLimiterConfig, consecutiveSkips map[uint64]int) bool {
	if s.lastUpdate.IsZero() {
		s.lastUpdate = now
	}

	elapsed := now.Sub(s.lastUpdate).Seconds()
	if elapsed > 0 {
		totalAllowedRate := float64(50 * 1024 * 1024)
		if config.LimitMiB > 0 {
			totalAllowedRate = float64(config.LimitMiB) * 1024 * 102.4 // 10% of limit
		}

		activeTargets := float64(len(s.targetDeficit))
		if activeTargets == 0 {
			activeTargets = 1.0
		}

		perTargetRate := (totalAllowedRate * (1.0 - pressure)) / activeTargets

		s.globalQuantum += perTargetRate * elapsed
		s.lastUpdate = now
	}

	if _, ok := s.targetLastQuantum[hash]; !ok {
		s.targetLastQuantum[hash] = s.globalQuantum
	}

	earnedQuantum := s.globalQuantum - s.targetLastQuantum[hash]
	s.targetLastQuantum[hash] = s.globalQuantum
	s.targetDeficit[hash] += earnedQuantum

	maxDeficit := float64(100 * 1024 * 1024) // 100MiB
	if config.LimitMiB > 0 {
		maxDeficit = float64(config.LimitMiB) * 1024 * 1024
	}
	if s.targetDeficit[hash] > maxDeficit {
		s.targetDeficit[hash] = maxDeficit
	}

	cost := float64(lastScrapeSize)
	if cost == 0 {
		cost = 1000
	}

	if pressure >= 1.0 {
		consecutiveSkips[hash]++
		return false
	}

	if s.targetDeficit[hash] >= cost {
		s.targetDeficit[hash] -= cost
		consecutiveSkips[hash] = 0
		return true
	}

	consecutiveSkips[hash]++
	return false
}

// probabilisticStrategy implements Strategy A: Probabilistic
type probabilisticStrategy struct{}

func (s *probabilisticStrategy) targetScrapeAllowed(hash uint64, pressure float64, lastScrapeSize, maxScrapeSize int, randFloat func() float64, consecutiveSkips map[uint64]int) bool {
	// Fairness formula: scale the drop probability by relative size of this scrape compared to max seen.
	// Smallest scrapes multiplied by 0.5, largest by 1.5. Cap at 1.0.
	sizeFactor := 1.0
	if maxScrapeSize > 0 {
		sizeFactor = float64(lastScrapeSize) / float64(maxScrapeSize)
	}

	// Exponential decay of drop probability based on starvation (consecutive skips)
	priorityMultiplier := 1.0
	if skips, ok := consecutiveSkips[hash]; ok {
		priorityMultiplier = math.Pow(0.5, float64(skips))
	}

	dropProb := pressure * (0.5 + sizeFactor) * priorityMultiplier
	if pressure >= 1.0 {
		dropProb = 1.0 // Force drop when hard limit is breached
	} else if dropProb > 1.0 {
		dropProb = 1.0
	}

	if dropProb == 1.0 || randFloat() < dropProb {
		consecutiveSkips[hash]++
		return false
	}

	consecutiveSkips[hash] = 0
	return true
}
