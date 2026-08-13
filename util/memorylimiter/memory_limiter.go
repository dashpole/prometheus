// Copyright The Prometheus Authors
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

package memorylimiter

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"runtime/metrics"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/config"
)

// LimiterState represents the current memory pressure level.
type LimiterState int32

const (
	// StateOK indicates memory usage is below configured thresholds.
	StateOK LimiterState = iota
	// StateSoftLimit indicates memory usage has exceeded the soft limit threshold.
	StateSoftLimit
	// StateHardLimit indicates memory usage has exceeded the hard limit threshold.
	StateHardLimit
)

// String returns the string representation of LimiterState.
func (s LimiterState) String() string {
	switch s {
	case StateOK:
		return "ok"
	case StateSoftLimit:
		return "soft"
	case StateHardLimit:
		return "hard"
	default:
		return "unknown"
	}
}

// MemoryLimiter provides memory pressure monitoring and enforcement controls.
type MemoryLimiter interface {
	// State returns the current memory limiter state.
	State() LimiterState
	// AllowScrape returns true if target scraping is allowed.
	AllowScrape() bool
	// AllowOTLP returns true if OTLP write requests are allowed.
	AllowOTLP() bool
	// AllowRemoteWrite returns true if remote write requests are allowed.
	AllowRemoteWrite() bool
	// AllowRemoteRead returns true if remote read requests are allowed.
	AllowRemoteRead() bool
	// AllowFederation returns true if federation requests are allowed.
	AllowFederation() bool
	// AllowBlockCompaction returns true if block compaction is allowed.
	AllowBlockCompaction() bool
	// AllowRecordingRules returns true if recording rules evaluation is allowed.
	AllowRecordingRules() bool
	// ApplyConfig updates the memory limiter configuration.
	ApplyConfig(cfg *config.MemoryLimiterConfig) error
	// Start starts periodic memory monitoring.
	Start(ctx context.Context)
	// Stop stops memory monitoring.
	Stop()
}

// memoryStats holds runtime memory statistics used by MemoryLimiter.
type memoryStats struct {
	TotalBytes     uint64
	FreeBytes      uint64
	ReleasedBytes  uint64
	GOMEMLIMIT     uint64
	GCLimiterCycle uint64
}

// metricsReader is a function that retrieves current runtime memory statistics.
type metricsReader func() memoryStats

func defaultMetricsReader() memoryStats {
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/gomemlimit:bytes"},
		{Name: "/gc/limiter/last-enabled:gc-cycle"},
	}
	metrics.Read(samples)
	var stats memoryStats
	for _, s := range samples {
		if s.Value.Kind() != metrics.KindUint64 {
			continue
		}
		switch s.Name {
		case "/memory/classes/total:bytes":
			stats.TotalBytes = s.Value.Uint64()
		case "/memory/classes/heap/free:bytes":
			stats.FreeBytes = s.Value.Uint64()
		case "/memory/classes/heap/released:bytes":
			stats.ReleasedBytes = s.Value.Uint64()
		case "/gc/gomemlimit:bytes":
			stats.GOMEMLIMIT = s.Value.Uint64()
		case "/gc/limiter/last-enabled:gc-cycle":
			stats.GCLimiterCycle = s.Value.Uint64()
		}
	}
	return stats
}

// Manager implements MemoryLimiter by tracking runtime memory usage and enforcing limits.
type Manager struct {
	logger *slog.Logger

	mu     sync.RWMutex
	config *config.MemoryLimiterConfig

	state                atomic.Int32
	lastInUse            atomic.Uint64
	lastGCLimiterCycle   uint64
	gcLimiterInitialized bool
	lastCheckTime        time.Time

	metricsReader metricsReader
	now           func() time.Time

	metrics *memoryLimiterMetrics

	reloadCh chan struct{}
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewManager initializes and returns a new Manager.
func NewManager(cfg *config.MemoryLimiterConfig, logger *slog.Logger, reg prometheus.Registerer) (*Manager, error) {
	if cfg != nil {
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid memory limiter config: %w", err)
		}
	}

	if logger == nil {
		logger = slog.Default()
	}

	m := &Manager{
		logger:        logger.With("component", "memory limiter"),
		config:        cfg,
		metricsReader: defaultMetricsReader,
		now:           time.Now,
		metrics:       newMemoryLimiterMetrics(reg),
		reloadCh:      make(chan struct{}, 1),
	}

	return m, nil
}

// ApplyConfig updates the Manager configuration.
func (m *Manager) ApplyConfig(cfg *config.MemoryLimiterConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cfg != nil {
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid memory limiter config: %w", err)
		}
	}

	m.config = cfg

	select {
	case m.reloadCh <- struct{}{}:
	default:
	}

	return nil
}

// State returns the current LimiterState.
func (m *Manager) State() LimiterState {
	if m == nil {
		return StateOK
	}
	return LimiterState(m.state.Load())
}

// Start starts periodic memory monitoring in the background.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.lastCheckTime = m.now()
	m.wg.Add(1)
	m.mu.Unlock()

	go m.run(loopCtx)
}

// Stop stops the background memory monitoring loop.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *Manager) run(ctx context.Context) {
	defer m.wg.Done()

	interval := 100 * time.Millisecond
	m.mu.RLock()
	if m.config != nil && m.config.CheckInterval > 0 {
		interval = time.Duration(m.config.CheckInterval)
	}
	m.mu.RUnlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial evaluation.
	m.Evaluate()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.reloadCh:
			m.mu.RLock()
			newInterval := 100 * time.Millisecond
			if m.config != nil && m.config.CheckInterval > 0 {
				newInterval = time.Duration(m.config.CheckInterval)
			}
			m.mu.RUnlock()
			ticker.Reset(newInterval)
			m.Evaluate()
		case <-ticker.C:
			m.Evaluate()
		}
	}
}

// Evaluate reads the runtime metrics and updates the limiter state.
func (m *Manager) Evaluate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := m.metricsReader()
	totalBytes := stats.TotalBytes
	freeBytes := stats.FreeBytes
	releasedBytes := stats.ReleasedBytes
	gomemlimit := stats.GOMEMLIMIT
	gcLimiterCycle := stats.GCLimiterCycle

	if m.config == nil || gomemlimit == 0 || gomemlimit == math.MaxInt64 {
		m.lastCheckTime = m.now()
		oldState := LimiterState(m.state.Swap(int32(StateOK)))
		if oldState != StateOK {
			m.metrics.transitionsTotal.WithLabelValues(oldState.String(), StateOK.String()).Inc()
		}
		m.metrics.active.WithLabelValues("hard").Set(0)
		m.metrics.active.WithLabelValues("soft").Set(0)
		m.metrics.limitBytes.WithLabelValues("soft").Set(0)
		m.metrics.limitBytes.WithLabelValues("hard").Set(0)
		m.metrics.inUseBytes.Set(0)
		return
	}

	var inUse uint64
	if totalBytes > (freeBytes + releasedBytes) {
		inUse = totalBytes - (freeBytes + releasedBytes)
	}
	m.lastInUse.Store(inUse)

	now := m.now()
	elapsed := 0.0
	if !m.lastCheckTime.IsZero() {
		elapsed = now.Sub(m.lastCheckTime).Seconds()
	}
	m.lastCheckTime = now

	oldState := LimiterState(m.state.Load())
	if elapsed > 0 {
		switch oldState {
		case StateSoftLimit:
			m.metrics.engagedSecondsTotal.WithLabelValues("soft").Add(elapsed)
		case StateHardLimit:
			m.metrics.engagedSecondsTotal.WithLabelValues("soft").Add(elapsed)
			m.metrics.engagedSecondsTotal.WithLabelValues("hard").Add(elapsed)
		}
	}

	pressureRatio := float64(inUse) / float64(gomemlimit)

	gcLimiterActive := m.gcLimiterInitialized && gcLimiterCycle > m.lastGCLimiterCycle
	m.lastGCLimiterCycle = gcLimiterCycle
	m.gcLimiterInitialized = true

	var newState LimiterState
	switch {
	case pressureRatio >= m.config.HardLimitRatio || gcLimiterActive:
		newState = StateHardLimit
	case pressureRatio >= m.config.SoftLimitRatio:
		newState = StateSoftLimit
	default:
		newState = StateOK
	}

	if newState != oldState {
		m.state.Store(int32(newState))
		m.metrics.transitionsTotal.WithLabelValues(oldState.String(), newState.String()).Inc()
		m.logger.Debug("Memory limiter state transition",
			"from", oldState.String(),
			"to", newState.String(),
			"pressure_ratio", pressureRatio,
			"in_use_bytes", inUse,
			"gomemlimit", gomemlimit,
			"gc_limiter_active", gcLimiterActive,
		)
	}

	// Update gauges.
	switch newState {
	case StateHardLimit:
		m.metrics.active.WithLabelValues("hard").Set(1)
		m.metrics.active.WithLabelValues("soft").Set(1)
	case StateSoftLimit:
		m.metrics.active.WithLabelValues("hard").Set(0)
		m.metrics.active.WithLabelValues("soft").Set(1)
	default:
		m.metrics.active.WithLabelValues("hard").Set(0)
		m.metrics.active.WithLabelValues("soft").Set(0)
	}

	m.metrics.limitBytes.WithLabelValues("soft").Set(float64(gomemlimit) * m.config.SoftLimitRatio)
	m.metrics.limitBytes.WithLabelValues("hard").Set(float64(gomemlimit) * m.config.HardLimitRatio)
	m.metrics.inUseBytes.Set(float64(inUse))
}

// AllowScrape returns true if scraping targets is allowed.
func (m *Manager) AllowScrape() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil || !cfg.Enforcement.FailScrapes {
		return true
	}
	return LimiterState(m.state.Load()) < StateHardLimit
}

// AllowOTLP returns true if OTLP ingestion is allowed.
func (m *Manager) AllowOTLP() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil || !cfg.Enforcement.RejectOTLP {
		return true
	}
	return LimiterState(m.state.Load()) < StateHardLimit
}

// AllowRemoteWrite returns true if remote write ingestion is allowed.
func (m *Manager) AllowRemoteWrite() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil || !cfg.Enforcement.RejectRemoteWrite {
		return true
	}
	return LimiterState(m.state.Load()) < StateHardLimit
}

// AllowRemoteRead returns true if remote read queries are allowed.
func (m *Manager) AllowRemoteRead() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil || !cfg.Enforcement.RejectRemoteRead {
		return true
	}
	return LimiterState(m.state.Load()) < StateSoftLimit
}

// AllowFederation returns true if federation endpoints are allowed.
func (m *Manager) AllowFederation() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil || !cfg.Enforcement.RejectFederation {
		return true
	}
	return LimiterState(m.state.Load()) < StateSoftLimit
}

// AllowBlockCompaction returns true if TSDB block compaction is allowed.
func (m *Manager) AllowBlockCompaction() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil || !cfg.Enforcement.PauseBlockCompaction {
		return true
	}
	return LimiterState(m.state.Load()) < StateSoftLimit
}

// AllowRecordingRules returns true if recording rule evaluation is allowed.
func (m *Manager) AllowRecordingRules() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()
	if cfg == nil || !cfg.Enforcement.PauseRecordingRules {
		return true
	}
	return LimiterState(m.state.Load()) < StateHardLimit
}
