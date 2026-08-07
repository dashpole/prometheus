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
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
)

type LimiterState int32

const (
	StateOK LimiterState = iota
	StateSoftLimit
	StateHardLimit
)

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

type MemoryLimiter interface {
	State() LimiterState
	AllowScrape() bool
	AllowOTLP() bool
	AllowRemoteWrite() bool
	AllowRemoteRead() bool
	AllowFederation() bool
	AllowBlockCompaction() bool
	AllowRecordingRules() bool
	ApplyConfig(cfg *config.MemoryLimiterConfig) error
	Start(ctx context.Context)
	Stop()
}

type MemoryStats struct {
	TotalBytes     uint64
	ReleasedBytes  uint64
	GOMEMLIMIT     uint64
	GCLimiterCycle uint64
}

type MetricsReader func() MemoryStats

func defaultMetricsReader() MemoryStats {
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/gomemlimit:bytes"},
		{Name: "/gc/limiter/last-enabled:gc-cycle"},
	}
	metrics.Read(samples)
	var stats MemoryStats
	if samples[0].Value.Kind() == metrics.KindUint64 {
		stats.TotalBytes = samples[0].Value.Uint64()
	}
	if samples[1].Value.Kind() == metrics.KindUint64 {
		stats.ReleasedBytes = samples[1].Value.Uint64()
	}
	if samples[2].Value.Kind() == metrics.KindUint64 {
		stats.GOMEMLIMIT = samples[2].Value.Uint64()
	}
	if samples[3].Value.Kind() == metrics.KindUint64 {
		stats.GCLimiterCycle = samples[3].Value.Uint64()
	}
	return stats
}

type Manager struct {
	logger *slog.Logger

	mu     sync.RWMutex
	config *config.MemoryLimiterConfig

	state                atomic.Int32
	lastInUse            atomic.Uint64
	lastGCLimiterCycle   uint64
	gcLimiterInitialized bool
	lastCheckTime        time.Time

	metricsReader MetricsReader
	now           func() time.Time

	metrics *memoryLimiterMetrics

	reloadCh chan struct{}
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewManager(cfg *config.MemoryLimiterConfig, logger *slog.Logger, reg prometheus.Registerer) (*Manager, error) {
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

	if cfg != nil {
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid memory limiter config: %w", err)
		}
	}

	return m, nil
}

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

func (m *Manager) State() LimiterState {
	if m == nil {
		return StateOK
	}
	return LimiterState(m.state.Load())
}

func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.lastCheckTime = m.now()
	m.mu.Unlock()

	m.wg.Add(1)
	go m.run(loopCtx)
}

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
	releasedBytes := stats.ReleasedBytes
	gomemlimit := stats.GOMEMLIMIT
	gcLimiterCycle := stats.GCLimiterCycle

	if m.config == nil || gomemlimit == 0 || gomemlimit == math.MaxInt64 {
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
	if totalBytes > releasedBytes {
		inUse = totalBytes - releasedBytes
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
		if oldState == StateSoftLimit {
			m.metrics.engagedSecondsTotal.WithLabelValues("soft").Add(elapsed)
		} else if oldState == StateHardLimit {
			m.metrics.engagedSecondsTotal.WithLabelValues("hard").Add(elapsed)
		}
	}

	pressureRatio := float64(inUse) / float64(gomemlimit)

	gcLimiterActive := m.gcLimiterInitialized && gcLimiterCycle > m.lastGCLimiterCycle
	m.lastGCLimiterCycle = gcLimiterCycle
	m.gcLimiterInitialized = true

	var newState LimiterState
	if pressureRatio >= m.config.HardLimitRatio || gcLimiterActive {
		newState = StateHardLimit
	} else if pressureRatio >= m.config.SoftLimitRatio {
		newState = StateSoftLimit
	} else {
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
	if newState == StateHardLimit {
		m.metrics.active.WithLabelValues("hard").Set(1)
		m.metrics.active.WithLabelValues("soft").Set(1)
	} else if newState == StateSoftLimit {
		m.metrics.active.WithLabelValues("hard").Set(0)
		m.metrics.active.WithLabelValues("soft").Set(1)
	} else {
		m.metrics.active.WithLabelValues("hard").Set(0)
		m.metrics.active.WithLabelValues("soft").Set(0)
	}

	m.metrics.limitBytes.WithLabelValues("soft").Set(float64(gomemlimit) * m.config.SoftLimitRatio)
	m.metrics.limitBytes.WithLabelValues("hard").Set(float64(gomemlimit) * m.config.HardLimitRatio)
	m.metrics.inUseBytes.Set(float64(inUse))
}

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
