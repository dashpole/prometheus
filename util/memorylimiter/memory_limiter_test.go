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
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/config"
)

func newMockMetricsReader(total, released, gomemlimit, gcLimiterCycle uint64) MetricsReader {
	return func() MemoryStats {
		return MemoryStats{
			TotalBytes:     total,
			ReleasedBytes:  released,
			GOMEMLIMIT:     gomemlimit,
			GCLimiterCycle: gcLimiterCycle,
		}
	}
}

func TestMemoryLimiterStateTransitions(t *testing.T) {
	cfg := &config.MemoryLimiterConfig{
		CheckInterval:  model.Duration(100 * time.Millisecond),
		SoftLimitRatio: 0.70,
		HardLimitRatio: 0.85,
		Enforcement: config.MemoryLimiterEnforcement{
			PauseBlockCompaction: true,
			RejectRemoteRead:     true,
			RejectFederation:     true,
			FailScrapes:          true,
			RejectOTLP:           true,
			RejectRemoteWrite:    true,
			PauseRecordingRules:  true,
		},
	}

	reg := prometheus.NewRegistry()
	mgr, err := NewManager(cfg, slog.Default(), reg)
	require.NoError(t, err)

	now := time.Now()
	mgr.now = func() time.Time { return now }

	gomemlimit := uint64(1000 * 1024 * 1024) // 1000 MB

	// 1. Normal state: 500 MB in-use (50% ratio < 70%)
	mgr.metricsReader = newMockMetricsReader(600*1024*1024, 100*1024*1024, gomemlimit, 0)
	mgr.Evaluate()
	require.Equal(t, StateOK, mgr.State())
	require.True(t, mgr.AllowScrape())
	require.True(t, mgr.AllowOTLP())
	require.True(t, mgr.AllowRemoteWrite())
	require.True(t, mgr.AllowRemoteRead())
	require.True(t, mgr.AllowFederation())
	require.True(t, mgr.AllowBlockCompaction())
	require.True(t, mgr.AllowRecordingRules())

	// 2. Soft limit state: 750 MB in-use (75% ratio >= 70%, < 85%)
	now = now.Add(100 * time.Millisecond)
	mgr.metricsReader = newMockMetricsReader(850*1024*1024, 100*1024*1024, gomemlimit, 0)
	mgr.Evaluate()
	require.Equal(t, StateSoftLimit, mgr.State())
	require.True(t, mgr.AllowScrape())
	require.True(t, mgr.AllowOTLP())
	require.True(t, mgr.AllowRemoteWrite())
	require.False(t, mgr.AllowRemoteRead())
	require.False(t, mgr.AllowFederation())
	require.False(t, mgr.AllowBlockCompaction())
	require.True(t, mgr.AllowRecordingRules())

	// 3. Hard limit state: 900 MB in-use (90% ratio >= 85%)
	now = now.Add(100 * time.Millisecond)
	mgr.metricsReader = newMockMetricsReader(1000*1024*1024, 100*1024*1024, gomemlimit, 0)
	mgr.Evaluate()
	require.Equal(t, StateHardLimit, mgr.State())
	require.False(t, mgr.AllowScrape())
	require.False(t, mgr.AllowOTLP())
	require.False(t, mgr.AllowRemoteWrite())
	require.False(t, mgr.AllowRemoteRead())
	require.False(t, mgr.AllowFederation())
	require.False(t, mgr.AllowBlockCompaction())
	require.False(t, mgr.AllowRecordingRules())

	// 4. Memory drops back to 400 MB (40% < 70%) -> Clean recovery to StateOK
	now = now.Add(100 * time.Millisecond)
	mgr.metricsReader = newMockMetricsReader(500*1024*1024, 100*1024*1024, gomemlimit, 0)
	mgr.Evaluate()
	require.Equal(t, StateOK, mgr.State())
	require.True(t, mgr.AllowScrape())
	require.True(t, mgr.AllowBlockCompaction())
}

func TestGCLimiterEscalation(t *testing.T) {
	cfg := &config.MemoryLimiterConfig{
		CheckInterval:  model.Duration(100 * time.Millisecond),
		SoftLimitRatio: 0.70,
		HardLimitRatio: 0.85,
		Enforcement: config.MemoryLimiterEnforcement{
			FailScrapes: true,
		},
	}

	mgr, err := NewManager(cfg, slog.Default(), nil)
	require.NoError(t, err)

	gomemlimit := uint64(1000 * 1024 * 1024)

	// In-use is low (500 MB / 50%), but GC CPU limiter engaged (cycle goes from 10 to 11)
	mgr.metricsReader = newMockMetricsReader(500*1024*1024, 0, gomemlimit, 10)
	mgr.Evaluate()
	require.Equal(t, StateOK, mgr.State())

	mgr.metricsReader = newMockMetricsReader(500*1024*1024, 0, gomemlimit, 11)
	mgr.Evaluate()
	require.Equal(t, StateHardLimit, mgr.State())
	require.False(t, mgr.AllowScrape())
}

func TestUnlimitedGOMEMLIMIT(t *testing.T) {
	cfg := &config.MemoryLimiterConfig{
		CheckInterval:  model.Duration(100 * time.Millisecond),
		SoftLimitRatio: 0.70,
		HardLimitRatio: 0.85,
	}

	mgr, err := NewManager(cfg, slog.Default(), nil)
	require.NoError(t, err)

	// GOMEMLIMIT is MaxInt64 (unlimited)
	mgr.metricsReader = newMockMetricsReader(500*1024*1024, 0, math.MaxInt64, 0)
	mgr.Evaluate()
	require.Equal(t, StateOK, mgr.State())
	require.True(t, mgr.AllowScrape())
}
