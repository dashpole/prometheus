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
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/util/testutil/synctest"
)

func TestMemoryLimiter_TargetScrapeAllowed(t *testing.T) {
	for _, tc := range []struct {
		desc        string
		cfg         *config.ScrapeMemoryLimiterConfig
		allocBytes  uint64
		totalBytes  uint64
		timeAdvance time.Duration
		wantAllowed []bool // expected result for multiple calls
	}{
		{
			desc: "no limits configured",
			cfg: &config.ScrapeMemoryLimiterConfig{
				LimitMiB:        0,
				LimitPercentage: 0,
			},
			allocBytes:  1024 * 1024 * 100, // 100 MiB
			wantAllowed: []bool{true},
		},
		{
			desc: "under hard limit MiB",
			cfg: &config.ScrapeMemoryLimiterConfig{
				LimitMiB: 10,
			},
			allocBytes:  1024 * 1024 * 9, // 9 MiB
			wantAllowed: []bool{true},
		},
		{
			desc: "over hard limit MiB",
			cfg: &config.ScrapeMemoryLimiterConfig{
				LimitMiB: 10,
			},
			allocBytes:  1024 * 1024 * 11, // 11 MiB
			wantAllowed: []bool{false},
		},
		{
			desc: "under percentage limit",
			cfg: &config.ScrapeMemoryLimiterConfig{
				LimitPercentage: 50,
			},
			allocBytes:  100, // 100 bytes
			totalBytes:  400, // 400 bytes (25%)
			wantAllowed: []bool{true},
		},
		{
			desc: "over percentage limit",
			cfg: &config.ScrapeMemoryLimiterConfig{
				LimitPercentage: 50,
			},
			allocBytes:  201, // 201 bytes
			totalBytes:  400, // 400 bytes (50.25%) -> gets rounded to 50
			wantAllowed: []bool{false},
		},
		{
			desc: "check interval caches result",
			cfg: &config.ScrapeMemoryLimiterConfig{
				LimitMiB:      10,
				CheckInterval: model.Duration(1 * time.Second),
			},
			allocBytes:  1024 * 1024 * 11,     // initially over limit
			wantAllowed: []bool{false, false}, // first call calculates, second call is cached
			timeAdvance: 500 * time.Millisecond,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger := slog.New(slog.DiscardHandler)
				l := newScrapeMemoryLimiter(tc.cfg, logger)

				// Mock the memory reading functions.
				callCount := 0
				l.readMemStats = func(m *runtime.MemStats) {
					m.Alloc = tc.allocBytes
					callCount++
				}
				l.totalMemory = func() uint64 {
					return tc.totalBytes
				}

				// First call initializes the lastCheck
				allowed := l.TargetScrapeAllowed(0, 0)
				require.Equal(t, tc.wantAllowed[0], allowed)
				require.Equal(t, 1, callCount)

				if len(tc.wantAllowed) > 1 {
					if tc.timeAdvance > 0 {
						time.Sleep(tc.timeAdvance)
					}

					allowed := l.TargetScrapeAllowed(0, 0)
					require.Equal(t, tc.wantAllowed[1], allowed)
					// Call count should still be 1 because of the check interval cache.
					require.Equal(t, 1, callCount)
				}
			})
		})
	}
}

func TestMemoryLimiter_Fairness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		l := newScrapeMemoryLimiter(&config.ScrapeMemoryLimiterConfig{
			LimitMiB:      100,
			SpikeLimitMiB: 20, // soft limit is 80
		}, logger)

		// Hardcode random to always return 0.5 for predictable probability testing.
		l.randFloat = func() float64 { return 0.5 }

		// Memory is at 90 MiB (50% pressure: (90 - 80) / (100 - 80) = 10 / 20 = 0.5)
		l.readMemStats = func(m *runtime.MemStats) {
			m.Alloc = 90 * 1024 * 1024
		}
		l.totalMemory = func() uint64 { return 1000 * 1024 * 1024 }

		// Establish maxScrapeSize. Since pressure is 0.5, sizeFactor is 1.0 (it's the max),
		// dropProb = 0.5 * 1.5 = 0.75, which is > 0.5 so it will be dropped!
		// Wait, if we want it to be accepted, we should just manually set maxScrapeSize.
		// Or we can let it be dropped, it sets maxScrapeSize anyway.
		allowed := l.TargetScrapeAllowed(1, 1000)
		require.False(t, allowed)
		require.Equal(t, 1000, l.maxScrapeSize)

		// Target 2 is large (1000). sizeFactor = 1.0. dropProb = 0.5 * (0.5 + 1.0) = 0.75.
		// Since 0.5 < 0.75, it should be dropped.
		require.False(t, l.TargetScrapeAllowed(2, 1000))

		// Target 3 is small (10). sizeFactor = 0.01. dropProb = 0.5 * (0.5 + 0.01) = 0.255.
		// Since 0.5 < 0.255 is false, it should be allowed.
		require.True(t, l.TargetScrapeAllowed(3, 10))

		// Test starvation prevention on Target 2 (large)
		// It has been skipped 1 time already.
		require.False(t, l.TargetScrapeAllowed(2, 1000)) // 2 skips
		require.False(t, l.TargetScrapeAllowed(2, 1000)) // 3 skips
		require.False(t, l.TargetScrapeAllowed(2, 1000)) // 4 skips
		require.False(t, l.TargetScrapeAllowed(2, 1000)) // 5 skips
		require.True(t, l.TargetScrapeAllowed(2, 1000))  // 6th time is forced allowed!
	})
}
