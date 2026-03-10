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

func TestMemoryLimiter_StrategyProbabilistic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		l := newScrapeMemoryLimiter(&config.ScrapeMemoryLimiterConfig{
			LimitMiB:      100,
			SpikeLimitMiB: 20, // soft limit is 80
			Strategy:      "probabilistic",
		}, logger)

		// Set random so probability drops are predictable.
		l.randFloat = func() float64 { return 0.2 }

		// Memory is at 90 MiB (50% pressure)
		l.readMemStats = func(m *runtime.MemStats) {
			m.Alloc = 90 * 1024 * 1024
		}
		l.totalMemory = func() uint64 { return 1000 * 1024 * 1024 }

		// Target 1 establishes maxScrapeSize of 1000.
		// multiplier = 1.0, pressure = 0.5, sizeFactor = 1.0
		// dropProb = 0.75. Since 0.2 < 0.75, it drops.
		require.False(t, l.TargetScrapeAllowed(1, 1000))

		// Target 2 is small (10). sizeFactor = 0.01.
		// dropProb = 0.5 * 0.51 = 0.255. 0.2 < 0.255 is true, drops.
		require.False(t, l.TargetScrapeAllowed(2, 10))

		// Test Exponential Decay Starvation Prevention on Target 1
		// Try 2: skips=1, multiplier=0.5. dropProb=0.375. 0.2 < 0.375 -> drops.
		require.False(t, l.TargetScrapeAllowed(1, 1000))
		// Try 3: skips=2, multiplier=0.25. dropProb=0.187. 0.2 < 0.187 is FALSE -> allowed!
		require.True(t, l.TargetScrapeAllowed(1, 1000))
	})
}

func TestMemoryLimiter_StrategyTokenBucket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		l := newScrapeMemoryLimiter(&config.ScrapeMemoryLimiterConfig{
			LimitMiB:      100,
			SpikeLimitMiB: 20, // soft limit is 80
			Strategy:      "token_bucket",
		}, logger)

		// Memory is at 90 MiB (50% pressure)
		l.readMemStats = func(m *runtime.MemStats) {
			m.Alloc = 90 * 1024 * 1024
		}

		// Initial TargetScrapeAllowed generates tokens.
		// maxTokens = 10 MiB (10% of 100 MiB limit).
		// rate = maxTokens * (1.0 - 0.5 pressure) = 5 MiB per second.

		// Attempting 11 MiB scrape. This instantly drops because cost > maxTokens.
		// Actually maxTokens = 10 * 1024 * 102.4 = 1,048,576. Wait, calculation was 100 * 1024 * 102.4 = 10,485,760 bytes = ~10MiB.
		// Let's scrape something extremely large: 30 million bytes. Cost is 30,000,000.
		// This is larger than max tokens, so it drops.
		require.False(t, l.TargetScrapeAllowed(1, 30_000_000))

		// Try again immediately, elapsed is 0, no new tokens. Drops.
		// skips=1, cost = 15,000,000. Still > 10,485,760.
		require.False(t, l.TargetScrapeAllowed(1, 30_000_000))

		// Try to scrape a tiny target. Cost=1000. 1000 <= 10,485,760 tokens. Succeeds!
		require.True(t, l.TargetScrapeAllowed(2, 1000))

		// Now let's advance time so tokens fully regenerate and see if Target 1 passes due to IOUs.
		time.Sleep(3 * time.Second)                            // rate is 5MB/s, so 3 seconds is 15MB, capping at ~10MB.
		require.False(t, l.TargetScrapeAllowed(3, 11_000_000)) // Target 3 has no IOUs, cost=11M > 10M -> drops.

		// Target 1 has IOUs (skips=2, discount=0.25). cost = 30M * 0.25 = 7.5M.
		// Bucket has 10M tokens. So this WILL succeed now!
		require.True(t, l.TargetScrapeAllowed(1, 30_000_000))
	})
}

func TestMemoryLimiter_StrategyDRR(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		l := newScrapeMemoryLimiter(&config.ScrapeMemoryLimiterConfig{
			LimitMiB:      100,
			SpikeLimitMiB: 20, // soft limit is 80
			Strategy:      "deficit_round_robin",
		}, logger)

		// Memory is at 90 MiB (50% pressure)
		l.readMemStats = func(m *runtime.MemStats) {
			m.Alloc = 90 * 1024 * 1024
		}

		// Initial TargetScrapeAllowed registers the target and sets its last quantum map to 0.
		// Next elapsed time will generate quantum.
		// Limit 100MiB means total rate is 10MiB/s. With 50% pressure, actual generation is 5MiB/s.
		// We have 1 active target, so it gets the full 5MiB/s rate.

		// Attempt to scrape 6 MiB immediately. Deficit is 0, so it drops.
		require.False(t, l.TargetScrapeAllowed(1, 6_000_000))

		// Wait 1 second. Target 1 should earn ~5 MiB.
		// 5 MiB is less than 6 MiB, so it should still drop.
		time.Sleep(1 * time.Second)
		require.False(t, l.TargetScrapeAllowed(1, 6_000_000))

		// Target 2 comes along, wants 1 MiB. It has 0 deficit, so it drops immediately, but is now registered.
		require.False(t, l.TargetScrapeAllowed(2, 1_000_000))

		// Wait 1 second (total 2s). Total active targets is now 2. Rate splits to 2.5 MiB/s each.
		// Target 1 was at 5 MiB. Now earns 2.5 MiB more -> 7.5 MiB.
		// Target 2 was at 0 MiB. Now earns 2.5 MiB -> 2.5 MiB.
		time.Sleep(1 * time.Second)

		// Target 1: needs 6 MiB. Has 7.5 MiB. Succeeds! Deficit becomes 1.5 MiB.
		require.True(t, l.TargetScrapeAllowed(1, 6_000_000))

		// Target 2: needs 1 MiB. Has 2.5 MiB. Succeeds! Deficit becomes 1.5 MiB.
		require.True(t, l.TargetScrapeAllowed(2, 1_000_000))
	})
}
