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
