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
	"github.com/prometheus/client_golang/prometheus"
)

type memoryLimiterMetrics struct {
	active              *prometheus.GaugeVec
	engagedSecondsTotal *prometheus.CounterVec
	transitionsTotal    *prometheus.CounterVec
	limitBytes          *prometheus.GaugeVec
	inUseBytes          prometheus.Gauge
}

func newMemoryLimiterMetrics(reg prometheus.Registerer) *memoryLimiterMetrics {
	m := &memoryLimiterMetrics{
		active: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "prometheus_memory_limiter_active",
				Help: "Boolean gauge indicating if a memory limiter threshold is currently engaged (1 for active, 0 for inactive).",
			},
			[]string{"limit"},
		),
		engagedSecondsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_memory_limiter_engaged_seconds_total",
				Help: "Total time in seconds spent with memory limiter thresholds engaged.",
			},
			[]string{"limit"},
		),
		transitionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "prometheus_memory_limiter_transitions_total",
				Help: "Total number of state transitions between memory limiter states.",
			},
			[]string{"from", "to"},
		),
		limitBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "prometheus_memory_limiter_limit_bytes",
				Help: "Evaluated memory limit in bytes for each threshold.",
			},
			[]string{"limit"},
		),
		inUseBytes: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "prometheus_memory_limiter_in_use_bytes",
				Help: "Current in-use memory in bytes evaluated by the memory limiter.",
			},
		),
	}

	if reg != nil {
		reg.MustRegister(
			m.active,
			m.engagedSecondsTotal,
			m.transitionsTotal,
			m.limitBytes,
			m.inUseBytes,
		)
	}

	// Initialize gauges with default 0 values.
	m.active.WithLabelValues("soft").Set(0)
	m.active.WithLabelValues("hard").Set(0)

	return m
}
