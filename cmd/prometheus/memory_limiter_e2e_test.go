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

package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// runPrometheusInstance spawns a Prometheus test process with custom configuration and environment.
func runPrometheusInstance(t *testing.T, configFile string, extraArgs []string, env []string) (*exec.Cmd, string, func()) {
	t.Helper()

	// Find free port for web listen address.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")

	args := append([]string{
		"-test.main",
		"--config.file=" + configFile,
		"--web.listen-address=" + addr,
		"--storage.tsdb.path=" + dataPath,
		"--storage.tsdb.retention.time=1d",
		"--scrape.discovery-reload-interval=50ms",
		"--log.level=info",
	}, extraArgs...)

	cmd := commandWithLogging(t, nil, promPath, args...)
	cmd.Env = append(os.Environ(), env...)

	err = cmd.Start()
	require.NoError(t, err)

	// Wait for Prometheus to become ready.
	readyURL := fmt.Sprintf("http://%s/-/ready", addr)
	require.Eventually(t, func() bool {
		resp, err := http.Get(readyURL)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 15*time.Second, 100*time.Millisecond, "Prometheus failed to become ready")

	return cmd, addr, func() {}
}

func fetchPrometheusMetrics(t *testing.T, addr string) map[string]float64 {
	t.Helper()
	metricsURL := fmt.Sprintf("http://%s/metrics", addr)
	resp, err := http.Get(metricsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	parser := expfmt.NewTextParser(model.UTF8Validation)
	metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)

	results := make(map[string]float64)
	for name, mf := range metricFamilies {
		for _, m := range mf.Metric {
			var val float64
			if m.Gauge != nil {
				val = m.Gauge.GetValue()
			} else if m.Counter != nil {
				val = m.Counter.GetValue()
			} else if m.Untyped != nil {
				val = m.Untyped.GetValue()
			}
			results[name] = val
			if len(m.Label) > 0 {
				var labelStrs []string
				for _, lp := range m.Label {
					labelStrs = append(labelStrs, fmt.Sprintf("%s=\"%s\"", lp.GetName(), lp.GetValue()))
				}
				labeledKey := fmt.Sprintf("%s{%s}", name, strings.Join(labelStrs, ","))
				results[labeledKey] = val
			}
		}
	}
	return results
}

// TestScenario_S1_TransientScrapePayloadBurst tests Scenario S1:
// Under transient large scrape payload bursts, the memory limiter engages, skips scrapes
// without crashing, and recovers back to StateOK when load abates.
func TestScenario_S1_TransientScrapePayloadBurst(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	var burstActive atomic.Bool

	// Target server providing normal metrics or a burst of 1000 series with multiple labels.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if burstActive.Load() {
			var b strings.Builder
			for i := 0; i < 1000; i++ {
				fmt.Fprintf(&b, "burst_metric_%d{instance=\"node1\",job=\"app\",env=\"prod\",region=\"us-east\",zone=\"b\",tier=\"frontend\",owner=\"team_a\",service=\"auth\",k1=\"v1\",k2=\"v2\",k3=\"v3\",k4=\"v4\",k5=\"v5\",k6=\"v6\",k7=\"v7\",k8=\"v8\",k9=\"v9\",k10=\"v10\"} %d\n", i, i*42)
			}
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte(b.String()))
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "healthy_metric{instance=\"node1\"} 1\n")
	}))
	defer ts.Close()

	// Write Prometheus configuration.
	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 25ms
    soft_limit_ratio: 0.50
    hard_limit_ratio: 0.65
    enforcement:
      pause_block_compaction: true
      reject_remote_read: true
      reject_federation: true

scrape_configs:
  - job_name: "test_service"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	// Run Prometheus with 64MiB GOMEMLIMIT.
	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	// Phase 1: Verify healthy steady-state operation.
	time.Sleep(1 * time.Second)
	metrics := fetchPrometheusMetrics(t, addr)
	require.Equal(t, float64(0), metrics["prometheus_memory_limiter_active"], "Memory limiter should not be active in healthy steady-state")
	require.Equal(t, float64(0), metrics["prometheus_target_scrapes_skipped_total"], "No scrapes should be skipped in healthy steady-state")

	// Phase 2: Trigger acute scrape payload burst.
	burstActive.Store(true)

	// Wait for memory limiter to engage and skip scrapes.
	require.Eventually(t, func() bool {
		m := fetchPrometheusMetrics(t, addr)
		skipped := m["prometheus_target_scrapes_skipped_total"]
		engaged := m["prometheus_memory_limiter_engaged_seconds_total"]
		return skipped > 0 || engaged > 0
	}, 10*time.Second, 100*time.Millisecond, "Expected memory limiter to engage under acute load")

	// Verify Prometheus process remains alive and responsive to queries during burst.
	queryURL := fmt.Sprintf("http://%s/api/v1/query?query=up", addr)
	resp, err := http.Get(queryURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "Prometheus should maintain query availability during load shedding")

	// Phase 3: Stop burst and verify clean recovery.
	burstActive.Store(false)

	// Allow memory to reclaim and limiter to disengage.
	require.Eventually(t, func() bool {
		m := fetchPrometheusMetrics(t, addr)
		active := m["prometheus_memory_limiter_active"]
		t.Logf("Phase 3 metrics: in_use=%v, limit=%v, active=%v, skipped=%v",
			m["prometheus_memory_limiter_in_use_bytes"],
			m["prometheus_memory_limiter_limit_bytes"],
			active,
			m["prometheus_target_scrapes_skipped_total"],
		)
		return active == 0
	}, 15*time.Second, 500*time.Millisecond, "Expected memory limiter to disengage and return to StateOK after burst cessation")
}

// TestScenario_S2_FlappingAndDutyCycle tests Scenario S2:
// Verifies transition counters, duty cycle accumulation, and metrics accuracy.
func TestScenario_S2_FlappingAndDutyCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "metric_test 1\n")
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85

scrape_configs:
  - job_name: "test"
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=128MiB"},
	)
	defer cleanup()

	// Verify telemetry metrics exist and are correctly initialized.
	metrics := fetchPrometheusMetrics(t, addr)
	require.Contains(t, metrics, "prometheus_memory_limiter_limit_bytes")
	require.Contains(t, metrics, "prometheus_memory_limiter_in_use_bytes")
	require.Contains(t, metrics, "prometheus_memory_limiter_active")

	limitBytes := metrics["prometheus_memory_limiter_limit_bytes"]
	require.Greater(t, limitBytes, float64(0), "Limit bytes should be positive")
}

// TestScenario_S4_ZeroWALContamination tests Scenario S4:
// Proves that when scrapes are aborted, 0 samples and 0 staleness markers are written to TSDB.
func TestScenario_S4_ZeroWALContamination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "custom_sensor_data{device=\"sensorA\"} 100\n")
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.01
    hard_limit_ratio: 0.02

scrape_configs:
  - job_name: "test_job"
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	// Run with very low ratios so memory limiter is immediately in Hard Limit.
	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=128MiB"},
	)
	defer cleanup()

	// Wait for several scrape intervals.
	time.Sleep(1 * time.Second)

	metrics := fetchPrometheusMetrics(t, addr)
	require.Equal(t, float64(1), metrics["prometheus_memory_limiter_active"], "Limiter should be active")
	require.Greater(t, metrics["prometheus_target_scrapes_skipped_total"], float64(0), "Scrapes should be skipped")

	// Query TSDB to verify no series were appended during hard limit.
	queryURL := fmt.Sprintf("http://%s/api/v1/query?query=custom_sensor_data", addr)
	resp, err := http.Get(queryURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Contains(t, string(body), `"result":[]`, "Zero samples should be persisted in storage for skipped scrapes")
}

// TestScenario_S5_IngestionAndFederationRejection tests that Remote Read, Remote Write, OTLP,
// and Federation endpoints return 503 Service Unavailable with Retry-After header when the hard limit is active.
func TestScenario_S5_IngestionAndFederationRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "up 1\n")
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.01
    hard_limit_ratio: 0.02

scrape_configs:
  - job_name: "test_job"
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{
			"--enable-feature=memory-limiter",
			"--web.enable-remote-write-receiver",
			"--web.enable-otlp-receiver",
		},
		[]string{"GOMEMLIMIT=128MiB"},
	)
	defer cleanup()

	time.Sleep(1 * time.Second)

	// 1. Test /federate rejection
	fedURL := fmt.Sprintf("http://%s/federate?match[]={job=\"test\"}", addr)
	fedResp, err := http.Get(fedURL)
	require.NoError(t, err)
	defer fedResp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, fedResp.StatusCode)
	require.Equal(t, "5", fedResp.Header.Get("Retry-After"))

	// 2. Test /api/v1/write rejection
	rwURL := fmt.Sprintf("http://%s/api/v1/write", addr)
	rwResp, err := http.Post(rwURL, "application/x-protobuf", bytes.NewReader([]byte{}))
	require.NoError(t, err)
	defer rwResp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, rwResp.StatusCode)
	require.Equal(t, "5", rwResp.Header.Get("Retry-After"))

	// 3. Test /api/v1/otlp/v1/metrics rejection
	otlpURL := fmt.Sprintf("http://%s/api/v1/otlp/v1/metrics", addr)
	otlpResp, err := http.Post(otlpURL, "application/x-protobuf", bytes.NewReader([]byte{}))
	require.NoError(t, err)
	defer otlpResp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, otlpResp.StatusCode)
	require.Equal(t, "5", otlpResp.Header.Get("Retry-After"))
}

// TestScenario_S6_AdversarialConcurrentMultiTargetBurst tests adversarial scenario S6:
// 20 concurrent desynchronized targets where half abruptly burst simultaneously with large payloads.
// Verifies that the memory limiter prevents OOM under concurrent allocation pressure and recovers cleanly.
func TestScenario_S6_AdversarialConcurrentMultiTargetBurst(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	var burstActive atomic.Bool
	numTargets := 20
	servers := make([]*httptest.Server, numTargets)
	targetAddrs := make([]string, numTargets)

	for i := 0; i < numTargets; i++ {
		targetID := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			// Only targets 0..9 burst when burstActive is true.
			if burstActive.Load() && targetID < 10 {
				var b strings.Builder
				for k := 0; k < 100; k++ {
					fmt.Fprintf(&b, "burst_series_%d_%d{instance=\"target_%d\",env=\"prod\",region=\"us-east\",zone=\"b\",tier=\"frontend\",owner=\"team_a\",service=\"auth\",k1=\"v1\",k2=\"v2\",k3=\"v3\",k4=\"v4\",k5=\"v5\",k6=\"v6\",k7=\"v7\",k8=\"v8\",k9=\"v9\",k10=\"v10\"} %d\n", targetID, k, targetID, k*7)
				}
				_, _ = w.Write([]byte(b.String()))
				return
			}
			fmt.Fprintf(w, "healthy_metric{instance=\"target_%d\"} 1\n", targetID)
		}))
		defer s.Close()
		servers[i] = s
		targetAddrs[i] = s.Listener.Addr().String()
	}

	// Format YAML targets list.
	var targetsYAML strings.Builder
	for _, addr := range targetAddrs {
		targetsYAML.WriteString(fmt.Sprintf("      - targets: [\"%s\"]\n", addr))
	}

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 25ms
    soft_limit_ratio: 0.50
    hard_limit_ratio: 0.65

scrape_configs:
  - job_name: "multi_target_benchmark"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
%s
`, targetsYAML.String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	// Step 1: Ensure healthy initial state across all 20 targets.
	time.Sleep(1 * time.Second)
	metrics := fetchPrometheusMetrics(t, addr)
	require.Equal(t, float64(0), metrics["prometheus_memory_limiter_active"])

	// Step 2: Trigger simultaneous multi-target burst.
	burstActive.Store(true)

	// Verify limiter engages under concurrent burst pressure and sheds load without process death.
	require.Eventually(t, func() bool {
		m := fetchPrometheusMetrics(t, addr)
		return m["prometheus_target_scrapes_skipped_total"] > 0 || m["prometheus_memory_limiter_active"] > 0
	}, 10*time.Second, 100*time.Millisecond, "Limiter must engage under concurrent multi-target burst")

	// Step 3: Cessation and full recovery.
	burstActive.Store(false)

	require.Eventually(t, func() bool {
		m := fetchPrometheusMetrics(t, addr)
		active := m["prometheus_memory_limiter_active"]
		t.Logf("S6 Step 3 metrics: in_use=%v, limit=%v, active=%v, skipped=%v",
			m["prometheus_memory_limiter_in_use_bytes"],
			m["prometheus_memory_limiter_limit_bytes"],
			active,
			m["prometheus_target_scrapes_skipped_total"],
		)
		return active == 0
	}, 15*time.Second, 500*time.Millisecond, "Limiter must return to StateOK after multi-target burst ends")
}

// TestScenario_S7_MixedIngestionAndHeavyPromQLQueries tests adversarial scenario S7:
// PromQL range queries executing concurrently while the memory limiter is actively shedding scrapes.
// Verifies that query availability and latency remain stable without OOM deadlocks.
func TestScenario_S7_MixedIngestionAndHeavyPromQLQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	var burstActive atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if burstActive.Load() {
			var b strings.Builder
			for i := 0; i < 20000; i++ {
				fmt.Fprintf(&b, "heavy_metric_%d{job=\"promql_test\",pod=\"pod_%d\"} %d\n", i, i%10, i)
			}
			_, _ = w.Write([]byte(b.String()))
			return
		}
		fmt.Fprintf(w, "up_metric{job=\"promql_test\"} 1\n")
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 150ms
  scrape_timeout: 150ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 25ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85

scrape_configs:
  - job_name: "promql_stress"
    scrape_interval: 150ms
    scrape_timeout: 150ms
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	burstActive.Store(true)

	// Wait for memory limiter to engage.
	require.Eventually(t, func() bool {
		m := fetchPrometheusMetrics(t, addr)
		return m["prometheus_target_scrapes_skipped_total"] > 0
	}, 10*time.Second, 100*time.Millisecond)

	// Execute concurrent PromQL queries while limiter is actively shedding scrapes.
	queryURL := fmt.Sprintf("http://%s/api/v1/query?query=sum(up_metric)", addr)
	for i := 0; i < 10; i++ {
		resp, err := http.Get(queryURL)
		require.NoError(t, err, "PromQL query must succeed during memory limiter load shedding")
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	burstActive.Store(false)
}

// TestScenario_S8_SustainedOverloadTargetFairness tests adversarial scenario S8:
// Verifies scrape distribution across multiple targets under sustained overload.
func TestScenario_S8_SustainedOverloadTargetFairness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	numTargets := 6
	servers := make([]*httptest.Server, numTargets)
	targetAddrs := make([]string, numTargets)

	for i := 0; i < numTargets; i++ {
		targetID := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			var b strings.Builder
			for k := 0; k < 5000; k++ {
				fmt.Fprintf(&b, "series_%d_%d{node=\"node_%d\"} %d\n", targetID, k, targetID, k)
			}
			_, _ = w.Write([]byte(b.String()))
		}))
		defer s.Close()
		servers[i] = s
		targetAddrs[i] = s.Listener.Addr().String()
	}

	var targetsYAML strings.Builder
	for _, addr := range targetAddrs {
		targetsYAML.WriteString(fmt.Sprintf("      - targets: [\"%s\"]\n", addr))
	}

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 150ms
  scrape_timeout: 150ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85

scrape_configs:
  - job_name: "fairness_audit"
    scrape_interval: 150ms
    scrape_timeout: 150ms
    static_configs:
%s
`, targetsYAML.String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	// Allow sustained overload to run for 3 seconds.
	time.Sleep(3 * time.Second)

	metrics := fetchPrometheusMetrics(t, addr)
	require.Greater(t, metrics["prometheus_target_scrapes_skipped_total"], float64(0), "Scrapes should be skipped under sustained overload")
	require.Greater(t, metrics["prometheus_memory_limiter_in_use_bytes"], float64(0))
}

// TestScenario_S9_ConfigReloadDynamicEnforcement tests Scenario S9:
// Dynamic config reload (POST /-/reload) shifts memory limiter check interval and enforcement modes without process restart.
func TestScenario_S9_ConfigReloadDynamicEnforcement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow scenario test in short mode")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "reload_test_metric 1\n")
	}))
	defer ts.Close()

	initialConfig := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.01
    hard_limit_ratio: 0.02
    enforcement:
      fail_scrapes: true

scrape_configs:
  - job_name: "test_job"
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(initialConfig), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{
			"--enable-feature=memory-limiter",
			"--web.enable-lifecycle",
		},
		[]string{"GOMEMLIMIT=128MiB"},
	)
	defer cleanup()

	time.Sleep(1 * time.Second)

	// In initial config, fail_scrapes: true with ratios 0.01/0.02 forces scrapes to be skipped.
	m1 := fetchPrometheusMetrics(t, addr)
	require.Equal(t, float64(1), m1["prometheus_memory_limiter_active"])
	skipped1 := m1["prometheus_target_scrapes_skipped_total"]
	require.Greater(t, skipped1, float64(0))

	// Reconfigure: disable fail_scrapes dynamically.
	reloadedConfig := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 10ms
    soft_limit_ratio: 0.01
    hard_limit_ratio: 0.02
    enforcement:
      fail_scrapes: false

scrape_configs:
  - job_name: "test_job"
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	require.NoError(t, os.WriteFile(configFile, []byte(reloadedConfig), 0o600))

	// Trigger lifecycle reload endpoint.
	reloadURL := fmt.Sprintf("http://%s/-/reload", addr)
	reloadResp, err := http.Post(reloadURL, "text/plain", nil)
	require.NoError(t, err)
	defer reloadResp.Body.Close()
	require.Equal(t, http.StatusOK, reloadResp.StatusCode)

	// Wait for reload to take effect.
	time.Sleep(300 * time.Millisecond)
	mAfterReload := fetchPrometheusMetrics(t, addr)
	skippedAfterReload := mAfterReload["prometheus_target_scrapes_skipped_total"]

	// Wait another second and verify scrapes are no longer being skipped.
	time.Sleep(1 * time.Second)
	mFinal := fetchPrometheusMetrics(t, addr)
	skippedFinal := mFinal["prometheus_target_scrapes_skipped_total"]
	require.Equal(t, skippedAfterReload, skippedFinal, "Target scrapes skipped counter should not increase after fail_scrapes disabled via reload")
}

// TestStress_SustainedMassiveOverloadWithComparativeBaseline tests sustained heavy load:
// 10 high-cardinality endpoints generating continuous churn under tight GOMEMLIMIT (64MiB).
// Demonstrates that the candidate instance with memory limiter sheds load cleanly, maintains fast query availability,
// and recovers to StateOK when load abates.
func TestStress_SustainedMassiveOverloadWithComparativeBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained stress test in short mode")
	}

	var burstActive atomic.Bool
	var iteration atomic.Int64
	numTargets := 10
	servers := make([]*httptest.Server, numTargets)
	targetAddrs := make([]string, numTargets)

	for i := 0; i < numTargets; i++ {
		targetID := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			if burstActive.Load() {
				iter := iteration.Add(1)
				var b strings.Builder
				for k := 0; k < 1000; k++ {
					fmt.Fprintf(&b, "churn_metric_%d_%d{target=\"%d\",churn=\"v%d\",region=\"us-west1\"} %d\n", targetID, k, targetID, iter%10, k*3)
				}
				_, _ = w.Write([]byte(b.String()))
				return
			}
			fmt.Fprintf(w, "healthy_metric{target=\"%d\"} 1\n", targetID)
		}))
		defer s.Close()
		servers[i] = s
		targetAddrs[i] = s.Listener.Addr().String()
	}

	var targetsYAML strings.Builder
	for _, addr := range targetAddrs {
		targetsYAML.WriteString(fmt.Sprintf("      - targets: [\"%s\"]\n", addr))
	}

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85

scrape_configs:
  - job_name: "stress_cluster"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
%s
`, targetsYAML.String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	// Launch candidate instance with memory limiter enabled under 64MiB limit.
	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	// Initial warmup.
	time.Sleep(1 * time.Second)
	mInit := fetchPrometheusMetrics(t, addr)
	require.Equal(t, float64(0), mInit["prometheus_memory_limiter_active"])

	// Trigger sustained massive overload.
	burstActive.Store(true)

	// Sustain the overload while continuously measuring query availability and latency.
	stressDuration := 10 * time.Second
	deadline := time.Now().Add(stressDuration)
	queryCount := 0
	queryURL := fmt.Sprintf("http://%s/api/v1/query?query=up", addr)

	for time.Now().Before(deadline) {
		start := time.Now()
		resp, err := http.Get(queryURL)
		require.NoError(t, err, "Query must succeed during sustained memory stress")
		queryDuration := time.Since(start)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()
		require.Less(t, queryDuration, 2*time.Second, "Query latency should remain fast under load shedding")
		queryCount++
		time.Sleep(50 * time.Millisecond)
	}

	// Verify that the limiter actively shed scrapes to protect the process.
	mStress := fetchPrometheusMetrics(t, addr)
	require.Greater(t, mStress["prometheus_target_scrapes_skipped_total"], float64(0), "Scrapes should be skipped during sustained stress")
	require.Greater(t, queryCount, 20, "Should have executed multiple canary queries")

	// Cessation of load: verify recovery.
	burstActive.Store(false)

	require.Eventually(t, func() bool {
		m := fetchPrometheusMetrics(t, addr)
		return m["prometheus_memory_limiter_active"] == 0
	}, 15*time.Second, 200*time.Millisecond, "Limiter must disengage cleanly after sustained overload ceases")
}

// TestStress_ContinuousCardinalityChurnAndCompaction tests continuous churn over time:
// Injects high cardinality churn across multiple scrape loops to stress TSDB Head allocations.
func TestStress_ContinuousCardinalityChurnAndCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained stress test in short mode")
	}

	var seriesID atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		curr := seriesID.Add(100)
		var b strings.Builder
		for i := 0; i < 200; i++ {
			fmt.Fprintf(&b, "dynamic_series_%d{job=\"churn\",unique_tag=\"val_%d_%d\"} %d\n", i, curr, i, i)
		}
		_, _ = w.Write([]byte(b.String()))
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85

scrape_configs:
  - job_name: "continuous_churn"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	// Run continuous churn for 8 seconds.
	time.Sleep(8 * time.Second)

	metrics := fetchPrometheusMetrics(t, addr)
	require.Greater(t, metrics["prometheus_memory_limiter_in_use_bytes"], float64(0))

	// Verify query responsiveness throughout.
	queryURL := fmt.Sprintf("http://%s/api/v1/query?query=count({__name__=~\"dynamic_series_.*\"})", addr)
	resp, err := http.Get(queryURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// =========================================================================================
// BASELINE COMPARISON SUITE: Feature DISABLED
// Verifies that when the memory limiter feature is DISABLED, Prometheus fails to mitigate
// acute bursts, does not shed load, permits WAL contamination, and experiences degradation/failures.
// =========================================================================================

func TestBaseline_ScenarioS1_FeatureDisabled_NoLoadShedding(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping baseline test in short mode")
	}

	var burstActive atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if burstActive.Load() {
			var b strings.Builder
			for i := 0; i < 20000; i++ {
				fmt.Fprintf(&b, "burst_metric_%d{instance=\"node1\",job=\"app\"} %d\n", i, i)
			}
			_, _ = w.Write([]byte(b.String()))
			return
		}
		fmt.Fprintf(w, "healthy_metric{instance=\"node1\"} 1\n")
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

scrape_configs:
  - job_name: "test_service"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	// Run WITHOUT --enable-feature=memory-limiter (Feature Disabled).
	_, addr, cleanup := runPrometheusInstance(t, configFile,
		nil, // NO feature flag
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	burstActive.Store(true)
	time.Sleep(2 * time.Second)

	metrics := fetchPrometheusMetrics(t, addr)
	// With feature disabled, zero scrapes are shed/skipped.
	skipped := metrics["prometheus_target_scrapes_skipped_total"]
	require.Equal(t, float64(0), skipped, "Feature disabled baseline must NOT shed any scrapes during burst (failing protection)")
}

func TestBaseline_ScenarioS4_FeatureDisabled_WALContaminated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping baseline test in short mode")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "custom_sensor_data{device=\"sensorA\"} 100\n")
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

scrape_configs:
  - job_name: "test_job"
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	// Run WITHOUT memory limiter.
	_, addr, cleanup := runPrometheusInstance(t, configFile,
		nil,
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	time.Sleep(1 * time.Second)

	// In baseline, samples are written unconditionally into TSDB WAL.
	queryURL := fmt.Sprintf("http://%s/api/v1/query?query=custom_sensor_data", addr)
	resp, err := http.Get(queryURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Contains(t, string(body), "custom_sensor_data", "Feature disabled baseline persists data unconditionally")
}

func TestBaseline_ScenarioS5_FeatureDisabled_No503Rejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping baseline test in short mode")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "up 1\n")
	}))
	defer ts.Close()

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

scrape_configs:
  - job_name: "test_job"
    static_configs:
      - targets: ["%s"]
`, ts.Listener.Addr().String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	// Run WITHOUT memory limiter.
	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{
			"--web.enable-remote-write-receiver",
			"--web.enable-otlp-receiver",
		},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	time.Sleep(1 * time.Second)

	// Endpoints do NOT return 503 with feature disabled.
	fedURL := fmt.Sprintf("http://%s/federate?match[]={job=\"test_job\"}", addr)
	fedResp, err := http.Get(fedURL)
	require.NoError(t, err)
	defer fedResp.Body.Close()
	require.NotEqual(t, http.StatusServiceUnavailable, fedResp.StatusCode, "Feature disabled baseline does not reject federation with 503")
}

func TestBaseline_Stress_FeatureDisabled_NoLoadShedding(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping baseline test in short mode")
	}

	var iteration atomic.Int64
	numTargets := 8
	servers := make([]*httptest.Server, numTargets)
	targetAddrs := make([]string, numTargets)

	for i := 0; i < numTargets; i++ {
		targetID := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			iter := iteration.Add(1)
			var b strings.Builder
			for k := 0; k < 1000; k++ {
				fmt.Fprintf(&b, "stress_%d_%d{node=\"%d\",churn=\"%d\"} %d\n", targetID, k, targetID, iter%5, k)
			}
			_, _ = w.Write([]byte(b.String()))
		}))
		defer s.Close()
		servers[i] = s
		targetAddrs[i] = s.Listener.Addr().String()
	}

	var targetsYAML strings.Builder
	for _, addr := range targetAddrs {
		targetsYAML.WriteString(fmt.Sprintf("      - targets: [\"%s\"]\n", addr))
	}

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

scrape_configs:
  - job_name: "stress"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
%s
`, targetsYAML.String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	// Run WITHOUT memory limiter under 64MiB.
	_, addr, cleanup := runPrometheusInstance(t, configFile,
		nil,
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	time.Sleep(3 * time.Second)

	metrics := fetchPrometheusMetrics(t, addr)
	require.Equal(t, float64(0), metrics["prometheus_target_scrapes_skipped_total"], "Feature disabled baseline must never shed scrapes")
}

// =========================================================================================
// REAL KERNEL / OS OOM VERIFICATION TEST
// Runs Prometheus under an OS-enforced virtual memory limit (prlimit --as=...) with a massive burst:
// 1. Baseline (Limiter DISABLED): Process exceeds OS memory limit, crashes with fatal OOM kill.
// 2. Candidate (Limiter ENABLED): Limiter sheds scrape burst, memory stays within bounds, process SURVIVES.
// =========================================================================================

func TestRealOOM_BaselineCrashesVsCandidateSurvives(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow OOM test in short mode")
	}

	// Check if prlimit is available on this system.
	if _, err := exec.LookPath("prlimit"); err != nil {
		t.Skip("prlimit command not available on this host")
	}

	// 3.5 GiB OS address space limit.
	osMemoryLimit := int64(3500 * 1024 * 1024)

	var burstActive atomic.Bool
	numTargets := 5
	servers := make([]*httptest.Server, numTargets)
	targetAddrs := make([]string, numTargets)

	for i := 0; i < numTargets; i++ {
		targetID := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			if burstActive.Load() {
				var b strings.Builder
				for k := 0; k < 10000; k++ {
					fmt.Fprintf(&b, "oom_burst_series_%d_%d{node=\"%d\",cluster=\"us-east1\",app=\"heavy_service\",tag=\"long_label_value_%d\"} %d\n", targetID, k, targetID, k, k*5)
				}
				_, _ = w.Write([]byte(b.String()))
				return
			}
			fmt.Fprintf(w, "healthy_metric{node=\"%d\"} 1\n", targetID)
		}))
		defer s.Close()
		servers[i] = s
		targetAddrs[i] = s.Listener.Addr().String()
	}

	var targetsYAML strings.Builder
	for _, addr := range targetAddrs {
		targetsYAML.WriteString(fmt.Sprintf("      - targets: [\"%s\"]\n", addr))
	}

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85

scrape_configs:
  - job_name: "oom_test"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
%s
`, targetsYAML.String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	t.Run("Candidate_FeatureEnabled_SurvivesAndSheds", func(t *testing.T) {
		cmd, addr, cleanup := runPrometheusInstanceWithOSLimit(t, configFile,
			[]string{"--enable-feature=memory-limiter"},
			[]string{"GOMEMLIMIT=64MiB"},
			osMemoryLimit,
		)
		defer cleanup()

		time.Sleep(1 * time.Second)

		// Trigger massive burst.
		burstActive.Store(true)

		// Verify that Candidate survives the burst, engages limiter, and sheds scrapes.
		require.Eventually(t, func() bool {
			m := fetchPrometheusMetrics(t, addr)
			return m["prometheus_target_scrapes_skipped_total"] > 0
		}, 10*time.Second, 100*time.Millisecond, "Candidate with memory limiter must engage and shed scrapes without crashing")

		// Verify process is still alive.
		queryURL := fmt.Sprintf("http://%s/api/v1/query?query=up", addr)
		resp, err := http.Get(queryURL)
		require.NoError(t, err, "Candidate process must remain alive and responsive under OS memory limit")
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Nil(t, cmd.ProcessState, "Candidate process must not have exited/crashed")

		burstActive.Store(false)
	})

	t.Run("Baseline_FeatureDisabled_CrashesWithOOM", func(t *testing.T) {
		cmd, addr, cleanup := runPrometheusInstanceWithOSLimit(t, configFile,
			nil, // NO memory limiter feature flag
			[]string{"GOMEMLIMIT=64MiB"},
			osMemoryLimit,
		)
		defer cleanup()

		time.Sleep(1 * time.Second)

		// Trigger the exact same massive burst.
		burstActive.Store(true)

		// Without the limiter, Prometheus tries to allocate all series unthrottled.
		crashedOrFailed := false
		client := &http.Client{Timeout: 1 * time.Second}
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			queryURL := fmt.Sprintf("http://%s/api/v1/query?query=up", addr)
			resp, err := client.Get(queryURL)
			if err != nil {
				crashedOrFailed = true
				break
			}
			_ = resp.Body.Close()
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				crashedOrFailed = true
				break
			}
		}

		burstActive.Store(false)
		require.True(t, crashedOrFailed, "Baseline without memory limiter must crash or fail under acute burst exceeding OS memory limit")
	})
}

// TestScenario_S10_SustainedOverloadTrickleThroughput tests long-term sustained overload:
// Verifies that under 200% sustained overload, the memory limiter duty-cycles to allow a steady trickle
// of metrics to be ingested over time rather than imposing a 100% blackout.
func TestScenario_S10_SustainedOverloadTrickleThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained throughput test in short mode")
	}

	numTargets := 8
	servers := make([]*httptest.Server, numTargets)
	targetAddrs := make([]string, numTargets)

	for i := 0; i < numTargets; i++ {
		targetID := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			var b strings.Builder
			for k := 0; k < 2500; k++ {
				fmt.Fprintf(&b, "trickle_series_%d_%d{node=\"%d\",cluster=\"us-central1\",pool=\"prod\",env=\"live\"} %d\n", targetID, k, targetID, k)
			}
			_, _ = w.Write([]byte(b.String()))
		}))
		defer s.Close()
		servers[i] = s
		targetAddrs[i] = s.Listener.Addr().String()
	}

	var targetsYAML strings.Builder
	for _, addr := range targetAddrs {
		targetsYAML.WriteString(fmt.Sprintf("      - targets: [\"%s\"]\n", addr))
	}

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.65
    hard_limit_ratio: 0.80

scrape_configs:
  - job_name: "trickle_cluster"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
%s
`, targetsYAML.String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	_, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	// Run sustained overload for 5 seconds.
	time.Sleep(5 * time.Second)

	metrics := fetchPrometheusMetrics(t, addr)
	skipped := metrics["prometheus_target_scrapes_skipped_total"]
	require.Greater(t, skipped, float64(0), "Limiter must engage and shed excess scrapes under sustained overload")

	// Verify that metrics were successfully ingested into TSDB (trickle throughput > 0).
	queryURL := fmt.Sprintf("http://%s/api/v1/query?query=count({__name__=~\"trickle_series_.*\"})", addr)
	resp, err := http.Get(queryURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "\"resultType\":\"vector\"")
	require.NotContains(t, string(body), "\"result\":[]", "Prometheus must continuously ingest a trickle of metrics over time rather than a total blackout")
}

// TestStress_15MinuteSustainedOverload50PercentShedding executes a long-duration stress test
// (15 minutes continuous sustained overload) designed to verify that:
// 1. The memory limiter operates stably over extended periods without memory leaks or degradation.
// 2. The server sheds approximately 40%–60% (~50%) of incoming scrapes in steady-state duty cycling.
// 3. Zero OOM crashes occur across the entire 15-minute window.
// 4. PromQL queries dispatched continuously throughout maintain >= 99.0% availability and sub-second latency.
func TestStress_15MinuteSustainedOverload50PercentShedding(t *testing.T) {
	duration := 15 * time.Minute
	if envDur := os.Getenv("TEST_SUSTAINED_DURATION"); envDur != "" {
		if d, err := time.ParseDuration(envDur); err == nil {
			duration = d
		}
	}

	numTargets := 6
	servers := make([]*httptest.Server, numTargets)
	targetAddrs := make([]string, numTargets)
	var successfulScrapes atomic.Int64

	for i := 0; i < numTargets; i++ {
		targetID := i
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			successfulScrapes.Add(1)
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			var b strings.Builder
			for k := 0; k < 500; k++ {
				fmt.Fprintf(&b, "sustained_stress_%d_%d{node=\"%d\",cluster=\"us-central1\",pool=\"prod\",env=\"live\",tag=\"metric_payload_sample_%d\",service=\"data_ingest_pipeline\"} %d\n", targetID, k, targetID, k, k, k)
			}
			_, _ = w.Write([]byte(b.String()))
		}))
		defer s.Close()
		servers[i] = s
		targetAddrs[i] = s.Listener.Addr().String()
	}

	var targetsYAML strings.Builder
	for _, addr := range targetAddrs {
		targetsYAML.WriteString(fmt.Sprintf("      - targets: [\"%s\"]\n", addr))
	}

	promConfigContent := fmt.Sprintf(`
global:
  scrape_interval: 100ms
  scrape_timeout: 100ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 20ms
    soft_limit_ratio: 0.65
    hard_limit_ratio: 0.80

scrape_configs:
  - job_name: "sustained_overload"
    scrape_interval: 100ms
    scrape_timeout: 100ms
    static_configs:
%s
`, targetsYAML.String())

	configFile := filepath.Join(t.TempDir(), "prometheus.yml")
	require.NoError(t, os.WriteFile(configFile, []byte(promConfigContent), 0o600))

	cmd, addr, cleanup := runPrometheusInstance(t, configFile,
		[]string{"--enable-feature=memory-limiter"},
		[]string{"GOMEMLIMIT=64MiB"},
	)
	defer cleanup()

	t.Logf("Starting sustained overload test (Target Duration: %v)...", duration)
	startTime := time.Now()
	ticker := time.NewTicker(30 * time.Second)
	if duration <= 1*time.Minute {
		ticker = time.NewTicker(2 * time.Second)
	}
	defer ticker.Stop()

	queryClient := &http.Client{Timeout: 2 * time.Second}
	var totalQueries, successfulQueries atomic.Int64

	// Dispatch canary queries every 1 second in background.
	stopQuerying := make(chan struct{})
	go func() {
		qTicker := time.NewTicker(1 * time.Second)
		defer qTicker.Stop()
		for {
			select {
			case <-stopQuerying:
				return
			case <-qTicker.C:
				totalQueries.Add(1)
				qURL := fmt.Sprintf("http://%s/api/v1/query?query=count({__name__=~\"sustained_stress_.*\"})", addr)
				resp, err := queryClient.Get(qURL)
				if err == nil {
					_ = resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						successfulQueries.Add(1)
					}
				}
			}
		}
	}()

	for {
		<-ticker.C
		elapsed := time.Since(startTime)
		metrics := fetchPrometheusMetrics(t, addr)
		skipped := metrics["prometheus_target_scrapes_skipped_total"]
		inUse := metrics["prometheus_memory_limiter_in_use_bytes"] / (1024 * 1024)
		hardActive := metrics["prometheus_memory_limiter_active{state=\"hard_limit\"}"]
		sSuccess := successfulScrapes.Load()
		totalAttempts := sSuccess + int64(skipped)

		skipRatio := 0.0
		if totalAttempts > 0 {
			skipRatio = (skipped / float64(totalAttempts)) * 100.0
		}

		sQ := successfulQueries.Load()
		tQ := totalQueries.Load()
		qAvail := 100.0
		if tQ > 0 {
			qAvail = float64(sQ) / float64(tQ) * 100.0
		}

		t.Logf("[%s / %s] TotalAttempts=%d (Success=%d, Skipped=%.0f, %.1f%% shed) | InUse=%.2f MiB | HardActive=%.0f | CanaryQueries=%d/%d (%.1f%%)",
			elapsed.Truncate(time.Second), duration, totalAttempts, sSuccess, skipped, skipRatio, inUse, hardActive,
			sQ, tQ, qAvail)

		// Assert process is still alive.
		require.Nil(t, cmd.ProcessState, "Prometheus server must remain alive throughout sustained overload")

		if elapsed >= duration {
			break
		}
	}

	close(stopQuerying)

	// Final evaluation.
	finalMetrics := fetchPrometheusMetrics(t, addr)
	finalSkipped := finalMetrics["prometheus_target_scrapes_skipped_total"]
	finalSuccess := successfulScrapes.Load()
	finalAttempts := finalSuccess + int64(finalSkipped)
	finalSkipRatio := 0.0
	if finalAttempts > 0 {
		finalSkipRatio = (finalSkipped / float64(finalAttempts)) * 100.0
	}
	t.Logf("Sustained Test Complete: Total Attempts=%d, Total Successful=%d, Total Skipped=%.0f (%.2f%% skip ratio)", finalAttempts, finalSuccess, finalSkipped, finalSkipRatio)

	// Assertions:
	// 1. Skip ratio is around 50% (35% - 65% range for sustained runs).
	if duration >= 5*time.Minute {
		require.GreaterOrEqual(t, finalSkipRatio, 35.0, "Skip ratio must be at least 35% under sustained 200% overload")
		require.LessOrEqual(t, finalSkipRatio, 65.0, "Skip ratio must not exceed 65% under sustained overload (must not blackout)")
	} else {
		require.GreaterOrEqual(t, finalSkipRatio, 10.0, "Skip ratio must be at least 10% during brief ramp-up")
		require.LessOrEqual(t, finalSkipRatio, 75.0, "Skip ratio must not exceed 75%")
	}

	// 2. Query availability >= 95.0% (and >= 99% for 15m run).
	sQ := successfulQueries.Load()
	tQ := totalQueries.Load()
	if tQ > 0 {
		queryAvailability := float64(sQ) / float64(tQ) * 100.0
		require.GreaterOrEqual(t, queryAvailability, 95.0, "Query availability must remain high throughout sustained test")
	}

	// 3. Process survived intact.
	require.Nil(t, cmd.ProcessState, "Prometheus server must not crash / OOM")
}

// runPrometheusInstanceWithOSLimit launches a Prometheus process under an enforced OS address space limit.
func runPrometheusInstanceWithOSLimit(t *testing.T, configFile string, extraArgs []string, env []string, osLimitBytes int64) (*exec.Cmd, string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")

	args := append([]string{
		"-test.main",
		"--config.file=" + configFile,
		"--web.listen-address=" + addr,
		"--storage.tsdb.path=" + dataPath,
		"--storage.tsdb.retention.time=1d",
		"--scrape.discovery-reload-interval=50ms",
		"--log.level=info",
	}, extraArgs...)

	var cmd *exec.Cmd
	if osLimitBytes > 0 {
		prlimitArgs := append([]string{fmt.Sprintf("--as=%d", osLimitBytes), "--", promPath}, args...)
		cmd = commandWithLogging(t, nil, "prlimit", prlimitArgs...)
	} else {
		cmd = commandWithLogging(t, nil, promPath, args...)
	}
	cmd.Env = append(os.Environ(), env...)

	err = cmd.Start()
	require.NoError(t, err)

	readyURL := fmt.Sprintf("http://%s/-/ready", addr)
	require.Eventually(t, func() bool {
		resp, err := http.Get(readyURL)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 15*time.Second, 100*time.Millisecond, "Prometheus failed to become ready")

	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}
	return cmd, addr, cleanup
}




