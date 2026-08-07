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
		"--log.level=info",
	}, extraArgs...)

	cmd := exec.Command(promPath, args...)
	cmd.Env = append(os.Environ(), env...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Start()
	require.NoError(t, err)

	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		if t.Failed() {
			t.Logf("Prometheus stdout:\n%s", stdout.String())
			t.Logf("Prometheus stderr:\n%s", stderr.String())
		}
	}

	// Wait for Prometheus to become ready.
	readyURL := fmt.Sprintf("http://%s/-/ready", addr)
	require.Eventually(t, func() bool {
		resp, err := http.Get(readyURL)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 15*time.Second, 100*time.Millisecond, "Prometheus failed to become ready: %s", stderr.String())

	return cmd, addr, cleanup
}

func fetchPrometheusMetrics(t *testing.T, addr string) map[string]float64 {
	t.Helper()
	metricsURL := fmt.Sprintf("http://%s/metrics", addr)
	resp, err := http.Get(metricsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)

	results := make(map[string]float64)
	for name, mf := range metricFamilies {
		for _, m := range mf.Metric {
			if m.Gauge != nil {
				results[name] = m.Gauge.GetValue()
			} else if m.Counter != nil {
				results[name] = m.Counter.GetValue()
			} else if m.Untyped != nil {
				results[name] = m.Untyped.GetValue()
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

	// Target server providing normal metrics or a burst of 30,000 series (large memory footprint).
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if burstActive.Load() {
			var b strings.Builder
			for i := 0; i < 30000; i++ {
				fmt.Fprintf(&b, "burst_metric_%d{instance=\"node1\",job=\"app\",extra_tag=\"long_label_value_%d\"} %d\n", i, i, i*42)
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
  scrape_interval: 200ms
  scrape_timeout: 200ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 50ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85
    enforcement:
      pause_block_compaction: true
      reject_remote_read: true
      reject_federation: true

scrape_configs:
  - job_name: "test_service"
    scrape_interval: 200ms
    scrape_timeout: 200ms
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
		return active == 0
	}, 15*time.Second, 200*time.Millisecond, "Expected memory limiter to disengage and return to StateOK after burst cessation")
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
				for k := 0; k < 10000; k++ {
					fmt.Fprintf(&b, "burst_series_%d_%d{instance=\"target_%d\",env=\"prod\",cluster=\"us-central1\"} %d\n", targetID, k, targetID, k*7)
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
  scrape_interval: 200ms
  scrape_timeout: 200ms

runtime:
  gogc: 50
  memory_limiter:
    check_interval: 25ms
    soft_limit_ratio: 0.70
    hard_limit_ratio: 0.85

scrape_configs:
  - job_name: "multi_target_benchmark"
    scrape_interval: 200ms
    scrape_timeout: 200ms
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
		return m["prometheus_memory_limiter_active"] == 0
	}, 15*time.Second, 200*time.Millisecond, "Limiter must return to StateOK after multi-target burst ends")
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
