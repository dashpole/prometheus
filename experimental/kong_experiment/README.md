# Kong Histogram Inconsistency Experiment & GMP Exporter Investigation

This directory contains simulation scripts, source artifacts, and reproduction test documentation for investigating Cloud Monitoring time series rejection errors reported by enterprise Kong API gateway users on GKE (b/516519320).

---

## 1. Background & Problem Statement

Enterprise customers migrating high-throughput Kong API gateways to GKE production reported frequent metric write failures in Google Managed Prometheus (GMP) / Cloud Monitoring:

```
write for resource failed: Points must be written in order.
One or more of the points specified had an older start time than the most recent point.
```

Unlike full batch errors (`timeSeries[0-137]`), sparse index errors isolated the failures specifically to cumulative histogram metrics (e.g., `kong_upstream_latency_ms`).

---

## 2. Proving Kong Format Inconsistencies (Phase 1)

Kong records observations in shared memory dictionaries (`ngx.shared.dict`). We analyzed `kong/plugins/prometheus/prometheus.lua` (archived here as `kong_prometheus.lua`) and authored a standalone Python simulation script (`prove_kong_inconsistency.py`).

### Verifiable Reviewer Steps

Run the simulation script locally to observe Kong's internal memory formatting:

```bash
python3 experimental/kong_experiment/prove_kong_inconsistency.py
```

### Verified Mechanisms
1. **Omitted Zero Buckets (`_bucket`):** When `observe()` records latency, Kong only increments bucket keys where `value <= bucket[i]`. Buckets with count `0` are omitted from shared memory entirely. When a lower latency observation occurs on a subsequent scrape, a new bucket boundary series (e.g., `le="25"`) dynamically appears in the scrape output for the first time.
2. **Mid-Scrape Yielding Desynchronization:** In `metric_data()`, keys are retrieved and sorted alphabetically (`_bucket` before `_count` before `_sum`) with `coroutine.yield()` called before each fetch. Observations occurring mid-scrape increment `_count` and `_sum` after `_bucket` has already been read, causing internal desynchronization within a single scrape response.

---

## 3. Reproducing Cloud Monitoring Rejections (Phase 2)

We authored table-driven unit tests in `google/export/kong_histogram_test.go` exercising `buildDistribution` and `getResetAdjusted` against mock sample batches simulating Kong's scrape anomalies.

### Verifiable Reviewer Steps

Run the reproduction test suite against the local test harness:

```bash
go test -v ./google/export -run TestKongHistogramInconsistencies
```

### Root Cause Analysis
* **Dynamic Zero Bucket Flaw:** When an omitted zero bucket dynamically appears mid-stream on Scrape N (>1), `getResetAdjusted` treats the new series reference as uninitialized (`!hasReset`) and marks `dist.skip = true`. The entire distribution batch is skipped on Scrape N, while `_count` and `_sum` advance their baseline tracking.
* **Out-of-Order Reset Timestamp Trigger:** When asynchronous multi-worker desynchronization or coroutine yielding causes `_count` to drop relative to the prior scrape (`v < lastValue`), `getResetAdjusted` moves `resetTimestamp` forward to `t - 1`. When `_count` recovers on Scrape N+1 while `_sum` lags, Monarch rejects the sample citing an older reset timestamp.

---

## 4. Proposed GMP Exporter Normalization (Phase 3)

To ensure resilient ingestion without dropping valid distributions or corrupting reset timestamps, we patched `google/export/transform.go` and `google/export/series_cache.go`:

1. **Authoritative Reset Coordination:** `seriesCache` tracks established cumulative histogram reset timestamps from `_count` series in `histogramResets`.
2. **Dynamic Bucket Normalization (`getResetAdjustedBucket`):** When a bucket boundary (`_bucket`) arrives without prior tracking (`!hasReset`), if an authoritative reset timestamp was already established on an earlier scrape (`rt < t`), the bucket inherits the established reset timestamp and initializes baseline `resetValue = 0`.

---

## 5. Artifact Index

* `prove_kong_inconsistency.py`: Python simulation script proving Kong memory omission and mid-scrape yield desynchronization.
* `kong_prometheus.lua`: Kong Prometheus plugin source (`prometheus.lua`) containing `lookup_or_create`, `observe`, and `metric_data`.
* `../../google/export/kong_histogram_test.go`: Table-driven unit test suite reproducing dynamic bucket appearance and counter desynchronization.
* `../../google/export/series_cache.go`: Updated series cache implementing `getResetAdjustedBucket` and reset timestamp coordination.
* `../../google/export/transform.go`: Updated distribution builder routing bucket samples to `getResetAdjustedBucket`.
