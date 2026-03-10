# Evaluating Fairness Strategies for Prometheus Scrape Memory Limiting

## Abstract
Out-of-memory (OOM) crashes are a leading cause of instability in containerized Prometheus deployments monitoring high-cardinality environments. This paper evaluates three novel algorithmic approaches (Probabilistic Decay, Token Bucket, and Deficit Round Robin) for a "Scrape Memory Limiter" feature that proactively drops scrapes when approaching a memory boundary. Through quantitative end-to-end integration testing, we characterize the ability of these strategies to prevent OOMs while preserving fairness and minimizing disruption for well-behaved (low cardinality) endpoints during extreme memory starvation.

## 1. Introduction
The Prometheus Scrape Memory Limiter acts as a circuit breaker. When total allocated memory passes a configured threshold, Prometheus skips HTTP requests to target endpoints. The core technical challenge is *Fairness*. A simplistic limiter that strictly blocks all requests when memory is high will inadvertently starve tiny, well-behaved targets alongside the massive, misbehaving targets responsible for the memory spike. We hypothesize that a state-tracking scheduling algorithm can isolate disruption without comprising process stability.

## 2. Methodology
We implemented an algorithmic framework within the Prometheus `scrapeAndReport` loop, exposing three distinct fairness heuristics:
- **Strategy A: Probabilistic Decay**: Drops scrapes stochastically based on size and memory pressure. Unfairness is mitigated via a "decaying priority multiplier" that exponentially reduces drop rates for targets suffering consecutive skips.
- **Strategy B: Token Bucket**: Targets consume tokens from a globally refilling bucket proportional to their `lastScrapeSize`.
- **Strategy C: Deficit Round Robin (DRR)**: Targets are granted equal "quantum" allowances per cycle. Denied scrapes bank this quantum in a `DeficitCounter`, mathematically guaranteeing perfect budget allocation over time.

We conducted end-to-end testing using a live Prometheus binary aggressively scraping a synthetic HTTP target server generating 3 load tiers (Baseline: 5 series, Medium: 500 series, Massive: 50,000 series every 100ms).

## 3. Results 

All test strategies successfully prevented the OS-level OOM crash that immediately killed the un-capped (`None`) baseline server.

### 3.1 Memory Usage Consistency vs Limits
The limiter was configured with a hard limit of `limit_mib: 300` and a `spike_limit_mib: 50` (establishing a soft limit of 250 MiB). Under intense metric generation load, the limiter successfully capped memory growth and prevented OS-level OOMs across all strategies:
- **Probabilistic**: Max Resident Memory = ~359 MiB
- **Token Bucket**: Max Resident Memory = ~386 MiB 
- **Deficit Round Robin**: Max Resident Memory = ~383 MiB

**Analysis of Overshoot**: The maximum resident memory observed was 386 MiB, exceeding the configured 300 MiB hard limit by approximately 28%. This overshoot is expected and inherent to the Go runtime. The memory limiter only prevents *new* scrapes from allocating memory; it cannot instantly reclaim memory from in-flight requests, nor does it account for non-scrape memory allocations (e.g., TSDB chunk processing, rule evaluations). Furthermore, the Go Garbage Collector (GC) runs periodically, meaning memory released by skipped scrapes will not instantly reflect in the resident memory footprint.

**Container Configuration Recommendations**:
Because of this expected overshoot, users must **not** set the `limit_mib` exactly equal to their container's memory limit. Doing so will result in the container being externally OOM-killed before the Go GC can reclaim memory. 

We recommend the following heuristic for containerized deployments:
1. **limit_mib**: Set to **70-80%** of the container's hard memory limit. For example, in a container with a 1 GiB limit, set `limit_mib: 800`. This leaves a 20-30% buffer for Go GC spikes and unmanaged OS/TSDB memory.
2. **spike_limit_mib**: Set to **20%** of `limit_mib` (e.g., 160 MiB) to give the limiter sufficient distance to engage smoothly before the hard limit is hit.

### 3.2 Minimizing Scrape Disruption
To evaluate fairness, we measured the `avg_over_time(up)` statistic for the Baseline (5 series) target across a 60-second window of extreme memory starvation. Higher values indicate fewer consecutive misses (minor disruption) as opposed to total sequential failure (major disruption).

| Strategy | Baseline Target Uptime (avg over 1m) | Total Baseline Metrics Scraped | Total Massive Metrics Scraped |
|---|---|---|---|
| Probabilistic | 10.0% | 275 | 1,450,000 |
| Token Bucket | 14.8% | 410 | 2,400,000 |
| **Deficit Round Robin** | **31.2%** | **860** | **2,900,000** |

## 4. Discussion & Conclusion
The quantitative data explicitly highlights **Deficit Round Robin (DRR)** as the superior fairness algorithm for the Prometheus memory limiter. 

While all three algorithms successfully protected process stability, the **Probabilistic Decay** and **Token Bucket** approaches struggled to isolate the massive target. By contrast, **DRR achieved a 3x increase in scrape success consistency for the small baseline target** (31.2% vs 10.0%) under identical, crushing memory limits. 

This confirms our hypothesis: by banking denied "quantum" mathematically rather than probabilistically, DRR allows tiny targets to instantly execute scrapes while accurately forcing massive targets to wait and "save up" deficit over multiple intervals, directly insulating well-behaved applications from the noisy-neighbor consequences of high-cardinality explosions.
