# Quantitative Fairness Results
This document captures the raw results of the Scrape Memory Limiter fairness strategies tested against the synthetic metric server.

## Strategy: none
| Metric | Value |
|---|---|
| Max Resident Memory (MiB) | N/A |
| Baseline Target Total Metrics Scraped | 0 |
| Massive Target Total Metrics Scraped | 0 |
| Baseline Avg Up | 0 |

## Strategy: probabilistic
| Metric | Value |
|---|---|
| Max Resident Memory (MiB) | 359.48 |
| Baseline Target Total Metrics Scraped | 275 |
| Massive Target Total Metrics Scraped | 1450000 |
| Baseline Avg Up | 0.1 |

## Strategy: token_bucket
| Metric | Value |
|---|---|
| Max Resident Memory (MiB) | 386.78 |
| Baseline Target Total Metrics Scraped | 410 |
| Massive Target Total Metrics Scraped | 2400000 |
| Baseline Avg Up | 0.14882032667876588 |

## Strategy: deficit_round_robin
| Metric | Value |
|---|---|
| Max Resident Memory (MiB) | 383.41 |
| Baseline Target Total Metrics Scraped | 860 |
| Massive Target Total Metrics Scraped | 2900000 |
| Baseline Avg Up | 0.31272727272727274 |

