#!/bin/bash
set -e

RESULTS_FILE="scrape_memory_limiter_results.md"

# Ensure we have a clean results file mapped
echo "# Quantitative Fairness Results" > "$RESULTS_FILE"
echo "This document captures the raw results of the Scrape Memory Limiter fairness strategies tested against the synthetic metric server." >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# Build the custom Prometheus and Target binaries
echo ">>> Building Prometheus..."
go build -o prometheus cmd/prometheus/main.go
echo ">>> Building Synthetic Target Server..."
go build -o integration-target integration-target.go

STRATEGIES=("none" "probabilistic" "token_bucket" "deficit_round_robin")
TEST_DURATION_SECS=60 # 1 minute per test to gather enough data

TARGET_PID=""
PROM_PID=""

cleanup() {
    echo ">>> Cleaning up processes..."
    [ -n "$PROM_PID" ] && kill -TERM "$PROM_PID" 2>/dev/null || true
    [ -n "$TARGET_PID" ] && kill -TERM "$TARGET_PID" 2>/dev/null || true
    wait "$PROM_PID" 2>/dev/null || true
    wait "$TARGET_PID" 2>/dev/null || true
}
trap cleanup EXIT

for STRATEGY in "${STRATEGIES[@]}"; do
    echo "=========================================================="
    echo ">>> Starting Integration Test for Strategy: $STRATEGY"
    echo "=========================================================="

    # 1. Setup config
    if [ "$STRATEGY" = "none" ]; then
        # Remove the scrape_memory_limiter lines completely
        sed '/^scrape_memory_limiter:/,/^strategy: STRATEGY_PLACEHOLDER/d' prometheus-integration.yml > prometheus-current.yml
    else
        sed "s/STRATEGY_PLACEHOLDER/$STRATEGY/g" prometheus-integration.yml > prometheus-current.yml
    fi
    
    # 2. Start synthetic target server
    ./integration-target &
    TARGET_PID=$!
    
    # Wait for target server to be ready
    sleep 2

    # 3. Start Prometheus with feature flag
    if [ "$STRATEGY" = "none" ]; then
        # Baseline run without the flag to establish unmitigated OOM behavior or pure throughput
        ./prometheus --web.listen-address=:19090 --config.file=prometheus-current.yml --storage.tsdb.path=data-$STRATEGY > prometheus_${STRATEGY}.log 2>&1 &
    else
        ./prometheus --web.listen-address=:19090 --config.file=prometheus-current.yml --storage.tsdb.path=data-$STRATEGY --enable-feature=scrape-memory-limiter > prometheus_${STRATEGY}.log 2>&1 &
    fi
    PROM_PID=$!

    echo ">>> Running test for $TEST_DURATION_SECS seconds..."
    
    # Monitor for crashes
    for i in $(seq 1 $TEST_DURATION_SECS); do
        if ! kill -0 $PROM_PID 2>/dev/null; then
            echo "!!! Prometheus crashed during strategy $STRATEGY!"
            break
        fi
        sleep 1
    done

    # 4. Gather quantitative data
    if kill -0 $PROM_PID 2>/dev/null; then
        echo ">>> Data gathering for $STRATEGY..."
        
        echo "## Strategy: $STRATEGY" >> "$RESULTS_FILE"
        echo "| Metric | Value |" >> "$RESULTS_FILE"
        echo "|---|---|" >> "$RESULTS_FILE"
        
        # Max Memory
        MAX_MEM=$(curl -s -g 'http://localhost:19090/api/v1/query?query=max_over_time(process_resident_memory_bytes[1m])' | grep -o '\"value\":\[[0-9.]*,\"[0-9.]*\"\]' | grep -o '\"[0-9.]*\"' | head -1 | tr -d '"' || echo "N/A")
        if [ "$MAX_MEM" != "N/A" ] && [ -n "$MAX_MEM" ]; then
            MAX_MEM_MIB=$(echo "scale=2; $MAX_MEM / 1024 / 1024" | bc)
            echo "| Max Resident Memory (MiB) | $MAX_MEM_MIB |" >> "$RESULTS_FILE"
        else
            echo "| Max Resident Memory (MiB) | N/A |" >> "$RESULTS_FILE"
        fi

        # Throughput (Succesful Scrapes) for baseline 
        BASELINE_SCRAPES=$(curl -s -g 'http://localhost:19090/api/v1/query?query=sum(scrape_samples_scraped{job="baseline_target"})' | grep -o '\"value\":\[[0-9.]*,\"[0-9]*\"\]' | grep -o '\"[0-9]*\"' | head -1 | tr -d '"' || echo "0")
        if [ -z "$BASELINE_SCRAPES" ]; then BASELINE_SCRAPES=0; fi
        echo "| Baseline Target Total Metrics Scraped | $BASELINE_SCRAPES |" >> "$RESULTS_FILE"
        
        # Throughput for massive
        MASSIVE_SCRAPES=$(curl -s -g 'http://localhost:19090/api/v1/query?query=sum(scrape_samples_scraped{job="massive_target"})' | grep -o '\"value\":\[[0-9.]*,\"[0-9]*\"\]' | grep -o '\"[0-9]*\"' | head -1 | tr -d '"' || echo "0")
        if [ -z "$MASSIVE_SCRAPES" ]; then MASSIVE_SCRAPES=0; fi
        echo "| Massive Target Total Metrics Scraped | $MASSIVE_SCRAPES |" >> "$RESULTS_FILE"
        
        # Average Up (Disruption proxy)
        BASELINE_UP=$(curl -s -g 'http://localhost:19090/api/v1/query?query=avg_over_time(up{job="baseline_target"}[1m])' | grep -o '\"value\":\[[0-9.]*,\"[0-9.]*\"\]' | grep -o '\"[0-9.]*\"' | head -1 | tr -d '"' || echo "0")
        if [ -z "$BASELINE_UP" ]; then BASELINE_UP=0; fi
        echo "| Baseline Avg Up | $BASELINE_UP |" >> "$RESULTS_FILE"

        echo "" >> "$RESULTS_FILE"

        echo ">>> Gracefully stopping Prometheus..."
        kill -TERM $PROM_PID
        wait $PROM_PID || true
        PROM_PID=""
    else
        echo "## Strategy: $STRATEGY" >> "$RESULTS_FILE"
        echo "**CRASHED**" >> "$RESULTS_FILE"
        echo "" >> "$RESULTS_FILE"
    fi

    # Cleanup Target Server
    if [ -n "$TARGET_PID" ]; then
        kill -TERM $TARGET_PID || true
        wait $TARGET_PID || true
        TARGET_PID=""
    fi
    
    echo ">>> Cleanup for $STRATEGY complete."
    sleep 2
done

# Explicitly unset trap so we don't double kill at the very end
trap - EXIT

echo ">>> All integration tests complete. Results written to $RESULTS_FILE"
