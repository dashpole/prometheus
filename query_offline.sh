#!/bin/bash
STRATEGIES=("none" "probabilistic" "token_bucket" "deficit_round_robin")

for STRATEGY in "${STRATEGIES[@]}"; do
    echo "Querying $STRATEGY..."
    ./prometheus --web.listen-address=:19090 --config.file=prometheus-current.yml --storage.tsdb.path=data-$STRATEGY > /dev/null 2>&1 &
    PROM_PID=$!
    sleep 3
    
    # query the sum of the gauge over the last 1 hour, since the test might have run 10 minutes ago.
    BASELINE=$(curl -s -g 'http://localhost:19090/api/v1/query?query=sum_over_time(scrape_samples_scraped{job="baseline_target"}[1h])' | grep -o '\"value\":\[[0-9.]*,\"[0-9.]*\"\]' | grep -o '\"[0-9.]*\"' | head -1 | tr -d '"')
    MASSIVE=$(curl -s -g 'http://localhost:19090/api/v1/query?query=sum_over_time(scrape_samples_scraped{job="massive_target"}[1h])' | grep -o '\"value\":\[[0-9.]*,\"[0-9.]*\"\]' | grep -o '\"[0-9.]*\"' | head -1 | tr -d '"')
    
    # Also get the total up time
    BASELINE_UP=$(curl -s -g 'http://localhost:19090/api/v1/query?query=sum_over_time(up{job="baseline_target"}[1h])' | grep -o '\"value\":\[[0-9.]*,\"[0-9.]*\"\]' | grep -o '\"[0-9.]*\"' | head -1 | tr -d '"')
    
    echo "$STRATEGY Baseline: $BASELINE"
    echo "$STRATEGY Massive: $MASSIVE"
    echo "$STRATEGY Baseline Up Sum: $BASELINE_UP"
    
    kill -TERM $PROM_PID
    wait $PROM_PID || true
done
