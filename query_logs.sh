#!/bin/bash
curl -s -g 'http://localhost:9090/api/v1/query?query=sum(up) by (job)'
echo ""
curl -s -g 'http://localhost:9090/api/v1/query?query=sum(scrape_samples_scraped) by (job)'
echo ""
curl -s -g 'http://localhost:9090/api/v1/query?query=sum(prometheus_target_scrapes_exceeded_memory_limit_total)'
