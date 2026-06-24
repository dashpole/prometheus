import re

def simulate_kong_observe_and_scrape():
    """
    Simulates Kong's prometheus.lua behavior based on exact code in:
    kong/plugins/prometheus/prometheus.lua
    """
    buckets = [25, 50, 80, 100, 250, 400, 700, 1000, 2000, 5000]
    bucket_count = len(buckets)
    
    # Shared dictionary ngx.shared.dict
    shared_dict = {}
    
    def lookup_or_create(name, label_str):
        keys = [f"{name}_count{label_str}", f"{name}_sum{label_str}"]
        bucket_pref = f"{name}_bucket" + (label_str[:-1] + "," if label_str != "{}" else "{")
        for buc in buckets:
            keys.append(f"{bucket_pref}le=\"{buc}\"}}")
        keys.append(f"{bucket_pref}le=\"Inf\"}}")
        return keys

    def incr(key, val):
        shared_dict[key] = shared_dict.get(key, 0) + val

    def observe(name, label_str, val):
        keys = lookup_or_create(name, label_str)
        incr(keys[0], 1)      # _count
        incr(keys[1], val)    # _sum
        seen = False
        for i in range(bucket_count - 1, -1, -1):
            if val <= buckets[i]:
                incr(keys[2 + i], 1)
                seen = True
            elif seen:
                break
        incr(keys[bucket_count + 2], 1) # Inf

    print("=== Test 1: Omitted Zero Buckets ===")
    observe("kong_upstream_latency_ms", '{route="users"}', 150)
    print("Keys in shared dictionary after observing val=150:")
    for k in sorted(shared_dict.keys()):
        print(f"  {k}: {shared_dict[k]}")
    
    # Check omitted buckets
    assert 'kong_upstream_latency_ms_bucket{route="users",le="25"}' not in shared_dict
    assert 'kong_upstream_latency_ms_bucket{route="users",le="50"}' not in shared_dict
    assert 'kong_upstream_latency_ms_bucket{route="users",le="80"}' not in shared_dict
    assert 'kong_upstream_latency_ms_bucket{route="users",le="100"}' not in shared_dict
    print("-> Buckets le=25, 50, 80, 100 are omitted from shared dictionary!")

    print("\n=== Test 2: Mid-Scrape Yielding Desynchronization ===")
    keys_sorted = sorted(shared_dict.keys())
    print("Keys sorted alphabetically in metric_data():")
    for idx, k in enumerate(keys_sorted):
        print(f"  [{idx}] {k}")
    
    scrape_result = {}
    for idx, k in enumerate(keys_sorted):
        # Simulate coroutine.yield() before retrieving value
        if k == 'kong_upstream_latency_ms_count{route="users"}':
            print(f"-> coroutine.yield() before reading {k}. Meanwhile, background observe(val=100) executes!")
            observe("kong_upstream_latency_ms", '{route="users"}', 100)
        scrape_result[k] = shared_dict[k]
    
    print("\nScrape output returned by metric_data():")
    for k, v in scrape_result.items():
        print(f"  {k}: {v}")
    
    print("\nInconsistencies observed in single scrape response:")
    print(f"  le=\"100\" bucket count: {scrape_result.get('kong_upstream_latency_ms_bucket{route=\"users\",le=\"100\"}', 0)} (Actual in dict: {shared_dict.get('kong_upstream_latency_ms_bucket{route=\"users\",le=\"100\"}', 0)})")
    print(f"  _count: {scrape_result['kong_upstream_latency_ms_count{route=\"users\"}']}")
    print(f"  _sum: {scrape_result['kong_upstream_latency_ms_sum{route=\"users\"}']}")

if __name__ == '__main__':
    simulate_kong_observe_and_scrape()
