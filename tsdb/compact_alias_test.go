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

package tsdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestCompactHeadAliasCombined(t *testing.T) {
	opts := DefaultOptions()
	opts.RetentionDuration = int64(time.Hour * 24 * 15 / time.Millisecond)
	opts.NoLockfile = true
	opts.MinBlockDuration = int64(time.Hour * 2 / time.Millisecond)
	opts.MaxBlockDuration = int64(time.Hour * 2 / time.Millisecond)

	db := newTestDB(t, withOpts(opts))
	defer func() { require.NoError(t, db.Close()) }()
	ctx := context.Background()

	// 1. Ingest a combined histogram
	app := db.Appender(ctx)
	h := &histogram.Histogram{
		Schema:        0,
		Count:         15,
		Sum:           18.4,
		ZeroThreshold: 0.001,
		ZeroCount:     3,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
		},
		PositiveBuckets: []int64{5, 2}, // Buckets at boundary 1.0 and 2.0 (deltas 5, 7)
		ClassicBuckets: []histogram.ClassicBucket{
			{UpperBound: 1.0, CumulativeCount: 5},
			{UpperBound: 2.5, CumulativeCount: 10},
			{UpperBound: 5.0, CumulativeCount: 15},
		},
	}
	ref, err := app.AppendHistogram(0, labels.FromStrings("job", "test", "__name__", "http_request_duration_seconds"), 1000, h, nil)
	require.NoError(t, err)
	require.NotEqual(t, storage.SeriesRef(0), ref)
	require.NoError(t, app.Commit())

	// 2. Compact Head to create disk block
	require.NoError(t, db.CompactHead(NewRangeHead(db.Head(), 0, 2000)))
	require.Len(t, db.Blocks(), 1)

	// Verify DB blocks stats in metadata
	blocks := db.Blocks()
	meta := blocks[0].Meta()
	// NumChunks should be exactly 1 (only the base series chunks)
	require.Equal(t, uint64(1), meta.Stats.NumChunks)
	// NumSeries should be 6 (1 base + 5 virtual series: _count, _sum, 3 classic buckets)
	require.Equal(t, uint64(6), meta.Stats.NumSeries)
	// NumFloatSamples should be 0 (no float chunks written!)
	require.Equal(t, uint64(0), meta.Stats.NumFloatSamples)
	// NumHistogramSamples should be 1
	require.Equal(t, uint64(1), meta.Stats.NumHistogramSamples)

	// 3. Query compacted block to verify postings and sample projections!
	querier, err := db.Querier(0, 2000)
	require.NoError(t, err)
	defer querier.Close()

	// Verify _count alias query!
	ssCount := querier.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_request_duration_seconds_count"))
	require.True(t, ssCount.Next())
	itCount := ssCount.At().Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, itCount.Next())
	tc, vc := itCount.At()
	require.Equal(t, int64(1000), tc)
	require.Equal(t, 15.0, vc)
	require.Equal(t, chunkenc.ValNone, itCount.Next())
	require.False(t, ssCount.Next())

	// Verify _sum alias query!
	ssSum := querier.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_request_duration_seconds_sum"))
	require.True(t, ssSum.Next())
	itSum := ssSum.At().Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, itSum.Next())
	ts, vs := itSum.At()
	require.Equal(t, int64(1000), ts)
	require.Equal(t, 18.4, vs)
	require.Equal(t, chunkenc.ValNone, itSum.Next())
	require.False(t, ssSum.Next())

	// Verify _bucket{le="2.5"} alias query!
	ssBucket := querier.Select(ctx, false, nil,
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_request_duration_seconds_bucket"),
		labels.MustNewMatcher(labels.MatchEqual, "le", "2.5"),
	)
	require.True(t, ssBucket.Next())
	itBucket := ssBucket.At().Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, itBucket.Next())
	tb, vb := itBucket.At()
	require.Equal(t, int64(1000), tb)
	require.Equal(t, 10.0, vb)
	require.Equal(t, chunkenc.ValNone, itBucket.Next())
	require.False(t, ssBucket.Next())
}
