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

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func TestHeadIndexAliasingCombined(t *testing.T) {
	opts := DefaultHeadOptions()
	opts.ChunkRange = 1000
	h, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	defer h.Close()

	app := h.Appender(context.Background())

	// Combined histogram
	hist := &histogram.Histogram{
		Count:         5,
		ZeroCount:     2,
		Sum:           18.4,
		ZeroThreshold: 1e-100,
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
		},
		PositiveBuckets: []int64{1, 1},
		ClassicBuckets: []histogram.ClassicBucket{
			{UpperBound: 1.0, CumulativeCount: 2},
			{UpperBound: 2.5, CumulativeCount: 4},
			{UpperBound: 5.0, CumulativeCount: 5},
		},
	}

	lset := labels.FromStrings("__name__", "http_request_duration_seconds", "job", "test")
	ref, err := app.AppendHistogram(0, lset, 1000, hist, nil)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Verify index postings!
	ir, err := h.Index()
	require.NoError(t, err)
	defer ir.Close()

	// 1. Verify postings for base series
	pBase, err := ir.Postings(context.Background(), "__name__", "http_request_duration_seconds")
	require.NoError(t, err)
	baseRefs, err := index.ExpandPostings(pBase)
	require.NoError(t, err)
	require.Len(t, baseRefs, 1)
	require.Equal(t, ref, baseRefs[0])

	// 2. Verify postings for _count alias!
	pCount, err := ir.Postings(context.Background(), "__name__", "http_request_duration_seconds_count")
	require.NoError(t, err)
	countRefs, err := index.ExpandPostings(pCount)
	require.NoError(t, err)
	require.Len(t, countRefs, 1)
	require.NotEqual(t, uint64(0), uint64(countRefs[0])&virtualSeriesMask)

	// 3. Verify postings for _bucket alias with le="2.5"!
	pBucket, err := ir.Postings(context.Background(), "le", "2.5")
	require.NoError(t, err)
	bucketRefs, err := index.ExpandPostings(pBucket)
	require.NoError(t, err)
	require.Len(t, bucketRefs, 1)
	require.NotEqual(t, uint64(0), uint64(bucketRefs[0])&virtualSeriesMask)

	// 4. Verify Series labels spoofing!
	var builder labels.ScratchBuilder
	err = ir.Series(bucketRefs[0], &builder, nil)
	require.NoError(t, err)
	lbls := builder.Labels()
	require.Equal(t, "http_request_duration_seconds_bucket", lbls.Get("__name__"))
	require.Equal(t, "test", lbls.Get("job"))
	require.Equal(t, "2.5", lbls.Get("le"))

	// 5. Verify Chunk loading and value projection!
	var chks []chunks.Meta
	err = ir.Series(bucketRefs[0], &builder, &chks)
	require.NoError(t, err)
	require.Len(t, chks, 1)

	cr, err := h.Chunks()
	require.NoError(t, err)
	defer cr.Close()

	chk, _, err := cr.ChunkOrIterable(chks[0])
	require.NoError(t, err)
	require.Equal(t, chunkenc.EncXOR, chk.Encoding())

	it := chk.Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	tVal, fVal := it.At()
	require.Equal(t, int64(1000), tVal)
	require.Equal(t, 4.0, fVal) // CumulativeCount for UpperBound 2.5 is 4!
	require.Equal(t, chunkenc.ValNone, it.Next())
}
