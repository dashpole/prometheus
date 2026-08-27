// Copyright 2026 The Prometheus Authors
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

	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

func TestHead_ScrapeEnvelope_CommitAndReplay(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultHeadOptions()
	opts.ChunkRange = 100000
	opts.EnableExemplarStorage = true
	opts.MaxExemplars.Store(1000)
	opts.EnableMetadataWALRecords = true

	w, err := wlog.New(promslog.NewNopLogger(), nil, dir, compression.Snappy)
	require.NoError(t, err)

	h, err := NewHead(nil, promslog.NewNopLogger(), w, nil, opts, nil)
	require.NoError(t, err)
	require.NoError(t, h.Init(0))

	app := h.AppenderV2(context.Background())

	// 1. Float series
	lbls1 := labels.FromStrings("__name__", "http_requests_total", "method", "POST")
	_, err = app.Append(0, lbls1, 0, 1000, 42.0, nil, nil, storage.AOptions{
		Exemplars: []exemplar.Exemplar{{
			Labels: labels.FromStrings("trace_id", "abc-123"),
			Value:  42.0,
			Ts:     1000,
			HasTs:  true,
		}},
		Metadata: metadata.Metadata{
			Type: model.MetricTypeCounter,
			Unit: "requests",
			Help: "Total HTTP requests",
		},
	})
	require.NoError(t, err)

	// 2. Exponential histogram
	h1 := &histogram.Histogram{
		Schema:          1,
		Count:           13,
		Sum:             15.5,
		ZeroThreshold:   0.001,
		ZeroCount:       2,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []int64{3, 5}, // bucket counts: 3, 3+5=8 => 3+8=11 + ZeroCount(2) = 13
	}
	lbls2 := labels.FromStrings("__name__", "request_duration_seconds", "handler", "query")
	_, err = app.Append(0, lbls2, 0, 1000, 0, h1, nil, storage.AOptions{})
	require.NoError(t, err)

	// 3. Float histogram
	fh1 := &histogram.FloatHistogram{
		Schema:          1,
		Count:           10.5,
		Sum:             15.5,
		ZeroThreshold:   0.001,
		ZeroCount:       2.5,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []float64{3.0, 5.0}, // bucket counts: 3.0, 5.0 => 3.0+5.0=8.0 + ZeroCount(2.5) = 10.5
	}
	lbls3 := labels.FromStrings("__name__", "request_duration_float_seconds", "handler", "query")
	_, err = app.Append(0, lbls3, 0, 1000, 0, nil, fh1, storage.AOptions{})
	require.NoError(t, err)

	require.NoError(t, app.Commit())
	require.NoError(t, h.Close())

	// Inspect WAL records to verify they were written as ScrapeEnvelopes.
	sr, err := wlog.NewSegmentsReader(dir)
	require.NoError(t, err)
	r := wlog.NewReader(sr)
	dec := record.NewDecoder(labels.NewSymbolTable(), promslog.NewNopLogger())

	var scrapeEnvelopesFound int
	for r.Next() {
		rec := r.Record()
		if dec.Type(rec) == record.ScrapeEnvelopes {
			scrapeEnvelopesFound++
			var env record.ScrapeEnvelope
			_, err := dec.ScrapeEnvelope(rec, &env)
			require.NoError(t, err)
			require.Len(t, env.Floats, 1)
			require.Len(t, env.Histograms, 1)
			require.Len(t, env.FloatHistograms, 1)
			require.Len(t, env.Exemplars, 1)
			require.Len(t, env.Metadata, 1)
		}
	}
	require.NoError(t, r.Err())
	require.NoError(t, sr.Close())
	require.Equal(t, 1, scrapeEnvelopesFound, "expected exactly 1 ScrapeEnvelope in WAL")

	// Replay WAL into a fresh Head.
	w2, err := wlog.New(promslog.NewNopLogger(), nil, dir, compression.Snappy)
	require.NoError(t, err)
	defer w2.Close()

	h2, err := NewHead(nil, promslog.NewNopLogger(), w2, nil, opts, nil)
	require.NoError(t, err)
	defer h2.Close()

	require.NoError(t, h2.Init(0))

	// Verify series and samples were correctly loaded by querying the new Head.
	q, err := NewBlockQuerier(h2, 0, 2000)
	require.NoError(t, err)
	defer q.Close()

	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"))
	require.True(t, ss.Next())
	series := ss.At()
	it := series.Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	t1, v1 := it.At()
	require.Equal(t, int64(1000), t1)
	require.Equal(t, 42.0, v1)
	require.Equal(t, chunkenc.ValNone, it.Next())
	require.False(t, ss.Next())

	// Verify histogram query
	ssH := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_seconds"))
	require.True(t, ssH.Next())
	seriesH := ssH.At()
	itH := seriesH.Iterator(nil)
	require.Equal(t, chunkenc.ValHistogram, itH.Next())
	tH, hGot := itH.AtHistogram(nil)
	require.Equal(t, int64(1000), tH)
	require.Equal(t, h1.Count, hGot.Count)
	require.Equal(t, h1.Sum, hGot.Sum)
	require.Equal(t, chunkenc.ValNone, itH.Next())
	require.False(t, ssH.Next())

	// Verify float histogram query
	ssFH := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_float_seconds"))
	require.True(t, ssFH.Next())
	seriesFH := ssFH.At()
	itFH := seriesFH.Iterator(nil)
	require.Equal(t, chunkenc.ValFloatHistogram, itFH.Next())
	tFH, fhGot := itFH.AtFloatHistogram(nil)
	require.Equal(t, int64(1000), tFH)
	require.Equal(t, fh1.Count, fhGot.Count)
	require.Equal(t, fh1.Sum, fhGot.Sum)
	require.Equal(t, chunkenc.ValNone, itFH.Next())
	require.False(t, ssFH.Next())

	// Verify metadata was replayed correctly
	ms := h2.series.getByHash(lbls1.Hash(), lbls1)
	require.NotNil(t, ms)
	require.NotNil(t, ms.meta)
	require.Equal(t, model.MetricTypeCounter, ms.meta.Type)
	require.Equal(t, "requests", ms.meta.Unit)
	require.Equal(t, "Total HTTP requests", ms.meta.Help)

	// Verify exemplar was replayed correctly
	eq, err := h2.ExemplarQuerier(context.Background())
	require.NoError(t, err)
	exs, err := eq.Select(0, 2000, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total")})
	require.NoError(t, err)
	require.Len(t, exs, 1)
	require.Len(t, exs[0].Exemplars, 1)
	require.Equal(t, 42.0, exs[0].Exemplars[0].Value)
	require.Equal(t, int64(1000), exs[0].Exemplars[0].Ts)
}
