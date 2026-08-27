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

package wlog

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/util/compression"
)

func TestWALWatcher_PreShardCorrelator(t *testing.T) {
	t.Run("DecodesAndDispatchesScrapeEnvelope", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		require.NoError(t, os.MkdirAll(walDir, 0o777))
		wal, err := New(promslog.NewNopLogger(), nil, walDir, compression.None)
		require.NoError(t, err)

		var enc record.Encoder
		// 1. Series
		series := []record.RefSeries{
			{Ref: 1, Labels: labels.FromStrings("__name__", "metric_1", "job", "test")},
			{Ref: 2, Labels: labels.FromStrings("__name__", "metric_2", "job", "test")},
		}
		require.NoError(t, wal.Log(enc.Series(series, nil)))

		// 2. ScrapeEnvelope
		env := record.ScrapeEnvelope{
			Floats: []record.RefSample{
				{Ref: 1, T: 1000, V: 42.0},
			},
			Histograms: []record.RefHistogramSample{
				{Ref: 2, T: 1000, H: &histogram.Histogram{Schema: 1, Count: 5, Sum: 10}},
			},
			Exemplars: []record.RefExemplar{
				{Ref: 1, T: 1000, V: 42.0, Labels: labels.FromStrings("trace_id", "abc")},
			},
			Metadata: []record.RefMetadata{
				{Ref: 1, Type: 1, Unit: "bytes", Help: "metric one"},
			},
		}
		require.NoError(t, wal.Log(enc.ScrapeEnvelope(env, nil)))
		require.NoError(t, wal.Close())

		wtm := &writeToMock{
			seriesSegmentIndexes: make(map[chunks.HeadSeriesRef]int),
		}

		watcher := NewWatcher(
			NewWatcherMetrics(prometheus.NewRegistry()),
			NewLiveReaderMetrics(prometheus.NewRegistry()),
			promslog.NewNopLogger(),
			"test",
			wtm,
			dir,
			true, // sendExemplars
			true, // sendHistograms
			true, // sendMetadata
			record.NewBuffersPool(),
		)
		watcher.startTime = time.UnixMilli(0)
		watcher.startTimestamp = 0
		watcher.SetMetrics()

		sr, err := NewSegmentsReader(walDir)
		require.NoError(t, err)
		lr := NewLiveReader(promslog.NewNopLogger(), nil, sr)
		readErr := watcher.readSegment(lr, 0, true)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			require.NoError(t, readErr)
		}
		_ = sr.Close()

		wtm.mu.Lock()
		defer wtm.mu.Unlock()

		require.Equal(t, 1, wtm.scrapeEnvelopesAppends, "expected 1 ScrapeEnvelope dispatched")
		require.Len(t, wtm.samplesAppended, 1)
		require.Len(t, wtm.histogramsAppended, 1)
		require.Len(t, wtm.exemplarsAppended, 1)
		require.Len(t, wtm.metadataStored, 1)
	})

	t.Run("NonTailing_StripsSamples_KeepsMetadata", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		require.NoError(t, os.MkdirAll(walDir, 0o777))
		wal, err := New(promslog.NewNopLogger(), nil, walDir, compression.None)
		require.NoError(t, err)

		var enc record.Encoder
		env := record.ScrapeEnvelope{
			Floats:    []record.RefSample{{Ref: 1, T: 1000, V: 42.0}},
			Exemplars: []record.RefExemplar{{Ref: 1, T: 1000, V: 42.0, Labels: labels.FromStrings("trace_id", "abc")}},
			Metadata:  []record.RefMetadata{{Ref: 1, Type: 1, Unit: "bytes", Help: "metric one"}},
		}
		require.NoError(t, wal.Log(enc.ScrapeEnvelope(env, nil)))
		require.NoError(t, wal.Close())

		wtm := &writeToMock{
			seriesSegmentIndexes: make(map[chunks.HeadSeriesRef]int),
		}

		watcher := NewWatcher(
			NewWatcherMetrics(prometheus.NewRegistry()),
			NewLiveReaderMetrics(prometheus.NewRegistry()),
			promslog.NewNopLogger(),
			"test",
			wtm,
			dir,
			true,
			true,
			true,
			record.NewBuffersPool(),
		)
		watcher.startTime = time.UnixMilli(0)
		watcher.startTimestamp = 0
		watcher.SetMetrics()

		sr, err := NewSegmentsReader(walDir)
		require.NoError(t, err)
		lr := NewLiveReader(promslog.NewNopLogger(), nil, sr)
		readErr := watcher.readSegment(lr, 0, false) // tail = false
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			require.NoError(t, readErr)
		}
		_ = sr.Close()

		wtm.mu.Lock()
		defer wtm.mu.Unlock()

		require.Equal(t, 0, wtm.scrapeEnvelopesAppends, "no ScrapeEnvelope should be appended when tail=false")
		require.Empty(t, wtm.samplesAppended)
		require.Empty(t, wtm.exemplarsAppended)
		require.Len(t, wtm.metadataStored, 1, "metadata should still be stored when tail=false")
	})

	t.Run("ExemplarsStrippedWhenDisabled", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		require.NoError(t, os.MkdirAll(walDir, 0o777))
		wal, err := New(promslog.NewNopLogger(), nil, walDir, compression.None)
		require.NoError(t, err)

		var enc record.Encoder
		env := record.ScrapeEnvelope{
			Floats:    []record.RefSample{{Ref: 1, T: 1000, V: 42.0}},
			Exemplars: []record.RefExemplar{{Ref: 1, T: 1000, V: 42.0, Labels: labels.FromStrings("trace_id", "abc")}},
		}
		require.NoError(t, wal.Log(enc.ScrapeEnvelope(env, nil)))
		require.NoError(t, wal.Close())

		wtm := &writeToMock{
			seriesSegmentIndexes: make(map[chunks.HeadSeriesRef]int),
		}

		watcher := NewWatcher(
			NewWatcherMetrics(prometheus.NewRegistry()),
			NewLiveReaderMetrics(prometheus.NewRegistry()),
			promslog.NewNopLogger(),
			"test",
			wtm,
			dir,
			false, // sendExemplars = false
			true,
			true,
			record.NewBuffersPool(),
		)
		watcher.startTime = time.UnixMilli(0)
		watcher.startTimestamp = 0
		watcher.SetMetrics()

		sr, err := NewSegmentsReader(walDir)
		require.NoError(t, err)
		lr := NewLiveReader(promslog.NewNopLogger(), nil, sr)
		readErr := watcher.readSegment(lr, 0, true)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			require.NoError(t, readErr)
		}
		_ = sr.Close()

		wtm.mu.Lock()
		defer wtm.mu.Unlock()

		require.Equal(t, 1, wtm.scrapeEnvelopesAppends)
		require.Len(t, wtm.samplesAppended, 1)
		require.Empty(t, wtm.exemplarsAppended, "exemplars should have been stripped when sendExemplars=false")
	})
}

func BenchmarkWALWatcher_Correlator(b *testing.B) {
	dir := b.TempDir()
	walDir := filepath.Join(dir, "wal")
	require.NoError(b, os.MkdirAll(walDir, 0o777))
	wal, err := New(promslog.NewNopLogger(), nil, walDir, compression.None)
	require.NoError(b, err)

	var enc record.Encoder
	// 100 series, 100 samples + 20 exemplars per scrape
	const numSeries = 100
	for s := 1; s <= numSeries; s++ {
		require.NoError(b, wal.Log(enc.Series([]record.RefSeries{
			{Ref: chunks.HeadSeriesRef(s), Labels: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", s))},
		}, nil)))
	}

	env := record.ScrapeEnvelope{
		Floats:    make([]record.RefSample, 0, numSeries),
		Exemplars: make([]record.RefExemplar, 0, 20),
	}
	for s := 1; s <= numSeries; s++ {
		env.Floats = append(env.Floats, record.RefSample{Ref: chunks.HeadSeriesRef(s), T: 1000, V: rand.Float64()})
		if s <= 20 {
			env.Exemplars = append(env.Exemplars, record.RefExemplar{
				Ref:    chunks.HeadSeriesRef(s),
				T:      1000,
				V:      rand.Float64(),
				Labels: labels.FromStrings("trace_id", "trace-xyz"),
			})
		}
	}

	for i := 0; i < 50; i++ {
		require.NoError(b, wal.Log(enc.ScrapeEnvelope(env, nil)))
	}
	require.NoError(b, wal.Close())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		wtm := &writeToMock{seriesSegmentIndexes: make(map[chunks.HeadSeriesRef]int)}
		watcher := NewWatcher(
			NewWatcherMetrics(prometheus.NewRegistry()),
			NewLiveReaderMetrics(prometheus.NewRegistry()),
			promslog.NewNopLogger(),
			"bench",
			wtm,
			dir,
			true,
			true,
			true,
			record.NewBuffersPool(),
		)
		watcher.startTime = time.UnixMilli(0)
		watcher.startTimestamp = 0
		watcher.MaxSegment = 0
		watcher.SetMetrics()

		sr, err := NewSegmentsReader(walDir)
		if err != nil {
			b.Fatal(err)
		}
		lr := NewLiveReader(promslog.NewNopLogger(), nil, sr)
		readErr := watcher.readSegment(lr, 0, true)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			b.Fatal(readErr)
		}
		_ = sr.Close()
	}
}
