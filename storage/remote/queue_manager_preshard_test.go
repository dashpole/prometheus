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

package remote

import (
	"testing"
	"time"

	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/common/model"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/record"
)

func newTestQueueManagerWithFlags(t testing.TB, cfg config.QueueConfig, sendExemplars, sendHistograms, sendMetadata bool, protoMsg remoteapi.WriteMessageType) *QueueManager {
	dir := t.TempDir()
	metrics := newQueueManagerMetrics(nil, "", "")
	c := NewTestWriteClient(protoMsg)
	mcfg := config.DefaultMetadataConfig
	mcfg.Send = sendMetadata
	m := NewQueueManager(metrics, nil, nil, nil, dir, newEWMARate(ewmaWeight, shardUpdateDuration), cfg, mcfg, labels.EmptyLabels(), nil, c, 100*time.Millisecond, newPool(), newHighestTimestampMetric(), nil, sendExemplars, sendHistograms, sendMetadata, protoMsg, record.NewBuffersPool(), false)
	return m
}

func TestQueueManager_PRW2(t *testing.T) {
	cfg := config.QueueConfig{
		Capacity:          100,
		MaxSamplesPerSend: 100,
		BatchSendDeadline: model.Duration(100 * time.Millisecond),
		MinBackoff:        model.Duration(10 * time.Millisecond),
		MaxBackoff:        model.Duration(100 * time.Millisecond),
		MaxShards:         1,
		MinShards:         1,
		SampleAgeLimit:    model.Duration(24 * time.Hour),
	}

	t.Run("PreShardCorrelation_AttachedExemplars_PRW2", func(t *testing.T) {
		m := newTestQueueManagerWithFlags(t, cfg, true, true, true, remoteapi.WriteV2MessageType)
		m.numShards = 1
		m.shards.queues = []*queue{newQueue(100, 100)}

		// Store series
		m.StoreSeries([]record.RefSeries{
			{Ref: 1, Labels: labels.FromStrings("__name__", "http_requests_total", "method", "POST")},
			{Ref: 2, Labels: labels.FromStrings("__name__", "request_duration_seconds", "handler", "query")},
			{Ref: 3, Labels: labels.FromStrings("__name__", "request_duration_float_seconds", "handler", "query")},
			{Ref: 4, Labels: labels.FromStrings("__name__", "orphan_metric", "handler", "query")},
		}, 0)

		// Store metadata
		m.StoreMetadata([]record.RefMetadata{
			{Ref: 1, Type: 1, Unit: "requests", Help: "Total requests"},
		})

		// Append scrape envelope with paired exemplars on series 1, 2, 3 and orphan exemplar on series 4
		h1 := &histogram.Histogram{Schema: 1, Count: 5, Sum: 10}
		fh1 := &histogram.FloatHistogram{Schema: 1, Count: 5.5, Sum: 10.5}

		now := time.Now().UnixMilli()

		env := record.ScrapeEnvelope{
			Floats: []record.RefSample{
				{Ref: 1, T: now, V: 42.0, ST: now - 500},
			},
			Histograms: []record.RefHistogramSample{
				{Ref: 2, T: now, H: h1, ST: now - 500},
			},
			FloatHistograms: []record.RefFloatHistogramSample{
				{Ref: 3, T: now, FH: fh1, ST: now - 500},
			},
			Exemplars: []record.RefExemplar{
				{Ref: 1, T: now, V: 42.0, Labels: labels.FromStrings("trace_id", "trace-1")},
				{Ref: 2, T: now, V: 10.0, Labels: labels.FromStrings("trace_id", "trace-2")},
				{Ref: 3, T: now, V: 10.5, Labels: labels.FromStrings("trace_id", "trace-3")},
				{Ref: 4, T: now, V: 99.0, Labels: labels.FromStrings("trace_id", "trace-orphan")},
			},
		}

		require.True(t, m.AppendScrapeEnvelope(env))

		// Check the queued batch on shard 0
		q := m.shards.queues[0]
		q.batchMtx.Lock()
		batch := make([]timeSeries, len(q.batch))
		copy(batch, q.batch)
		q.batchMtx.Unlock()

		require.Len(t, batch, 4, "expected 4 timeSeries queued (3 with attached exemplars, 1 orphan)")

		// Build PRW 2.0 payload via populateV2TimeSeries
		symTable := writev2.NewSymbolTable()
		pendingData := make([]writev2.TimeSeries, len(batch))
		nSamples, nExemplars, nHistograms, _, _ := populateV2TimeSeries(
			&symTable,
			batch,
			pendingData,
			true,  // sendExemplars
			true,  // sendNativeHistograms
			false, // enableTypeAndUnitLabels
		)

		require.Equal(t, 1, nSamples)
		require.Equal(t, 4, nExemplars)
		require.Equal(t, 2, nHistograms)

		// Assert that Series 1, 2, 3 have their exemplars attached to the sample's TimeSeries (0 empty TimeSeries)
		// Series 1: 1 Sample + 1 Exemplar
		require.Len(t, pendingData[0].Samples, 1)
		require.Len(t, pendingData[0].Exemplars, 1)
		require.Equal(t, 42.0, pendingData[0].Samples[0].Value)
		require.Equal(t, now, pendingData[0].Samples[0].Timestamp)
		require.Equal(t, now-500, pendingData[0].Samples[0].StartTimestamp)
		require.Equal(t, 42.0, pendingData[0].Exemplars[0].Value)

		// Series 2: 1 Histogram + 1 Exemplar
		require.Len(t, pendingData[1].Histograms, 1)
		require.Len(t, pendingData[1].Exemplars, 1)
		require.Equal(t, 10.0, pendingData[1].Exemplars[0].Value)

		// Series 3: 1 FloatHistogram + 1 Exemplar
		require.Len(t, pendingData[2].Histograms, 1)
		require.Len(t, pendingData[2].Exemplars, 1)
		require.Equal(t, 10.5, pendingData[2].Exemplars[0].Value)

		// Series 4 (orphan exemplar): 0 Samples, 1 Exemplar
		require.Empty(t, pendingData[3].Samples)
		require.Len(t, pendingData[3].Exemplars, 1)
		require.Equal(t, 99.0, pendingData[3].Exemplars[0].Value)

		// Ensure no completely empty TimeSeries exist
		for i, ts := range pendingData {
			hasContent := len(ts.Samples) > 0 || len(ts.Histograms) > 0 || len(ts.Exemplars) > 0
			require.True(t, hasContent, "timeSeries index %d has no samples, histograms, or exemplars", i)
		}
	})

	t.Run("ExemplarsDisabled_PRW2", func(t *testing.T) {
		m := newTestQueueManagerWithFlags(t, cfg, false, true, true, remoteapi.WriteV2MessageType)
		m.numShards = 1
		m.shards.queues = []*queue{newQueue(100, 100)}

		m.StoreSeries([]record.RefSeries{
			{Ref: 1, Labels: labels.FromStrings("__name__", "http_requests_total")},
		}, 0)

		now := time.Now().UnixMilli()

		env := record.ScrapeEnvelope{
			Floats:    []record.RefSample{{Ref: 1, T: now, V: 42.0}},
			Exemplars: []record.RefExemplar{{Ref: 1, T: now, V: 42.0, Labels: labels.FromStrings("trace_id", "trace-1")}},
		}

		require.True(t, m.AppendScrapeEnvelope(env))

		q := m.shards.queues[0]
		q.batchMtx.Lock()
		batch := make([]timeSeries, len(q.batch))
		copy(batch, q.batch)
		q.batchMtx.Unlock()

		require.Len(t, batch, 1)
		require.Empty(t, batch[0].exemplars, "exemplars should not be attached when sendExemplars=false")
	})
}
