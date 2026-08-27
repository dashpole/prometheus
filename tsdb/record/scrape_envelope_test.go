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

package record_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

func makeTestHistogram(schema int32, customValues []float64) *histogram.Histogram {
	h := &histogram.Histogram{
		Schema:        schema,
		ZeroThreshold: 0.001,
		ZeroCount:     5,
		Count:         42,
		Sum:           123.456,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []int64{10, 5, 3, 2},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
		},
		NegativeBuckets: []int64{7},
	}
	if len(customValues) > 0 {
		h.Schema = histogram.CustomBucketsSchema
		h.CustomValues = customValues
	}
	return h
}

func makeTestFloatHistogram(schema int32, customValues []float64) *histogram.FloatHistogram {
	fh := &histogram.FloatHistogram{
		Schema:        schema,
		ZeroThreshold: 0.001,
		ZeroCount:     5.5,
		Count:         42.5,
		Sum:           123.456,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
		},
		PositiveBuckets: []float64{10.1, 5.2},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
		},
		NegativeBuckets: []float64{7.3},
	}
	if len(customValues) > 0 {
		fh.Schema = histogram.CustomBucketsSchema
		fh.CustomValues = customValues
	}
	return fh
}

func TestScrapeEnvelope_RoundTrip(t *testing.T) {
	for _, enableST := range []bool{false, true} {
		t.Run(fmt.Sprintf("enableST=%v", enableST), func(t *testing.T) {
			enc := record.Encoder{EnableSTStorage: enableST}
			dec := record.NewDecoder(labels.NewSymbolTable(), promslog.NewNopLogger())

			env := record.ScrapeEnvelope{
				Floats: []record.RefSample{
					{Ref: 100, T: 1000, ST: 900, V: 1.23},
					{Ref: 101, T: 1000, ST: 900, V: 4.56},
					{Ref: 102, T: 1001, ST: 901, V: 7.89},
				},
				Histograms: []record.RefHistogramSample{
					{Ref: 100, T: 1000, ST: 900, H: makeTestHistogram(1, nil)},
					{Ref: 103, T: 1000, ST: 900, H: makeTestHistogram(0, []float64{1.0, 2.5, 5.0})},
				},
				FloatHistograms: []record.RefFloatHistogramSample{
					{Ref: 104, T: 1000, ST: 900, FH: makeTestFloatHistogram(1, nil)},
					{Ref: 105, T: 1000, ST: 900, FH: makeTestFloatHistogram(0, []float64{0.5, 1.5, 3.0})},
				},
				Exemplars: []record.RefExemplar{
					{Ref: 100, T: 1000, V: 1.23, Labels: labels.FromStrings("traceID", "abc-123")},
					{Ref: 101, T: 1000, V: 4.56, Labels: labels.FromStrings("traceID", "def-456")},
				},
				Metadata: []record.RefMetadata{
					{Ref: 100, Type: uint8(record.Counter), Unit: "seconds", Help: "Total seconds"},
					{Ref: 103, Type: uint8(record.HistogramSample), Unit: "bytes", Help: "Request size"},
				},
			}

			encoded := enc.ScrapeEnvelope(env, nil)
			require.Equal(t, record.ScrapeEnvelopes, dec.Type(encoded))

			decoded, err := dec.ScrapeEnvelope(encoded, nil)
			require.NoError(t, err)

			expectedFloats := env.Floats
			expectedHistograms := env.Histograms
			expectedFloatHistograms := env.FloatHistograms
			if !enableST {
				// ST is not preserved in V1 encoding.
				expectedFloats = make([]record.RefSample, len(env.Floats))
				for i, s := range env.Floats {
					expectedFloats[i] = s
					expectedFloats[i].ST = 0
				}
				expectedHistograms = make([]record.RefHistogramSample, len(env.Histograms))
				for i, h := range env.Histograms {
					expectedHistograms[i] = h
					expectedHistograms[i].ST = 0
				}
				expectedFloatHistograms = make([]record.RefFloatHistogramSample, len(env.FloatHistograms))
				for i, fh := range env.FloatHistograms {
					expectedFloatHistograms[i] = fh
					expectedFloatHistograms[i].ST = 0
				}
			}

			require.Equal(t, expectedFloats, decoded.Floats)
			require.Equal(t, expectedHistograms, decoded.Histograms)
			require.Equal(t, expectedFloatHistograms, decoded.FloatHistograms)
			require.Equal(t, env.Exemplars, decoded.Exemplars)
			require.Equal(t, env.Metadata, decoded.Metadata)

			// Test buffer pool reuse.
			pool := record.NewBuffersPool()
			pooledEnv := pool.GetScrapeEnvelope()
			pooledEnv, err = dec.ScrapeEnvelope(encoded, pooledEnv)
			require.NoError(t, err)
			require.Equal(t, expectedFloats, pooledEnv.Floats)
			pool.PutScrapeEnvelope(pooledEnv)
		})
	}
}

func TestScrapeEnvelope_Empty(t *testing.T) {
	enc := record.Encoder{}
	dec := record.NewDecoder(labels.NewSymbolTable(), promslog.NewNopLogger())

	var env record.ScrapeEnvelope
	require.True(t, env.IsEmpty())

	encoded := enc.ScrapeEnvelope(env, nil)
	require.Equal(t, record.ScrapeEnvelopes, dec.Type(encoded))

	decoded, err := dec.ScrapeEnvelope(encoded, nil)
	require.NoError(t, err)
	require.True(t, decoded.IsEmpty())
}

func TestScrapeEnvelope_CRCValidation(t *testing.T) {
	enc := record.Encoder{EnableSTStorage: true}
	dec := record.NewDecoder(labels.NewSymbolTable(), promslog.NewNopLogger())

	env := record.ScrapeEnvelope{
		Floats: []record.RefSample{
			{Ref: 100, T: 1000, ST: 900, V: 1.23},
			{Ref: 101, T: 1000, ST: 900, V: 4.56},
		},
		Exemplars: []record.RefExemplar{
			{Ref: 100, T: 1000, V: 1.23, Labels: labels.FromStrings("traceID", "abc-123")},
		},
	}

	encoded := enc.ScrapeEnvelope(env, nil)

	// Corrupt payload byte.
	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[len(corrupted)-1] ^= 0xFF

	_, err := dec.ScrapeEnvelope(corrupted, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, encoding.ErrInvalidChecksum), "expected ErrInvalidChecksum on corrupted payload, got: %v", err)

	// Corrupt CRC field.
	corruptedCRC := make([]byte, len(encoded))
	copy(corruptedCRC, encoded)
	corruptedCRC[4] ^= 0xFF

	_, err = dec.ScrapeEnvelope(corruptedCRC, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, encoding.ErrInvalidChecksum), "expected ErrInvalidChecksum on corrupted CRC, got: %v", err)

	// Record too short.
	_, err = dec.ScrapeEnvelope(encoded[:5], nil)
	require.Error(t, err)

	// Invalid type byte.
	invalidType := make([]byte, len(encoded))
	copy(invalidType, encoded)
	invalidType[0] = byte(record.Samples)
	_, err = dec.ScrapeEnvelope(invalidType, nil)
	require.Error(t, err)

	// Unsupported version.
	invalidVersion := make([]byte, len(encoded))
	copy(invalidVersion, encoded)
	invalidVersion[1] = 99
	// Recompute CRC so it passes CRC check and hits version check.
	crc := binary.BigEndian.Uint32(encoded[3:7])
	binary.BigEndian.PutUint32(invalidVersion[3:7], crc)
	_, err = dec.ScrapeEnvelope(invalidVersion, nil)
	require.Error(t, err)
}

func TestScrapeEnvelope_MultiPageBoundary(t *testing.T) {
	const pageSize = 32 * 1024 // 32KB
	dir := t.TempDir()

	w, err := wlog.NewSize(promslog.NewNopLogger(), nil, dir, 128*pageSize, compression.None)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	// Build a large envelope that exceeds 32KB (spanning 3-4 pages).
	numSamples := 3000
	numHistograms := 200
	numExemplars := 500

	env := record.ScrapeEnvelope{
		Floats:          make([]record.RefSample, numSamples),
		Histograms:      make([]record.RefHistogramSample, numHistograms),
		FloatHistograms: make([]record.RefFloatHistogramSample, numHistograms),
		Exemplars:       make([]record.RefExemplar, numExemplars),
	}

	for i := 0; i < numSamples; i++ {
		env.Floats[i] = record.RefSample{
			Ref: chunks.HeadSeriesRef(i + 1),
			T:   int64(1000 + i),
			ST:  int64(900 + i),
			V:   float64(i) * 1.5,
		}
	}
	for i := 0; i < numHistograms; i++ {
		env.Histograms[i] = record.RefHistogramSample{
			Ref: chunks.HeadSeriesRef(i + 1),
			T:   int64(1000 + i),
			ST:  int64(900 + i),
			H:   makeTestHistogram(1, nil),
		}
		env.FloatHistograms[i] = record.RefFloatHistogramSample{
			Ref: chunks.HeadSeriesRef(i + 1),
			T:   int64(1000 + i),
			ST:  int64(900 + i),
			FH:  makeTestFloatHistogram(1, nil),
		}
	}
	for i := 0; i < numExemplars; i++ {
		env.Exemplars[i] = record.RefExemplar{
			Ref:    chunks.HeadSeriesRef(i + 1),
			T:      int64(1000 + i),
			V:      float64(i) * 2.0,
			Labels: labels.FromStrings("traceID", fmt.Sprintf("trace-%08d", i)),
		}
	}

	enc := record.Encoder{EnableSTStorage: true}
	rec := enc.ScrapeEnvelope(env, nil)
	require.Greater(t, len(rec), pageSize*2, "expected record size to span multiple 32KB pages")

	// Log to WAL.
	err = w.Log(rec)
	require.NoError(t, err)

	// Verify reading via Reader.
	r, err := wlog.NewSegmentsReader(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	reader := wlog.NewReader(r)
	require.True(t, reader.Next())
	require.NoError(t, reader.Err())

	readRec := reader.Record()
	require.Equal(t, len(rec), len(readRec))

	dec := record.NewDecoder(labels.NewSymbolTable(), promslog.NewNopLogger())
	require.Equal(t, record.ScrapeEnvelopes, dec.Type(readRec))

	decoded, err := dec.ScrapeEnvelope(readRec, nil)
	require.NoError(t, err)
	require.Equal(t, env.Floats, decoded.Floats)
	require.Equal(t, env.Histograms, decoded.Histograms)
	require.Equal(t, env.FloatHistograms, decoded.FloatHistograms)
	require.Equal(t, env.Exemplars, decoded.Exemplars)

	// Verify reading via LiveReader.
	rLive, err := wlog.NewSegmentsReader(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rLive.Close()) })

	lr := wlog.NewLiveReader(promslog.NewNopLogger(), nil, rLive)
	require.True(t, lr.Next())
	require.NoError(t, lr.Err())
	decodedLive, err := dec.ScrapeEnvelope(lr.Record(), nil)
	require.NoError(t, err)
	require.Equal(t, env.Floats, decodedLive.Floats)
}

func TestScrapeEnvelope_CorruptedChunks(t *testing.T) {
	const pageSize = 32 * 1024
	dir := t.TempDir()

	w, err := wlog.NewSize(promslog.NewNopLogger(), nil, dir, 128*pageSize, compression.None)
	require.NoError(t, err)

	// Create multi-page envelope.
	env := record.ScrapeEnvelope{
		Floats: make([]record.RefSample, 3000),
	}
	for i := range env.Floats {
		env.Floats[i] = record.RefSample{
			Ref: chunks.HeadSeriesRef(i + 1),
			T:   int64(1000 + i),
			ST:  int64(900 + i),
			V:   float64(i),
		}
	}
	enc := record.Encoder{EnableSTStorage: true}
	rec := enc.ScrapeEnvelope(env, nil)
	require.NoError(t, w.Log(rec))
	require.NoError(t, w.Close())

	// Corrupt a byte in the middle of the second 32KB page.
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, files)

	segFile := filepath.Join(dir, files[0].Name())
	data, err := os.ReadFile(segFile)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), pageSize*2)

	// Corrupt byte at page 1 offset (32KB + 100).
	data[pageSize+100] ^= 0xFF
	require.NoError(t, os.WriteFile(segFile, data, 0666))

	// Reader should detect CRC corruption and return error.
	r, err := wlog.NewSegmentsReader(dir)
	require.NoError(t, err)
	defer r.Close()

	reader := wlog.NewReader(r)
	hasRecord := reader.Next()
	// Either Next returns false with an error, or the reconstructed record fails envelope CRC.
	if !hasRecord {
		require.Error(t, reader.Err())
	} else {
		dec := record.NewDecoder(labels.NewSymbolTable(), promslog.NewNopLogger())
		_, decErr := dec.ScrapeEnvelope(reader.Record(), nil)
		require.Error(t, decErr)
	}
}
