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

package remote

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/teststorage"
)

func TestSampledReadEndpoint(t *testing.T) {
	store := promqltest.LoadedStorage(t, `
		load 1m
			test_metric1{foo="bar",baz="qux"} 1
	`)
	defer store.Close()

	addNativeHistogramsToTestSuite(t, store, 1)

	h := NewReadHandler(nil, nil, store, func() config.Config {
		return config.Config{
			GlobalConfig: config.GlobalConfig{
				// We expect external labels to be added, with the source labels honored.
				ExternalLabels: labels.FromStrings("b", "c", "baz", "a", "d", "e"),
			},
		}
	}, 1e6, 1, 0)

	// Encode the request.
	matcher1, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric1")
	require.NoError(t, err)

	matcher2, err := labels.NewMatcher(labels.MatchEqual, "d", "e")
	require.NoError(t, err)

	matcher3, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_histogram_metric1")
	require.NoError(t, err)

	matcher4, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_nhcb_metric1")
	require.NoError(t, err)

	query1, err := ToQuery(0, 1, []*labels.Matcher{matcher1, matcher2}, &storage.SelectHints{Step: 0, Func: "avg"})
	require.NoError(t, err)

	query2, err := ToQuery(0, 1, []*labels.Matcher{matcher3, matcher2}, &storage.SelectHints{Step: 0, Func: "avg"})
	require.NoError(t, err)

	query3, err := ToQuery(0, 1, []*labels.Matcher{matcher4, matcher2}, &storage.SelectHints{Step: 0, Func: "avg"})
	require.NoError(t, err)

	req := &prompb.ReadRequest{Queries: []*prompb.Query{query1, query2, query3}}
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	compressed := snappy.Encode(nil, data)
	request, err := http.NewRequest(http.MethodPost, "", bytes.NewBuffer(compressed))
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)

	require.Equal(t, 2, recorder.Code/100)

	require.Equal(t, "application/x-protobuf", recorder.Result().Header.Get("Content-Type"))
	require.Equal(t, "snappy", recorder.Result().Header.Get("Content-Encoding"))

	// Decode the response.
	compressed, err = io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	uncompressed, err := snappy.Decode(nil, compressed)
	require.NoError(t, err)

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	require.NoError(t, err)

	require.Len(t, resp.Results, 3, "Expected 3 results.")

	require.Equal(t, &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "test_metric1"},
					{Name: "b", Value: "c"},
					{Name: "baz", Value: "qux"},
					{Name: "d", Value: "e"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 0}},
			},
		},
	}, resp.Results[0])

	require.Equal(t, &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "test_histogram_metric1"},
					{Name: "b", Value: "c"},
					{Name: "baz", Value: "qux"},
					{Name: "d", Value: "e"},
				},
				Histograms: []prompb.Histogram{
					prompb.FromFloatHistogram(0, tsdbutil.GenerateTestFloatHistogram(0)),
				},
			},
		},
	}, resp.Results[1])

	require.Equal(t, &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "test_nhcb_metric1"},
					{Name: "b", Value: "c"},
					{Name: "baz", Value: "qux"},
					{Name: "d", Value: "e"},
				},
				Histograms: []prompb.Histogram{{
					// We cannot use prompb.FromFloatHistogram as that's one
					// of the things we are testing here.
					Schema:    histogram.CustomBucketsSchema,
					Count:     &prompb.Histogram_CountFloat{CountFloat: 5},
					Sum:       18.4,
					ZeroCount: &prompb.Histogram_ZeroCountFloat{},
					PositiveSpans: []prompb.BucketSpan{
						{Offset: 0, Length: 2},
						{Offset: 1, Length: 2},
					},
					PositiveCounts: []float64{1, 2, 1, 1},
					CustomValues:   []float64{0, 1, 2, 3, 4},
				}},
			},
		},
	}, resp.Results[2])
}

func BenchmarkStreamReadEndpoint(b *testing.B) {
	store := promqltest.LoadedStorage(b, `
	load 1m
		test_metric1{foo="bar1",baz="qux"} 0+100x119
		test_metric1{foo="bar2",baz="qux"} 0+100x120
		test_metric1{foo="bar3",baz="qux"} 0+100x240
	`)

	b.Cleanup(func() { store.Close() })

	api := NewReadHandler(nil, nil, store, func() config.Config {
		return config.Config{}
	},
		0, 1, 0,
	)

	matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric1")
	require.NoError(b, err)

	query, err := ToQuery(0, 14400001, []*labels.Matcher{matcher}, &storage.SelectHints{
		Step:  1,
		Func:  "sum",
		Start: 0,
		End:   14400001,
	})
	require.NoError(b, err)

	req := &prompb.ReadRequest{
		Queries:               []*prompb.Query{query},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
	}
	data, err := proto.Marshal(req)
	require.NoError(b, err)

	b.ReportAllocs()

	for b.Loop() {
		compressed := snappy.Encode(nil, data)
		request, err := http.NewRequest(http.MethodPost, "", bytes.NewBuffer(compressed))
		require.NoError(b, err)

		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, request)

		require.Equal(b, 2, recorder.Code/100)

		var results []*prompb.ChunkedReadResponse
		stream := NewChunkedReader(recorder.Result().Body, config.DefaultChunkedReadLimit, nil)

		for {
			res := &prompb.ChunkedReadResponse{}
			err := stream.NextProto(res)
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(b, err)
			results = append(results, res)
		}

		require.Len(b, results, 6, "Expected 6 results.")
	}
}

func TestStreamReadEndpoint(t *testing.T) {
	// First with 120 float samples. We expect 1 frame with 1 chunk.
	// Second with 121 float samples, We expect 1 frame with 2 chunks.
	// Third with 241 float samples. We expect 1 frame with 2 chunks, and 1 frame with 1 chunk for the same series due to bytes limit.
	// Fourth with 25 histogram samples. We expect 1 frame with 1 chunk.
	store := promqltest.LoadedStorage(t, `
		load 1m
			test_metric1{foo="bar1",baz="qux"} 0+100x119
			test_metric1{foo="bar2",baz="qux"} 0+100x120
			test_metric1{foo="bar3",baz="qux"} 0+100x240
	`)
	defer store.Close()

	addNativeHistogramsToTestSuite(t, store, 25)

	api := NewReadHandler(nil, nil, store, func() config.Config {
		return config.Config{
			GlobalConfig: config.GlobalConfig{
				// We expect external labels to be added, with the source labels honored.
				ExternalLabels: labels.FromStrings("baz", "a", "b", "c", "d", "e"),
			},
		}
	},
		1e6, 1,
		// Labelset has 57 bytes. Full chunk in test data has roughly 240 bytes. This allows us to have at max 2 chunks in this test.
		57+480,
	)

	// Encode the request.
	matcher1, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric1")
	require.NoError(t, err)

	matcher2, err := labels.NewMatcher(labels.MatchEqual, "d", "e")
	require.NoError(t, err)

	matcher3, err := labels.NewMatcher(labels.MatchEqual, "foo", "bar1")
	require.NoError(t, err)

	matcher4, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_histogram_metric1")
	require.NoError(t, err)

	query1, err := ToQuery(0, 14400001, []*labels.Matcher{matcher1, matcher2}, &storage.SelectHints{
		Step:  1,
		Func:  "avg",
		Start: 0,
		End:   14400001,
	})
	require.NoError(t, err)

	query2, err := ToQuery(0, 14400001, []*labels.Matcher{matcher1, matcher3}, &storage.SelectHints{
		Step:  1,
		Func:  "avg",
		Start: 0,
		End:   14400001,
	})
	require.NoError(t, err)

	query3, err := ToQuery(0, 14400001, []*labels.Matcher{matcher4}, &storage.SelectHints{
		Step:  1,
		Func:  "avg",
		Start: 0,
		End:   14400001,
	})
	require.NoError(t, err)

	req := &prompb.ReadRequest{
		Queries:               []*prompb.Query{query1, query2, query3},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
	}
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	compressed := snappy.Encode(nil, data)
	request, err := http.NewRequest(http.MethodPost, "", bytes.NewBuffer(compressed))
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		panic(fmt.Sprintf("HTTP_CODE_FAIL: %d, BODY: %s", recorder.Code, recorder.Body.String()))
	}

	require.Equal(t, 2, recorder.Code/100)

	require.Equal(t, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse", recorder.Result().Header.Get("Content-Type"))
	require.Empty(t, recorder.Result().Header.Get("Content-Encoding"))

	var results []*prompb.ChunkedReadResponse
	stream := NewChunkedReader(recorder.Result().Body, config.DefaultChunkedReadLimit, nil)
	for {
		res := &prompb.ChunkedReadResponse{}
		err := stream.NextProto(res)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}

	require.Len(t, results, 6, "Expected 6 results.")

	require.Equal(t, []*prompb.ChunkedReadResponse{
		{
			ChunkedSeries: []*prompb.ChunkedSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
						{Name: "foo", Value: "bar1"},
					},
					Chunks: []prompb.Chunk{
						{
							Type:      prompb.Chunk_XOR,
							MaxTimeMs: 7140000,
							Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
						},
					},
				},
			},
		},
		{
			ChunkedSeries: []*prompb.ChunkedSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
						{Name: "foo", Value: "bar2"},
					},
					Chunks: []prompb.Chunk{
						{
							Type:      prompb.Chunk_XOR,
							MaxTimeMs: 7140000,
							Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
						},
						{
							Type:      prompb.Chunk_XOR,
							MinTimeMs: 7200000,
							MaxTimeMs: 7200000,
							Data:      []byte("\000\001\200\364\356\006@\307p\000\000\000\000\000"),
						},
					},
				},
			},
		},
		{
			ChunkedSeries: []*prompb.ChunkedSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
						{Name: "foo", Value: "bar3"},
					},
					Chunks: []prompb.Chunk{
						{
							Type:      prompb.Chunk_XOR,
							MaxTimeMs: 7140000,
							Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
						},
						{
							Type:      prompb.Chunk_XOR,
							MinTimeMs: 7200000,
							MaxTimeMs: 14340000,
							Data:      []byte("\000x\200\364\356\006@\307p\000\000\000\000\000\340\324\003\340>\224\355\260\277\322\200\372\005(=\240R\207:\003(\025\240\362\201z\003(\365\240r\203:\005(\r\241\322\201\372\r(\r\240R\237:\007(5\2402\201z\037(\025\2402\203:\005(\375\240R\200\372\r(\035\241\322\201:\003(5\240r\326g\364\271\213\227!\253q\037\312N\340GJ\033E)\375\024\241\266\362}(N\217(V\203)\336\207(\326\203(N\334W\322\203\2644\240}\005(\373AJ\031\3202\202\264\374\240\275\003(kA\3129\320R\201\2644\240\375\264\277\322\200\332\005(3\240r\207Z\003(\027\240\362\201Z\003(\363\240R\203\332\005(\017\241\322\201\332\r(\023\2402\237Z\007(7\2402\201Z\037(\023\240\322\200\332\005(\377\240R\200\332\r "),
						},
					},
				},
			},
		},
		{
			ChunkedSeries: []*prompb.ChunkedSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
						{Name: "foo", Value: "bar3"},
					},
					Chunks: []prompb.Chunk{
						{
							Type:      prompb.Chunk_XOR,
							MinTimeMs: 14400000,
							MaxTimeMs: 14400000,
							Data:      []byte("\000\001\200\350\335\r@\327p\000\000\000\000\000"),
						},
					},
				},
			},
		},
		{
			ChunkedSeries: []*prompb.ChunkedSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
						{Name: "foo", Value: "bar1"},
					},
					Chunks: []prompb.Chunk{
						{
							Type:      prompb.Chunk_XOR,
							MaxTimeMs: 7140000,
							Data:      []byte("\000x\000\000\000\000\000\000\000\000\000\340\324\003\302|\005\224\000\301\254}\351z2\320O\355\264n[\007\316\224\243md\371\320\375\032Pm\nS\235\016Q\255\006P\275\250\277\312\201Z\003(3\240R\207\332\005(\017\240\322\201\332=(\023\2402\203Z\007(w\2402\201Z\017(\023\265\227\364P\033@\245\007\364\nP\033C\245\002t\036P+@e\036\364\016Pk@e\002t:P;A\245\001\364\nS\373@\245\006t\006P+C\345\002\364\006Pk@\345\036t\nP\033A\245\003\364:P\033@\245\006t\016ZJ\377\\\205\313\210\327\270\017\345+F[\310\347E)\355\024\241\366\342}(v\215(N\203)\326\207(\336\203(V\332W\362\202t4\240m\005(\377AJ\006\320\322\202t\374\240\255\003(oA\312:\3202"),
						},
					},
				},
			},
			QueryIndex: 1,
		},
		{
			ChunkedSeries: []*prompb.ChunkedSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_histogram_metric1"},
						{Name: "b", Value: "c"},
						{Name: "baz", Value: "qux"},
						{Name: "d", Value: "e"},
					},
					Chunks: []prompb.Chunk{
						{
							Type:      prompb.Chunk_FLOAT_HISTOGRAM,
							MaxTimeMs: 1440000,
							Data:      []byte("\x00\x19\x00\xff\x3f\x50\x62\x4d\xd2\xf1\xa9\xfc\x8c\xa4\x94\x65\x24\xa2\x20\x14\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x20\x19\x33\x33\x33\x33\x33\x33\x1f\xf8\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x1f\xf8\x00\x00\x00\x00\x00\x00\x1f\xf8\x00\x00\x00\x00\x00\x00\x1f\xf8\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x1f\xf8\x00\x00\x00\x00\x00\x00\x1f\xf8\x00\x00\x00\x00\x00\x00\x7c\x75\x30\x6b\x17\xbb\x01\xe9\x0f\xe1\x2f\xff\xec\x07\x84\xbf\xff\x84\xbf\xff\x84\xbf\xff\xb0\x1e\x12\xff\xfe\x12\xff\xfa\x5e\xb0\xbd\x9a\x4f\xff\xff\xff\xff\xff\xfe\xc0\x7a\xc2\xf6\x03\xd8\x0f\x60\x3d\x61\x7b\x01\xec\x06\xd2\x47\xde\xd0\x7a\xf5\xcf\xff\xff\xff\xff\xff\xfe\xb0\xbd\xa0\xf5\x85\xeb\x0b\xd6\x17\xb4\x1e\xb0\xbd\x61\x68\x5f\x60\x5c\x56\x66\x66\x66\x66\x66\x6d\xa0\xf6\x05\xed\x07\xb4\x1e\xd0\x7b\x02\xf6\x83\xda\x0d\x04\xcc\xc9\x99\x99\x99\x99\x99\x9d\x81\x73\xb0\x2f\x60\x5e\xc0\xb9\xd8\x17\xb0\x2d\x1c\x6a\x13\xf5\x0f\xde\x75\x09\xf3\x33\xa8\x4f\x99\x6e\x12\x77\x03\xdd\x94\xff\xff\xff\xff\xff\xff\xa8\x4f\xdc\x0f\x50\x9f\xa8\x4f\xd4\x27\xee\x07\xa8\x4f\xd4\x27\xb6\x8b\xfd\xa1\x7b\x73\xda\xaa\xaa\xaa\xaa\xaa\xbb\x81\xed\x0b\xdc\x0f\x70\x3d\xc0\xf6\x85\xee\x07\xb8\x1a\x4c\xce\xcc\xcc\xcc\xcc\xcc\xcf\x68\x5c\xed\x0b\xda\x17\xb4\x2e\x76\x85\xed\x0b\x6c\x1b\xbd\x81\xfd\x99\x72\x66\x66\x66\x66\x66\x73\xb0\x3f\x33\x3b\x03\xf3\x28\x98\xee\xca\xd5\x55\x55\x55\x55\x55\xd8\x1f\x8e\xc0\xfe\xc0\xfe\xc0\xfc\x76\x07\xf6\x07\xd2\xf3\xdb\x9e\x7f\xff\xff\xff\xff\xff\x8c\xe3\x18\xce\x31\x6a\x27\xe3\x1d\x7a\xf7\xff\xff\xff\xff\xff\xfe\x71\x9c\xe7\x19\xcd\x02\xf5\x89\xf0\x56\x66\x66\x66\x66\x66\x63\xac\x4f\x8c\x63\xac\x4f\x8c\x50\x67\x78\x38\x64\xcc\xcc\xcc\xcc\xcc\xda\xc4\xfd\xe0\xf5\x89\xfa\xc4\xfd\x62\x7e\xf0\x7a\xc4\xfd\x62\x7a\x07\xee\x0b\x83\xd5\x55\x55\x55\x55\x55\xbc\x1e\xe0\xbd\xe0\xf7\x83\xde\x0f\x70\x5e\xf0\x7b\xc1\xa1\xcc\xc7\x3f\xff\xff\xff\xff\xff\xdc\x17\x3b\x82\xf7\x05\xee\x0b\x9d\xc1\x7b\x82\xd0\x2f\x68\x7e\x0b\x55\x55\x55\x55\x55\x54\xed\x0f\xcc\xce\xd0\xfc\xca\x0c\xc6\x16\xcc\xcc\xcc\xcc\xcc\xce\xd0\xfc\x76\x87\xf6\x87\xf6\x87\xe3\xb4\x3f\xb4\x3e\x8e\xf3\x9e\x4c\xcc\xcc\xcc\xcc\xcd\x19\xc6\x31\x9c\x62\x81\x31\x82\xd5\x55\x55\x55\x55\x55\x38\xce\x73\x8c\xe6\x83\x7b\x04\xf8\x67\xff\xff\xff\xff\xff\xf1\xd8\x27\xc6\x31\xd8\x27\xc6\x28\x13\x0c\x1e\xaa\xaa\xaa\xaa\xaa\xad\x82\x7c\x3b\x04\xfd\x82\x7e\xc1\x3e\x1d\x82\x7e\xc1\x3d\x0f\xe3\x8e\x4c\xcc\xcc\xcc\xcc\xcd\x0c\x70\xc3\x0c\x70\xc2"),
						},
					},
				},
			},
			QueryIndex: 2,
		},
	}, results)
}

func addNativeHistogramsToTestSuite(t *testing.T, storage *teststorage.TestStorage, n int) {
	lbls := labels.FromStrings("__name__", "test_histogram_metric1", "baz", "qux")

	app := storage.Appender(t.Context())
	for i, fh := range tsdbutil.GenerateTestFloatHistograms(n) {
		_, err := app.AppendHistogram(0, lbls, int64(i)*int64(60*time.Second/time.Millisecond), nil, fh)
		require.NoError(t, err)
	}

	lbls = labels.FromStrings("__name__", "test_nhcb_metric1", "baz", "qux")
	for i, fh := range tsdbutil.GenerateTestCustomBucketsFloatHistograms(n) {
		_, err := app.AppendHistogram(0, lbls, int64(i)*int64(60*time.Second/time.Millisecond), nil, fh)
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())
}
