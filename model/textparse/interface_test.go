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

package textparse

import (
	"errors"
	"io"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestNewParser(t *testing.T) {
	t.Parallel()

	requireNilParser := func(t *testing.T, p Parser) {
		require.Nil(t, p)
	}

	requirePromParser := func(t *testing.T, p Parser) {
		require.NotNil(t, p)
		wrapper, ok := p.(*ClassicToNativeParser)
		require.True(t, ok)
		_, ok = wrapper.Base().(*PromParser)
		require.True(t, ok)
	}

	requireOpenMetricsParser := func(t *testing.T, p Parser) {
		require.NotNil(t, p)
		wrapper, ok := p.(*ClassicToNativeParser)
		require.True(t, ok)
		_, ok = wrapper.Base().(*OpenMetricsParser)
		require.True(t, ok)
	}

	requireProtobufParser := func(t *testing.T, p Parser) {
		require.NotNil(t, p)
		_, ok := p.(*ProtobufParser)
		require.True(t, ok)
	}

	for name, tt := range map[string]*struct {
		contentType            string
		fallbackScrapeProtocol config.ScrapeProtocol
		validateParser         func(*testing.T, Parser)
		err                    string
	}{
		"empty-string": {
			validateParser: requireNilParser,
			err:            "non-compliant scrape target sending blank Content-Type and no fallback_scrape_protocol specified for target",
		},
		"empty-string-fallback-text-plain": {
			validateParser:         requirePromParser,
			fallbackScrapeProtocol: config.PrometheusText0_0_4,
			err:                    "non-compliant scrape target sending blank Content-Type, using fallback_scrape_protocol \"text/plain\"",
		},
		"invalid-content-type-1": {
			contentType:    "invalid/",
			validateParser: requireNilParser,
			err:            "expected token after slash",
		},
		"invalid-content-type-1-fallback-text-plain": {
			contentType:            "invalid/",
			validateParser:         requirePromParser,
			fallbackScrapeProtocol: config.PrometheusText0_0_4,
			err:                    "expected token after slash",
		},
		"invalid-content-type-1-fallback-openmetrics": {
			contentType:            "invalid/",
			validateParser:         requireOpenMetricsParser,
			fallbackScrapeProtocol: config.OpenMetricsText0_0_1,
			err:                    "expected token after slash",
		},
		"invalid-content-type-1-fallback-protobuf": {
			contentType:            "invalid/",
			validateParser:         requireProtobufParser,
			fallbackScrapeProtocol: config.PrometheusProto,
			err:                    "expected token after slash",
		},
		"invalid-content-type-2": {
			contentType:    "invalid/invalid/invalid",
			validateParser: requireNilParser,
			err:            "unexpected content after media subtype",
		},
		"invalid-content-type-2-fallback-text-plain": {
			contentType:            "invalid/invalid/invalid",
			validateParser:         requirePromParser,
			fallbackScrapeProtocol: config.PrometheusText1_0_0,
			err:                    "unexpected content after media subtype",
		},
		"invalid-content-type-3": {
			contentType:    "/",
			validateParser: requireNilParser,
			err:            "no media type",
		},
		"invalid-content-type-3-fallback-text-plain": {
			contentType:            "/",
			validateParser:         requirePromParser,
			fallbackScrapeProtocol: config.PrometheusText1_0_0,
			err:                    "no media type",
		},
		"invalid-content-type-4": {
			contentType:    "application/openmetrics-text; charset=UTF-8; charset=utf-8",
			validateParser: requireNilParser,
			err:            "duplicate parameter name",
		},
		"invalid-content-type-4-fallback-open-metrics": {
			contentType:            "application/openmetrics-text; charset=UTF-8; charset=utf-8",
			validateParser:         requireOpenMetricsParser,
			fallbackScrapeProtocol: config.OpenMetricsText1_0_0,
			err:                    "duplicate parameter name",
		},
		"openmetrics": {
			contentType:    "application/openmetrics-text",
			validateParser: requireOpenMetricsParser,
		},
		"openmetrics-with-charset": {
			contentType:    "application/openmetrics-text; charset=utf-8",
			validateParser: requireOpenMetricsParser,
		},
		"openmetrics-with-charset-and-version": {
			contentType:    "application/openmetrics-text; version=1.0.0; charset=utf-8",
			validateParser: requireOpenMetricsParser,
		},
		"plain-text": {
			contentType:    "text/plain",
			validateParser: requirePromParser,
		},
		"protobuf": {
			contentType:    "application/vnd.google.protobuf",
			validateParser: requireProtobufParser,
		},
		"plain-text-with-version": {
			contentType:    "text/plain; version=0.0.4",
			validateParser: requirePromParser,
		},
		"some-other-valid-content-type": {
			contentType:    "text/html",
			validateParser: requireNilParser,
			err:            "received unsupported Content-Type \"text/html\" and no fallback_scrape_protocol specified for target",
		},
		"some-other-valid-content-type-fallback-text-plain": {
			contentType:            "text/html",
			validateParser:         requirePromParser,
			fallbackScrapeProtocol: config.PrometheusText0_0_4,
			err:                    "received unsupported Content-Type \"text/html\", using fallback_scrape_protocol \"text/plain\"",
		},
	} {
		t.Run(name, func(t *testing.T) {
			tt := tt // Copy to local variable before going parallel.
			t.Parallel()

			fallbackProtoMediaType := tt.fallbackScrapeProtocol.HeaderMediaType()

			p, err := New([]byte{}, tt.contentType, labels.NewSymbolTable(), ParserOptions{FallbackContentType: fallbackProtoMediaType})
			tt.validateParser(t, p)
			if tt.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.err)
			}
		})
	}
}

// parsedEntry represents data that is parsed for each entry.
type parsedEntry struct {
	// In all but EntryComment, EntryInvalid.
	m string

	// In EntryHistogram.
	shs *histogram.Histogram
	fhs *histogram.FloatHistogram

	// In EntrySeries.
	v float64

	// In EntrySeries and EntryHistogram.
	lset labels.Labels
	t    *int64
	es   []exemplar.Exemplar
	st   int64

	// In EntryType.
	typ model.MetricType
	// In EntryHelp.
	help string
	// In EntryUnit.
	unit string
	// In EntryComment.
	comment string
}

func requireEntries(t *testing.T, exp, got []parsedEntry) {
	t.Helper()

	// Transform expected -53 custom bucket histograms to Schema 0 combined classic histograms
	for i := range exp {
		if exp[i].shs != nil && exp[i].shs.Schema == histogram.CustomBucketsSchema {
			counts := map[int]float64{}
			it := exp[i].shs.PositiveBucketIterator()
			var runningCount float64
			for it.Next() {
				b := it.At()
				runningCount += float64(b.Count)
				counts[int(b.Index)] = runningCount
			}

			var classicBuckets []histogram.ClassicBucket
			var lastVal float64
			for idx, val := range exp[i].shs.CustomValues {
				if c, ok := counts[idx]; ok {
					lastVal = c
				}
				classicBuckets = append(classicBuckets, histogram.ClassicBucket{
					UpperBound:      val,
					CumulativeCount: lastVal,
				})
			}
			if c, ok := counts[len(exp[i].shs.CustomValues)]; ok {
				lastVal = c
			} else {
				lastVal = float64(exp[i].shs.Count)
			}
			classicBuckets = append(classicBuckets, histogram.ClassicBucket{
				UpperBound:      math.Inf(1),
				CumulativeCount: lastVal,
			})

			exp[i].shs = &histogram.Histogram{
				Schema:         0,
				Count:          exp[i].shs.Count,
				Sum:            exp[i].shs.Sum,
				ClassicBuckets: classicBuckets,
			}
		}
		if exp[i].fhs != nil && exp[i].fhs.Schema == histogram.CustomBucketsSchema {
			counts := map[int]float64{}
			it := exp[i].fhs.PositiveBucketIterator()
			for it.Next() {
				b := it.At()
				counts[int(b.Index)] = b.Count
			}

			var classicBuckets []histogram.ClassicBucket
			var lastVal float64
			for idx, val := range exp[i].fhs.CustomValues {
				if c, ok := counts[idx]; ok {
					lastVal = c
				}
				classicBuckets = append(classicBuckets, histogram.ClassicBucket{
					UpperBound:      val,
					CumulativeCount: lastVal,
				})
			}
			if c, ok := counts[len(exp[i].fhs.CustomValues)]; ok {
				lastVal = c
			} else {
				lastVal = exp[i].fhs.Count
			}
			classicBuckets = append(classicBuckets, histogram.ClassicBucket{
				UpperBound:      math.Inf(1),
				CumulativeCount: lastVal,
			})

			exp[i].fhs = &histogram.FloatHistogram{
				Schema:         0,
				Count:          exp[i].fhs.Count,
				Sum:            exp[i].fhs.Sum,
				ClassicBuckets: classicBuckets,
			}
		}
	}

	testutil.RequireEqualWithOptions(t, exp, got, []cmp.Option{
		// We reuse slices so we sometimes have empty vs nil differences
		// we need to ignore with cmpopts.EquateEmpty().
		// However we have to filter out labels, as only
		// one comparer per type has to be specified,
		// and RequireEqualWithOptions uses
		// cmp.Comparer(labels.Equal).
		cmp.FilterValues(func(x, y any) bool {
			_, xIsLabels := x.(labels.Labels)
			_, yIsLabels := y.(labels.Labels)
			return !xIsLabels && !yIsLabels
		}, cmpopts.EquateEmpty()),
		cmpopts.EquateNaNs(),
		cmp.AllowUnexported(parsedEntry{}),
	})
}

func testParse(t *testing.T, p Parser) (ret []parsedEntry) {
	t.Helper()

	for {
		et, err := p.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		var got parsedEntry
		var m []byte
		switch et {
		case EntryInvalid:
			t.Fatal("entry invalid not expected")
		case EntrySeries, EntryHistogram:
			var ts *int64
			if et == EntrySeries {
				m, ts, got.v = p.Series()
			} else {
				m, ts, got.shs, got.fhs = p.Histogram()
			}
			if ts != nil {
				// TODO(bwplotka): Change to 0 in the interface for set check to
				// avoid pointer mangling.
				got.t = int64p(*ts)
			}
			got.m = string(m)
			p.Labels(&got.lset)
			got.st = p.StartTimestamp()

			for e := (exemplar.Exemplar{}); p.Exemplar(&e); {
				got.es = append(got.es, e)
			}
		case EntryType:
			m, got.typ = p.Type()
			got.m = string(m)

		case EntryHelp:
			m, h := p.Help()
			got.m = string(m)
			got.help = string(h)

		case EntryUnit:
			m, u := p.Unit()
			got.m = string(m)
			got.unit = string(u)

		case EntryComment:
			got.comment = string(p.Comment())
		}
		ret = append(ret, got)
	}
	return ret
}
