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

package convertclassic

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
)

func TestClassicConvert(t *testing.T) {
	tests := map[string]struct {
		setup       func() *TempHistogram
		expectedErr error
		expectedH   *histogram.Histogram
		expectedFH  *histogram.FloatHistogram
	}{
		"empty": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				return &h
			},
			expectedH: &histogram.Histogram{
				Schema: 0,
				ClassicBuckets: []histogram.ClassicBucket{
					{UpperBound: math.Inf(1), CumulativeCount: 0},
				},
			},
		},
		"sum only": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetSum(1000.25)
				return &h
			},
			expectedH: &histogram.Histogram{
				Schema: 0,
				Sum:    1000.25,
				ClassicBuckets: []histogram.ClassicBucket{
					{UpperBound: math.Inf(1), CumulativeCount: 0},
				},
			},
		},
		"single integer bucket": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetSum(1000.25)
				h.SetBucketCount(0.5, 1000)
				return &h
			},
			expectedH: &histogram.Histogram{
				Schema: 0,
				Count:  1000,
				Sum:    1000.25,
				ClassicBuckets: []histogram.ClassicBucket{
					{UpperBound: 0.5, CumulativeCount: 1000},
					{UpperBound: math.Inf(1), CumulativeCount: 1000},
				},
			},
		},
		"single float bucket": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetSum(1000.25)
				h.SetBucketCount(0.5, 1337.42)
				return &h
			},
			expectedFH: &histogram.FloatHistogram{
				Schema: 0,
				Count:  1337.42,
				Sum:    1000.25,
				ClassicBuckets: []histogram.ClassicBucket{
					{UpperBound: 0.5, CumulativeCount: 1337.42},
					{UpperBound: math.Inf(1), CumulativeCount: 1337.42},
				},
			},
		},
		"happy case integer bucket": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetCount(1000)
				h.SetSum(1000.25)
				h.SetBucketCount(0.5, 50)
				h.SetBucketCount(1.0, 950)
				h.SetBucketCount(math.Inf(1), 1000)
				return &h
			},
			expectedH: &histogram.Histogram{
				Schema: 0,
				Count:  1000,
				Sum:    1000.25,
				ClassicBuckets: []histogram.ClassicBucket{
					{UpperBound: 0.5, CumulativeCount: 50},
					{UpperBound: 1.0, CumulativeCount: 950},
					{UpperBound: math.Inf(1), CumulativeCount: 1000},
				},
			},
		},
		"happy case float bucket": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetCount(1000)
				h.SetSum(1000.25)
				h.SetBucketCount(0.5, 50)
				h.SetBucketCount(1.0, 950.5)
				h.SetBucketCount(math.Inf(1), 1000)
				return &h
			},
			expectedFH: &histogram.FloatHistogram{
				Schema: 0,
				Count:  1000,
				Sum:    1000.25,
				ClassicBuckets: []histogram.ClassicBucket{
					{UpperBound: 0.5, CumulativeCount: 50},
					{UpperBound: 1.0, CumulativeCount: 950.5},
					{UpperBound: math.Inf(1), CumulativeCount: 1000},
				},
			},
		},
		"non cumulative bucket": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetCount(1000)
				h.SetSum(1000.25)
				h.SetBucketCount(0.5, 50)
				h.SetBucketCount(1.0, 950)
				h.SetBucketCount(math.Inf(1), 900)
				return &h
			},
			expectedErr: errCountNotCumulative,
		},
		"negative count": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetCount(-1000)
				h.SetSum(1000.25)
				h.SetBucketCount(0.5, 50)
				h.SetBucketCount(1.0, 950)
				h.SetBucketCount(math.Inf(1), 900)
				return &h
			},
			expectedErr: errNegativeCount,
		},
		"NaN bucket upper bound": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				// Add a real bucket first so that the NaN call reaches the
				// default branch of the switch in SetBucketCount; without an
				// existing bucket, len(h.buckets)==0 and the NaN is silently
				// appended without triggering the panic.
				h.SetBucketCount(1.0, 5)
				h.SetBucketCount(math.NaN(), 10)
				return &h
			},
			expectedErr: errNaNBucket,
		},
		"mixed order": {
			setup: func() *TempHistogram {
				h := NewTempHistogram()
				h.SetBucketCount(0.5, 50)
				h.SetBucketCount(math.Inf(1), 1000)
				h.SetBucketCount(1.0, 950)
				h.SetCount(1000)
				h.SetSum(1000.25)
				return &h
			},
			expectedH: &histogram.Histogram{
				Schema: 0,
				Count:  1000,
				Sum:    1000.25,
				ClassicBuckets: []histogram.ClassicBucket{
					{UpperBound: 0.5, CumulativeCount: 50},
					{UpperBound: 1.0, CumulativeCount: 950},
					{UpperBound: math.Inf(1), CumulativeCount: 1000},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			th := test.setup()
			h, fh, err := th.Convert()
			if test.expectedErr != nil {
				require.ErrorIs(t, err, test.expectedErr)
				return
			}
			require.Equal(t, test.expectedH, h)
			if h != nil {
				require.NoError(t, h.Validate())
			}
			require.Equal(t, test.expectedFH, fh)
			if fh != nil {
				require.NoError(t, fh.Validate())
			}
		})
	}
}
