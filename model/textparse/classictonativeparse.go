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
	"strconv"

	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/convertclassic"
)

type collectionState int

const (
	stateStart collectionState = iota
	stateCollecting
	stateEmitting
	stateInhibiting // Inhibiting conversion, because there was an exponential histogram with the same labels.
)

// The ClassicToNativeParser wraps a Parser and converts classic histograms to native
// histograms.
//
// Since Parser interface is line based, this parser needs to keep track
// of the last classic histogram series it saw to collate them into a
// single native histogram.
//
// Note:
//   - Only series that have the histogram metadata type are considered for
//     conversion.
//   - The classic series are also returned if keepClassicHistograms is true.
type ClassicToNativeParser struct {
	// The parser we're wrapping.
	parser Parser
	// Option to keep classic histograms along with converted histograms.
	keepClassicHistograms bool

	// Labels builder.
	builder labels.ScratchBuilder

	// State of the parser.
	state collectionState

	// Caches the values from the underlying parser.
	// For Series and Histogram.
	bytes []byte
	ts    *int64
	value float64
	h     *histogram.Histogram
	fh    *histogram.FloatHistogram
	// For Metric.
	lset labels.Labels
	// For Type.
	bName []byte
	typ   model.MetricType

	// Caches the entry itself if we are inserting a converted native histogram
	// halfway through.
	entry Entry
	err   error

	// Caches the values and metric for the inserted converted native histogram.
	bytesNative        []byte
	hNative            *histogram.Histogram
	fhNative           *histogram.FloatHistogram
	lsetNative         labels.Labels
	exemplars          []exemplar.Exemplar
	stNative           int64
	metricStringNative string

	// Collates values from the classic histogram series to build
	// the converted histogram later.
	tempLsetNative    labels.Labels
	tempNative        convertclassic.TempHistogram
	tempExemplars     []exemplar.Exemplar
	tempExemplarCount int
	tempST            int64

	// Remembers the last base histogram metric name (assuming it's
	// a classic histogram) so we can tell if the next float series
	// is part of the same classic histogram.
	lastHistogramName       string
	lastHistogramLabelsHash uint64
	// Reused buffer for hashing labels.
	hBuffer []byte
}

func NewClassicToNativeParser(p Parser, st *labels.SymbolTable, keepClassicHistograms bool) Parser {
	return &ClassicToNativeParser{
		parser:                p,
		keepClassicHistograms: keepClassicHistograms,
		builder:               labels.NewScratchBuilderWithSymbolTable(st, 16),
		tempNative:            convertclassic.NewTempHistogram(),
	}
}

func (p *ClassicToNativeParser) Series() ([]byte, *int64, float64) {
	return p.bytes, p.ts, p.value
}

func (p *ClassicToNativeParser) Histogram() ([]byte, *int64, *histogram.Histogram, *histogram.FloatHistogram) {
	if p.state == stateEmitting {
		return p.bytesNative, p.ts, p.hNative, p.fhNative
	}
	return p.bytes, p.ts, p.h, p.fh
}

func (p *ClassicToNativeParser) Help() ([]byte, []byte) {
	return p.parser.Help()
}

func (p *ClassicToNativeParser) Type() ([]byte, model.MetricType) {
	return p.bName, p.typ
}

func (p *ClassicToNativeParser) Unit() ([]byte, []byte) {
	return p.parser.Unit()
}

func (p *ClassicToNativeParser) Comment() []byte {
	return p.parser.Comment()
}

func (p *ClassicToNativeParser) Labels(l *labels.Labels) {
	if p.state == stateEmitting {
		*l = p.lsetNative
		return
	}
	*l = p.lset
}

func (p *ClassicToNativeParser) Exemplar(ex *exemplar.Exemplar) bool {
	if p.state == stateEmitting {
		if len(p.exemplars) == 0 {
			return false
		}
		*ex = p.exemplars[0]
		p.exemplars = p.exemplars[1:]
		return true
	}
	return p.parser.Exemplar(ex)
}

func (p *ClassicToNativeParser) StartTimestamp() int64 {
	switch p.state {
	case stateStart, stateInhibiting:
		if p.entry == EntrySeries || p.entry == EntryHistogram {
			return p.parser.StartTimestamp()
		}
	case stateCollecting:
		return p.tempST
	case stateEmitting:
		return p.stNative
	}
	return 0
}

func (p *ClassicToNativeParser) Next() (Entry, error) {
	for {
		if p.state == stateEmitting {
			p.state = stateStart
			if p.entry == EntrySeries {
				isNative := p.handleClassicHistogramSeries(p.lset)
				if isNative && !p.keepClassicHistograms {
					// Do not return the classic histogram series if it was converted to native and we are not keeping classic histograms.
					continue
				}
			}
			return p.entry, p.err
		}

		p.entry, p.err = p.parser.Next()
		if p.err != nil {
			if errors.Is(p.err, io.EOF) && p.processNative() {
				return EntryHistogram, nil
			}
			return EntryInvalid, p.err
		}
		switch p.entry {
		case EntrySeries:
			p.bytes, p.ts, p.value = p.parser.Series()
			p.parser.Labels(&p.lset)
			var isNative bool
			switch p.state {
			case stateCollecting:
				if p.differentMetric() && p.processNative() {
					// We are collecting classic series, but the next series
					// has different type or labels. If we can convert what
					// we have collected so far to native histogram, then we can return it.
					return EntryHistogram, nil
				}
				isNative = p.handleClassicHistogramSeries(p.lset)
			case stateInhibiting:
				if p.differentMetric() {
					// Next has different labels than the previous exponential
					// histogram so we can start collecting classic histogram
					// series.
					p.state = stateStart
					isNative = p.handleClassicHistogramSeries(p.lset)
				} else {
					// Next has the same labels as the previous exponential
					// histogram, so we are still in the inhibiting state and
					// we should not convert to native.
					isNative = false
				}
			case stateStart:
				isNative = p.handleClassicHistogramSeries(p.lset)
			default:
				// This should not happen.
				return EntryInvalid, errors.New("unexpected state in ClassicToNativeParser")
			}
			if isNative && !p.keepClassicHistograms {
				// Do not return the classic histogram series if it was converted to native and we are not keeping classic histograms.
				continue
			}
			return p.entry, p.err
		case EntryHistogram:
			p.state = stateInhibiting
			p.bytes, p.ts, p.h, p.fh = p.parser.Histogram()
			p.parser.Labels(&p.lset)
			p.storeExponentialLabels()
		case EntryType:
			p.bName, p.typ = p.parser.Type()
		}
		if p.processNative() {
			return EntryHistogram, nil
		}
		return p.entry, p.err
	}
}

// Return true if labels have changed and we should emit the native histogram.
func (p *ClassicToNativeParser) differentMetric() bool {
	if p.typ != model.MetricTypeHistogram {
		// Different metric type.
		return true
	}
	_, name := convertclassic.GetHistogramMetricBaseName(p.lset.Get(labels.MetricName))
	if p.lastHistogramName != name {
		// Different metric name.
		return true
	}
	nextHash, _ := p.lset.HashWithoutLabels(p.hBuffer, labels.BucketLabel)
	// Different label values.
	return p.lastHistogramLabelsHash != nextHash
}

// Save the label set of the classic histogram without suffix and bucket `le` label.
func (p *ClassicToNativeParser) storeClassicLabels(name string) {
	p.lastHistogramName = name
	p.lastHistogramLabelsHash, _ = p.lset.HashWithoutLabels(p.hBuffer, labels.BucketLabel)
}

func (p *ClassicToNativeParser) storeExponentialLabels() {
	p.lastHistogramName = p.lset.Get(labels.MetricName)
	p.lastHistogramLabelsHash, _ = p.lset.HashWithoutLabels(p.hBuffer)
}

// handleClassicHistogramSeries collates the classic histogram series to be converted to native
// if it is actually a classic histogram series (and not a normal float series) and if there
// isn't already a native histogram with the same name (assuming it is always processed
// right before the classic histograms) and returns true if the collation was done.
func (p *ClassicToNativeParser) handleClassicHistogramSeries(lset labels.Labels) bool {
	if p.typ != model.MetricTypeHistogram {
		return false
	}
	mName := lset.Get(labels.MetricName)
	// Sanity check to ensure that the TYPE metadata entry name is the same as the base name.
	suffixType, name := convertclassic.GetHistogramMetricBaseName(mName)
	if name != string(p.bName) {
		return false
	}
	switch suffixType {
	case convertclassic.SuffixBucket:
		if !lset.Has(labels.BucketLabel) {
			// This should not really happen.
			return false
		}
		le, err := strconv.ParseFloat(lset.Get(labels.BucketLabel), 64)
		if err == nil && !math.IsNaN(le) {
			p.processClassicHistogramSeries(lset, name, func(hist *convertclassic.TempHistogram) {
				_ = hist.SetBucketCount(le, p.value)
			})
			return true
		}
	case convertclassic.SuffixCount:
		p.processClassicHistogramSeries(lset, name, func(hist *convertclassic.TempHistogram) {
			_ = hist.SetCount(p.value)
		})
		return true
	case convertclassic.SuffixSum:
		p.processClassicHistogramSeries(lset, name, func(hist *convertclassic.TempHistogram) {
			_ = hist.SetSum(p.value)
		})
		return true
	}
	return false
}

func (p *ClassicToNativeParser) processClassicHistogramSeries(lset labels.Labels, name string, updateHist func(*convertclassic.TempHistogram)) {
	if p.state != stateCollecting {
		p.storeClassicLabels(name)
		p.tempST = p.parser.StartTimestamp()
		p.state = stateCollecting
		p.tempLsetNative = convertclassic.GetHistogramMetricBase(lset, name)
	}
	p.storeExemplars()
	updateHist(&p.tempNative)
}

func (p *ClassicToNativeParser) storeExemplars() {
	for ex := p.nextExemplarPtr(); p.parser.Exemplar(ex); ex = p.nextExemplarPtr() {
		p.tempExemplarCount++
	}
}

func (p *ClassicToNativeParser) nextExemplarPtr() *exemplar.Exemplar {
	switch {
	case p.tempExemplarCount == len(p.tempExemplars)-1:
		// Reuse the previously allocated exemplar, it was not filled up.
	case len(p.tempExemplars) == cap(p.tempExemplars):
		// Let the runtime grow the slice.
		p.tempExemplars = append(p.tempExemplars, exemplar.Exemplar{})
	default:
		// Take the next element into use.
		p.tempExemplars = p.tempExemplars[:len(p.tempExemplars)+1]
	}
	return &p.tempExemplars[len(p.tempExemplars)-1]
}

func (p *ClassicToNativeParser) swapExemplars() {
	p.exemplars = p.tempExemplars[:p.tempExemplarCount]
	p.tempExemplars = p.tempExemplars[:0]
}

// processNative converts the collated classic histogram series to native and caches the info
// to be returned to callers. Returns true if the conversion was successful.
func (p *ClassicToNativeParser) processNative() bool {
	if p.state != stateCollecting {
		return false
	}
	h, fh, err := p.tempNative.Convert()
	if err == nil {
		if h != nil {
			if err := h.Validate(); err != nil {
				return false
			}
			p.hNative = h
			p.fhNative = nil
		} else if fh != nil {
			if err := fh.Validate(); err != nil {
				return false
			}
			p.hNative = nil
			p.fhNative = fh
		}

		lblsWithMetricName := p.tempLsetNative.DropReserved(func(n string) bool { return n == labels.MetricName })
		// Ensure we return `metric` instead of `metric{}` for name only
		// series, for consistency with wrapped parsers.
		if lblsWithMetricName.IsEmpty() {
			p.metricStringNative = p.tempLsetNative.Get(labels.MetricName)
		} else {
			p.metricStringNative = p.tempLsetNative.Get(labels.MetricName) + lblsWithMetricName.StringNoSpace()
		}

		p.bytesNative = []byte(p.metricStringNative)
		p.lsetNative = p.tempLsetNative
		p.swapExemplars()
		p.stNative = p.tempST
		p.state = stateEmitting
	} else {
		p.state = stateStart
	}
	p.tempNative.Reset()
	p.tempExemplarCount = 0
	p.tempST = 0
	return err == nil
}

func (p *ClassicToNativeParser) Base() Parser {
	return p.parser
}
