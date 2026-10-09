/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package aggregate

import (
	"context"
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func fathomryExactBucket(bounds []float64, value int64) int {
	integer := new(big.Rat).SetInt64(value)
	for index, bound := range bounds {
		if integer.Cmp(new(big.Rat).SetFloat64(bound)) <= 0 {
			return index
		}
	}
	return len(bounds)
}

func fathomryBoundaryControls() ([]float64, []int64) {
	bounds := []float64{-math.MaxFloat64, math.Nextafter(-0x1p63, math.Inf(-1)), -0x1p63,
		math.Nextafter(-0x1p63, math.Inf(1)), -0x1p53 - 4, -0x1p53 - 2, -0x1p53,
		-100, -1.5, -0.5, -math.SmallestNonzeroFloat64, 0, math.SmallestNonzeroFloat64,
		0.5, 1.5, 100, 0x1p53, 0x1p53 + 2, 0x1p53 + 4,
		math.Nextafter(0x1p63, math.Inf(-1)), 0x1p63, math.Nextafter(0x1p63, math.Inf(1)), math.MaxFloat64}
	values := []int64{math.MinInt64, math.MinInt64 + 1, math.MinInt64 + 1024,
		-(1 << 53) - 5, -(1 << 53) - 4, -(1 << 53) - 3, -(1 << 53) - 2, -(1 << 53) - 1,
		-(1 << 53), -(1 << 53) + 1, -101, -100, -99, -2, -1, 0, 1, 2, 99, 100, 101,
		(1 << 53) - 1, 1 << 53, (1 << 53) + 1, (1 << 53) + 2, (1 << 53) + 3,
		(1 << 53) + 4, (1 << 53) + 5, math.MaxInt64 - 1024, math.MaxInt64 - 1, math.MaxInt64}
	return bounds, values
}

func TestFathomryHistogramBucketExactIntegerComparison(t *testing.T) {
	bounds, values := fathomryBoundaryControls()
	for _, value := range values {
		want := fathomryExactBucket(bounds, value)
		if got := histogramBucketIndex(bounds, value); got != want {
			t.Errorf("int64 %d: bucket %d, want %d", value, got, want)
		}
		for _, bound := range bounds {
			one := []float64{bound}
			if got, want := histogramBucketIndex(one, value), fathomryExactBucket(one, value); got != want {
				t.Errorf("int64 %d / bound %.17g: bucket %d, want %d", value, bound, got, want)
			}
		}
	}
	random := rand.New(rand.NewPCG(128, 8981))
	for range 10000 {
		bound := math.Float64frombits(random.Uint64())
		if math.IsNaN(bound) || math.IsInf(bound, 0) {
			continue
		}
		one := []float64{bound}
		value := int64(random.Uint64())
		if got, want := histogramBucketIndex(one, value), fathomryExactBucket(one, value); got != want {
			t.Fatalf("random int64 %d / bound %.17g: bucket %d, want %d", value, bound, got, want)
		}
	}
}

func TestFathomryHistogramBothTemporalitiesRetainExactBuckets(t *testing.T) {
	bounds, values := fathomryBoundaryControls()
	for _, temporality := range []metricdata.Temporality{metricdata.CumulativeTemporality, metricdata.DeltaTemporality} {
		for _, value := range values {
			measure, collect := Builder[int64]{Temporality: temporality, AggregationLimit: 1}.ExplicitBucketHistogram(bounds, false, false)
			var data metricdata.Aggregation
			measure(context.Background(), value, attribute.Set{})
			if collect(&data) != 1 {
				t.Fatal("missing native histogram")
			}
			assertFathomryHistogram(t, data, temporality, bounds, []int64{value})
			count := collect(&data)
			if temporality == metricdata.DeltaTemporality {
				if count != 0 {
					t.Fatal("delta collection did not reset")
				}
			} else {
				if count != 1 {
					t.Fatal("cumulative collection discarded data")
				}
				assertFathomryHistogram(t, data, temporality, bounds, []int64{value})
			}
			measure(context.Background(), 0, attribute.Set{})
			if collect(&data) != 1 {
				t.Fatal("second collection lost the new measurement")
			}
			if temporality == metricdata.DeltaTemporality {
				assertFathomryHistogram(t, data, temporality, bounds, []int64{0})
			} else {
				assertFathomryHistogram(t, data, temporality, bounds, []int64{value, 0})
			}
		}
	}
}

func assertFathomryHistogram(t *testing.T, data metricdata.Aggregation, temporality metricdata.Temporality, bounds []float64, values []int64) {
	t.Helper()
	histogram, ok := data.(metricdata.Histogram[int64])
	if !ok || histogram.Temporality != temporality || len(histogram.DataPoints) != 1 {
		t.Fatal("native integer histogram shape or temporality changed")
	}
	point := histogram.DataPoints[0]
	wantCounts := make([]uint64, len(bounds)+1)
	var sum int64
	minimum, maximum := values[0], values[0]
	for _, value := range values {
		wantCounts[fathomryExactBucket(bounds, value)]++
		sum += value
		minimum, maximum = min(minimum, value), max(maximum, value)
	}
	gotMin, hasMin := point.Min.Value()
	gotMax, hasMax := point.Max.Value()
	if !slices.Equal(point.BucketCounts, wantCounts) || !slices.Equal(point.Bounds, bounds) ||
		point.Count != uint64(len(values)) || point.Sum != sum || !hasMin || !hasMax || gotMin != minimum || gotMax != maximum {
		t.Fatalf("native %v values %v: counts %v, want %v; sum %d, want %d", temporality, values, point.BucketCounts, wantCounts, point.Sum, sum)
	}
}

func TestFathomryHistogramFloatPathUnchanged(t *testing.T) {
	bounds, _ := fathomryBoundaryControls()
	values := append(slices.Clone(bounds), math.NaN(), math.Inf(-1), math.Inf(1), math.Copysign(0, -1))
	for _, bound := range bounds {
		values = append(values, math.Nextafter(bound, math.Inf(-1)), math.Nextafter(bound, math.Inf(1)))
	}
	for _, value := range values {
		if got, want := histogramBucketIndex(bounds, value), sort.SearchFloat64s(bounds, value); got != want {
			t.Fatalf("float64 %.17g: bucket %d, original %d", value, got, want)
		}
	}
}

func FuzzFathomryHistogramBucketExactness(f *testing.F) {
	bounds, values := fathomryBoundaryControls()
	for _, bound := range bounds {
		for _, value := range values {
			f.Add(value, math.Float64bits(bound))
		}
	}
	f.Fuzz(func(t *testing.T, value int64, bits uint64) {
		bound := math.Float64frombits(bits)
		if math.IsNaN(bound) || math.IsInf(bound, 0) {
			return
		}
		bounds := []float64{-math.MaxFloat64, bound, math.MaxFloat64}
		if got, want := histogramBucketIndex(bounds, value), fathomryExactBucket(bounds, value); got != want {
			t.Fatalf("int64 %d / IEEE boundary %016x: bucket %d, want %d", value, bits, got, want)
		}
	})
}

var fathomryBucketResult int

func BenchmarkFathomryHistogramBucket(b *testing.B) {
	for _, profile := range []struct {
		name   string
		bounds []float64
		values []int64
	}{
		{"small", []float64{-100, -10, -1, 0, 1, 10, 100}, []int64{-101, -10, -1, 0, 1, 10, 101}},
		{"large", []float64{-0x1p63, -0x1p53 - 4, -0x1p53, 0, 0x1p53, 0x1p53 + 4, 0x1p63},
			[]int64{math.MinInt64, -(1 << 53) - 3, -(1 << 53) - 1, 0, (1 << 53) + 1, (1 << 53) + 3, math.MaxInt64}},
	} {
		b.Run(profile.name+"/corrected-int64", func(b *testing.B) {
			b.ReportAllocs()
			index := 0
			for b.Loop() {
				fathomryBucketResult = histogramBucketIndex(profile.bounds, profile.values[index])
				index = (index + 1) % len(profile.values)
			}
		})
		b.Run(profile.name+"/upstream-float64-detour", func(b *testing.B) {
			b.ReportAllocs()
			index := 0
			for b.Loop() {
				fathomryBucketResult = sort.SearchFloat64s(profile.bounds, float64(profile.values[index]))
				index = (index + 1) % len(profile.values)
			}
		})
		b.Run(profile.name+"/unchanged-float64", func(b *testing.B) {
			b.ReportAllocs()
			index := 0
			for b.Loop() {
				fathomryBucketResult = histogramBucketIndex(profile.bounds, float64(profile.values[index]))
				index = (index + 1) % len(profile.values)
			}
		})
	}
}
