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

package otel

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	metricapi "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	colmetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

var integralKinds = []string{"int64-counter", "int64-updowncounter", "int64-gauge", "int64-histogram"}

func TestInt64NativeAggregationExactness(t *testing.T) {
	for _, sample := range []struct {
		name  string
		value int64
	}{{"small", 101}, {"above-float64-exact-range", (1 << 53) + 1}} {
		for _, kind := range integralKinds {
			t.Run(sample.name+"/"+kind, func(t *testing.T) {
				reader := sdkmetric.NewManualReader()
				provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
				t.Cleanup(func() {
					if err := provider.Shutdown(context.Background()); err != nil {
						t.Error(err)
					}
				})
				meter := provider.Meter("integer-native-control")
				var measure func(int64)
				var err error
				switch kind {
				case "int64-counter":
					var instrument metricapi.Int64Counter
					instrument, err = meter.Int64Counter("count")
					measure = func(value int64) { instrument.Add(context.Background(), value) }
				case "int64-updowncounter":
					var instrument metricapi.Int64UpDownCounter
					instrument, err = meter.Int64UpDownCounter("count")
					measure = func(value int64) { instrument.Add(context.Background(), value) }
				case "int64-gauge":
					var instrument metricapi.Int64Gauge
					instrument, err = meter.Int64Gauge("count")
					measure = func(value int64) { instrument.Record(context.Background(), value) }
				case "int64-histogram":
					var instrument metricapi.Int64Histogram
					instrument, err = meter.Int64Histogram("count", metricapi.WithExplicitBucketBoundaries(10))
					measure = func(value int64) { instrument.Record(context.Background(), value) }
				}
				if err != nil {
					t.Fatal(err)
				}
				values := []int64{sample.value, nextIntegralValue(kind, sample.value)}
				for index, value := range values {
					measure(value)
					var data metricdata.ResourceMetrics
					if err := reader.Collect(context.Background(), &data); err != nil {
						t.Fatal(err)
					}
					assertNativeIntegral(t, data, kind, values[:index+1])
				}
			})
		}
	}
}

func TestInt64InternalCollectionAndOTLPTypedFields(t *testing.T) {
	for _, sample := range []struct {
		name  string
		value int64
	}{{"small", 101}, {"above-float64-exact-range", (1 << 53) + 1}} {
		for _, kind := range integralKinds {
			t.Run(sample.name+"/"+kind, func(t *testing.T) {
				received := make(chan *metricpb.Metric, 8)
				definition := InstrumentV1{Name: "count", Kind: kind}
				if kind == "int64-histogram" {
					definition.Boundaries = []float64{10}
				}
				fixture := newFixture(t, OptionsV1{MetricsEndpoint: "metrics", Instruments: []InstrumentV1{definition}},
					http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						data, err := io.ReadAll(request.Body)
						if err != nil {
							t.Error(err)
							writer.WriteHeader(http.StatusBadRequest)
							return
						}
						var decoded colmetric.ExportMetricsServiceRequest
						if err := proto.Unmarshal(data, &decoded); err != nil {
							t.Error(err)
							writer.WriteHeader(http.StatusBadRequest)
							return
						}
						for _, resource := range decoded.ResourceMetrics {
							for _, scope := range resource.ScopeMetrics {
								for _, metric := range scope.Metrics {
									received <- metric
								}
							}
						}
						writer.Header().Set("Content-Type", "application/x-protobuf")
					}))
				values := []int64{sample.value, nextIntegralValue(kind, sample.value)}
				for index, value := range values {
					receipt, err := fixture.client.MeasureInt64(context.Background(), fault.Correlation{Call: "integer"}, "count", value)
					if result := outcomeOf(t, receipt, err); result.Err() != nil {
						t.Fatal(result.Err())
					}
					var data metricdata.ResourceMetrics
					if err := fixture.owner.reader.Collect(context.Background(), &data); err != nil {
						t.Fatal(err)
					}
					assertNativeIntegral(t, data, kind, values[:index+1])
					if result := flushOne(t, fixture); result.Err() != nil {
						t.Fatal(result.Err())
					}
					select {
					case metric := <-received:
						assertWireIntegral(t, metric, kind, values[:index+1])
					default:
						t.Fatal("receiver did not observe the exported metric")
					}
				}
			})
		}
	}
}

func nextIntegralValue(kind string, first int64) int64 {
	if kind == "int64-updowncounter" {
		return -4
	}
	if kind == "int64-gauge" {
		return -first
	}
	return 4
}

func integralExpected(kind string, values []int64) int64 {
	if kind == "int64-gauge" {
		return values[len(values)-1]
	}
	var sum int64
	for _, value := range values {
		sum += value
	}
	return sum
}

func assertNativeIntegral(t *testing.T, data metricdata.ResourceMetrics, kind string, values []int64) {
	t.Helper()
	if len(data.ScopeMetrics) != 1 || len(data.ScopeMetrics[0].Metrics) != 1 {
		t.Fatal("unexpected native metric shape")
	}
	want := integralExpected(kind, values)
	var got int64
	switch aggregation := data.ScopeMetrics[0].Metrics[0].Data.(type) {
	case metricdata.Sum[int64]:
		if len(aggregation.DataPoints) != 1 || aggregation.Temporality != metricdata.CumulativeTemporality ||
			aggregation.IsMonotonic != (kind == "int64-counter") {
			t.Fatal("unexpected integral sum semantics")
		}
		got = aggregation.DataPoints[0].Value
	case metricdata.Gauge[int64]:
		if kind != "int64-gauge" || len(aggregation.DataPoints) != 1 {
			t.Fatal("unexpected integral gauge")
		}
		got = aggregation.DataPoints[0].Value
	case metricdata.Histogram[int64]:
		if kind != "int64-histogram" || len(aggregation.DataPoints) != 1 || aggregation.Temporality != metricdata.CumulativeTemporality {
			t.Fatal("unexpected integral histogram")
		}
		point := aggregation.DataPoints[0]
		minimum, hasMin := point.Min.Value()
		maximum, hasMax := point.Max.Value()
		wantMin, wantMax := values[0], values[0]
		for _, value := range values {
			wantMin, wantMax = min(wantMin, value), max(wantMax, value)
		}
		if point.Count != uint64(len(values)) || !hasMin || !hasMax || minimum != wantMin || maximum != wantMax ||
			len(point.Bounds) != 1 || point.Bounds[0] != 10 || len(point.BucketCounts) != 2 ||
			point.BucketCounts[0] != uint64(len(values)-1) || point.BucketCounts[1] != 1 {
			t.Fatalf("histogram bounds/count/extrema changed: %+v", point)
		}
		got = point.Sum
	default:
		t.Fatalf("native aggregation lost int64 type: %T", aggregation)
	}
	t.Logf("native %s samples=%d want=%d got=%d", kind, len(values), want, got)
	if got != want {
		t.Errorf("native int64 aggregation lost precision: got %d, want %d", got, want)
	}
}

func assertWireIntegral(t *testing.T, metric *metricpb.Metric, kind string, values []int64) {
	t.Helper()
	want := integralExpected(kind, values)
	if kind == "int64-histogram" {
		histogram := metric.GetHistogram()
		if histogram == nil || histogram.AggregationTemporality != metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE || len(histogram.DataPoints) != 1 {
			t.Fatal("OTLP histogram shape changed")
		}
		point := histogram.DataPoints[0]
		minimum, maximum := values[0], values[0]
		for _, value := range values {
			minimum, maximum = min(minimum, value), max(maximum, value)
		}
		// OTLP histogram sum/extrema are double fields by protocol, unlike the
		// native int64 aggregation and OTLP NumberDataPoint.as_int above.
		if point.Sum == nil || point.Min == nil || point.Max == nil || point.GetSum() != float64(want) ||
			point.GetMin() != float64(minimum) || point.GetMax() != float64(maximum) ||
			point.Count != uint64(len(values)) || len(point.ExplicitBounds) != 1 || point.ExplicitBounds[0] != 10 ||
			len(point.BucketCounts) != 2 || point.BucketCounts[0] != uint64(len(values)-1) || point.BucketCounts[1] != 1 {
			t.Fatalf("OTLP histogram protocol values changed: %+v", point)
		}
		t.Logf("wire histogram native_sum=%d otlp_double_sum=%.0f count=%d", want, point.GetSum(), point.Count)
		return
	}
	var points []*metricpb.NumberDataPoint
	if kind == "int64-gauge" {
		points = metric.GetGauge().GetDataPoints()
	} else {
		sum := metric.GetSum()
		if sum == nil || sum.AggregationTemporality != metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE || sum.IsMonotonic != (kind == "int64-counter") {
			t.Fatal("OTLP sum semantics changed")
		}
		points = sum.DataPoints
	}
	if len(points) != 1 {
		t.Fatal("unexpected OTLP point count")
	}
	integer, ok := points[0].Value.(*metricpb.NumberDataPoint_AsInt)
	if !ok {
		t.Fatalf("integral instrument encoded as %T instead of as_int", points[0].Value)
	}
	t.Logf("wire %s samples=%d want=%d as_int=%d", kind, len(values), want, integer.AsInt)
	if integer.AsInt != want {
		t.Errorf("OTLP as_int lost precision: got %d, want %d", integer.AsInt, want)
	}
}
