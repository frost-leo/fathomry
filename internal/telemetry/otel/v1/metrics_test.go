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
	"errors"
	"log/slog"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricInstrumentsAggregationAndCardinality(t *testing.T) {
	definitions := []InstrumentV1{}
	for _, kind := range []string{"int64-counter", "float64-counter", "int64-updowncounter", "float64-updowncounter", "int64-gauge", "float64-gauge", "int64-histogram", "float64-histogram"} {
		instrument := InstrumentV1{Name: kind, Kind: kind, AttributeKeys: []string{"dimension"}}
		if kind == "int64-histogram" || kind == "float64-histogram" {
			instrument.Boundaries = []float64{1, 5, 10}
		}
		definitions = append(definitions, instrument)
	}
	fixture := newFixture(t, OptionsV1{MetricsEndpoint: "metrics", MetricCardinality: 2, Instruments: definitions}, nil)
	for _, instrument := range definitions {
		for _, dimension := range []string{"a", "b", "c"} {
			var resultErr error
			if instrument.Kind[:5] == "int64" {
				receipt, err := fixture.client.MeasureInt64(context.Background(), fault.Correlation{Call: "metric"}, instrument.Name, 2, slog.String("dimension", dimension))
				resultErr = outcomeOf(t, receipt, err).Err()
			} else {
				receipt, err := fixture.client.MeasureFloat64(context.Background(), fault.Correlation{Call: "metric"}, instrument.Name, 2, slog.String("dimension", dimension))
				resultErr = outcomeOf(t, receipt, err).Err()
			}
			if resultErr != nil {
				t.Fatal(resultErr)
			}
		}
	}
	var data metricdata.ResourceMetrics
	if err := fixture.owner.reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	if len(data.ScopeMetrics) != 1 || len(data.ScopeMetrics[0].Metrics) != 8 {
		t.Fatal("instrument scope/count changed")
	}
	for _, metric := range data.ScopeMetrics[0].Metrics {
		switch aggregation := metric.Data.(type) {
		case metricdata.Sum[int64]:
			total := int64(0)
			overflow := false
			for _, point := range aggregation.DataPoints {
				total += point.Value
				_, found := point.Attributes.Value("otel.metric.overflow")
				overflow = overflow || found
			}
			if total != 6 || len(aggregation.DataPoints) != 2 || !overflow || aggregation.Temporality != metricdata.CumulativeTemporality {
				t.Fatal("cardinality lost total/overflow")
			}
		case metricdata.Sum[float64]:
			total := 0.0
			for _, point := range aggregation.DataPoints {
				total += point.Value
			}
			if total != 6 {
				t.Fatal("float sum changed")
			}
		case metricdata.Histogram[int64]:
			var count uint64
			var sum int64
			for _, point := range aggregation.DataPoints {
				count += point.Count
				sum += point.Sum
				if len(point.Bounds) != 3 {
					t.Fatal("histogram bounds changed")
				}
			}
			if count != 3 || sum != 6 {
				t.Fatal("histogram changed")
			}
		case metricdata.Histogram[float64]:
			var count uint64
			sum := 0.0
			for _, point := range aggregation.DataPoints {
				count += point.Count
				sum += point.Sum
			}
			if count != 3 || sum != 6 {
				t.Fatal("float histogram changed")
			}
		case metricdata.Gauge[int64]:
			if len(aggregation.DataPoints) > 2 {
				t.Fatal("gauge cardinality unbounded")
			}
		case metricdata.Gauge[float64]:
			if len(aggregation.DataPoints) > 2 {
				t.Fatal("gauge cardinality unbounded")
			}
		default:
			t.Fatal("unexpected aggregation")
		}
	}
}
func TestMetricInvalidMeasurementsDoNotCreateInstruments(t *testing.T) {
	fixture := newFixture(t, OptionsV1{MetricsEndpoint: "metrics", Instruments: []InstrumentV1{{Name: "count", Kind: "int64-counter", AttributeKeys: []string{"region"}}}}, nil)
	for _, test := range []struct {
		name  string
		value int64
		attrs []slog.Attr
	}{
		{"unknown", 1, nil}, {"count", -1, nil}, {"count", 1, []slog.Attr{slog.String("item", "high-cardinality")}},
		{"count", 1, []slog.Attr{slog.Group("region", slog.String("x", "y"))}},
	} {
		receipt, err := fixture.client.MeasureInt64(context.Background(), fault.Correlation{Call: "invalid"}, test.name, test.value, test.attrs...)
		if outcomeOf(t, receipt, err).Err() == nil {
			t.Fatal("unsupported measurement accepted")
		}
	}
	receipt, err := fixture.client.MeasureFloat64(context.Background(), fault.Correlation{Call: "wrong-type"}, "count", 1)
	if !errors.Is(outcomeOf(t, receipt, err).Err(), ErrUnsupported) {
		t.Fatal("instrument type mismatch ignored")
	}
	var data metricdata.ResourceMetrics
	if err := fixture.owner.reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	if dataPoints(data) != 0 {
		t.Fatal("rejected measurements affected SDK")
	}
}
