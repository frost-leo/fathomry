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

package iceberg_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/metrics"
	nativeotel "github.com/apache/iceberg-go/metrics/otel"
	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPublicRESTReporterLossAndSourceOwnedJoin(t *testing.T) {
	server := dataBenchmarkPeer(t)
	defer server.Close()
	settings := publicSettings(server.URL)
	settings.Properties = map[string]string{"rest-metrics-reporting-enabled": "true"}
	settings.MaxOperations, settings.MaxHandles = 384, 256
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var requests atomic.Int64
	var logs bytes.Buffer
	owner, inbox, _ := openPublicNative(t, settings, iceberg.NativeOptions{
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		Transport: func(base *http.Transport) (http.RoundTripper, func() error, error) {
			return serviceRoundTripper(func(request *http.Request) (*http.Response, error) {
				if strings.HasSuffix(request.URL.Path, "/tables/historical/metrics") {
					requests.Add(1)
					entered <- struct{}{}
					<-request.Context().Done()
					<-release
					return nil, request.Context().Err()
				}
				return base.RoundTrip(request)
			}), func() error { return nil }, nil
		},
	})
	receipt, err := owner.Client().Use(testContext(t), func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{"isolated", "historical"})
		if err != nil {
			return err
		}
		for range 140 {
			scan, err := tbl.Scan(iceberg.ScanOptions{})
			if err != nil {
				return err
			}
			tasks, err := scan.PlanFiles()
			if err != nil {
				return err
			}
			if len(tasks) != 1 || tasks[0].File.Count() != 65 {
				return errors.New("native reported plan changed")
			}
			if err = scan.Close(); err != nil {
				return err
			}
		}
		return nil
	})
	if result := settlePublic(t, receipt, err); result.Err() != nil {
		t.Fatal("optional report saturation failed accepted work", result.Err())
	}
	ackPublic(t, inbox)
	for range 4 {
		select {
		case <-entered:
		case <-testContext(t).Done():
			t.Fatal("native dispatcher worker not exercised")
		}
	}
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = owner.Close(short)
	cancel()
	if err == nil || owner.ShutdownComplete() {
		t.Error("source released before actual reporter transport return")
	}
	unblock()
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("native REST report dispatcher did not join", err)
	}
	if requests.Load() != 4 || !strings.Contains(logs.String(), "dropped_total") {
		t.Fatal("bounded native report loss was not observed")
	}
}

type testNativeReporter struct {
	report func(context.Context, metrics.MetricsReport)
	closed atomic.Int64
}

func (r *testNativeReporter) Report(ctx context.Context, report metrics.MetricsReport) {
	r.report(ctx, report)
}
func (r *testNativeReporter) Close() error { r.closed.Add(1); return nil }

func TestPublicScopedReporterFailureDoesNotFailData(t *testing.T) {
	server := dataBenchmarkPeer(t)
	defer server.Close()
	var logs bytes.Buffer
	sourceReporter := &testNativeReporter{report: func(context.Context, metrics.MetricsReport) { panic("source-report-only") }}
	owner, inbox, _ := openPublicNative(t, publicSettings(server.URL), iceberg.NativeOptions{Logger: slog.New(slog.NewTextHandler(&logs, nil)), Reporter: sourceReporter})
	reporter := &testNativeReporter{report: func(context.Context, metrics.MetricsReport) { panic("report-only-canary") }}
	receipt, err := owner.Client().Use(testContext(t), func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{"isolated", "historical"})
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(iceberg.ScanOptions{Reporter: reporter})
		if err != nil {
			return err
		}
		rows := benchmarkRows{}
		summary, err := scan.Records(rows.visit)
		if err != nil {
			return err
		}
		return rows.complete(summary.Complete)
	})
	if result := settlePublic(t, receipt, err); result.Err() != nil {
		t.Fatal("optional reporter changed data outcome", result.Err())
	}
	ackPublic(t, inbox)
	receipt, err = owner.Client().Use(testContext(t), func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{"isolated", "historical"})
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(iceberg.ScanOptions{})
		if err != nil {
			return err
		}
		_, err = scan.PlanFiles()
		return err
	})
	if result := settlePublic(t, receipt, err); result.Err() != nil {
		t.Fatal("source reporter changed planning outcome", result.Err())
	}
	ackPublic(t, inbox)
	if !strings.Contains(logs.String(), "source-report-only") || !strings.Contains(logs.String(), "report-only-canary") {
		t.Fatal("explicit source logger did not receive native report failures")
	}
	if reporter.closed.Load() != 0 {
		t.Fatal("borrowed reporter was closed")
	}
}

func TestPublicReporterActualJoinAndCallerOwnership(t *testing.T) {
	server := dataBenchmarkPeer(t)
	defer server.Close()
	entered, unblock, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	reporter := &testNativeReporter{report: func(context.Context, metrics.MetricsReport) {
		once.Do(func() { close(entered); <-unblock; close(exited) })
	}}
	owner, inbox, _ := openPublicNative(t, publicSettings(server.URL), iceberg.NativeOptions{Reporter: reporter})
	done := make(chan struct{})
	go func() {
		defer close(done)
		receipt, err := owner.Client().Use(testContext(t), func(s *iceberg.Session) error {
			tbl, err := s.LoadTable([]string{"isolated", "historical"})
			if err != nil {
				return err
			}
			scan, err := tbl.Scan(iceberg.ScanOptions{})
			if err != nil {
				return err
			}
			_, err = scan.Records(func(arrow.RecordBatch) (bool, error) { return true, nil })
			return err
		})
		if err != nil || receipt == nil {
			t.Error("reporting family not admitted", err)
			return
		}
		_, err = receipt.WaitReleased(testContext(t))
		if err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-testContext(t).Done():
		close(unblock)
		t.Fatal("native reporter not invoked")
	}
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err := owner.Close(short)
	cancel()
	if err == nil || owner.ShutdownComplete() {
		t.Error("source released while native reporter still running")
	}
	select {
	case <-exited:
		t.Error("reporter returned without release")
	default:
	}
	close(unblock)
	select {
	case <-done:
	case <-testContext(t).Done():
		t.Fatal("reporter/family did not join")
	}
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source did not complete actual release", err)
	}
	ackPublic(t, inbox)
	if reporter.closed.Load() != 0 {
		t.Fatal("source closed caller reporter")
	}
}

type failingMetricExporter struct {
	failure error
	exports atomic.Int64
	closed  atomic.Int64
}

func (*failingMetricExporter) Temporality(metric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
func (*failingMetricExporter) Aggregation(kind metric.InstrumentKind) metric.Aggregation {
	return metric.DefaultAggregationSelector(kind)
}
func (e *failingMetricExporter) Export(context.Context, *metricdata.ResourceMetrics) error {
	e.exports.Add(1)
	return e.failure
}
func (e *failingMetricExporter) ForceFlush(context.Context) error { return e.failure }
func (e *failingMetricExporter) Shutdown(context.Context) error   { e.closed.Add(1); return nil }

func TestPublicNativeOTelReportingAndIndependentFailure(t *testing.T) {
	server := dataBenchmarkPeer(t)
	defer server.Close()
	reader := metric.NewManualReader()
	marker := errors.New("test export unavailable")
	exporter := &failingMetricExporter{failure: marker}
	periodic := metric.NewPeriodicReader(exporter, metric.WithInterval(time.Hour))
	provider := metric.NewMeterProvider(metric.WithReader(reader), metric.WithReader(periodic))
	defer provider.Shutdown(context.Background())
	otelReporter, err := nativeotel.NewReporter(nativeotel.WithMeter(provider.Meter("gh137-native")), nativeotel.WithAttributes(nativeotel.AttrOperation))
	if err != nil {
		t.Fatal(err)
	}
	reports := &metrics.InMemoryReporter{}
	reporter := metrics.Combine(reports, otelReporter)
	owner, inbox, _ := openPublicNative(t, publicSettings(server.URL), iceberg.NativeOptions{Reporter: reporter})
	receipt, err := owner.Client().Use(testContext(t), func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{"isolated", "historical"})
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(iceberg.ScanOptions{})
		if err != nil {
			return err
		}
		rows := benchmarkRows{}
		summary, err := scan.Records(rows.visit)
		if err != nil {
			return err
		}
		return rows.complete(summary.Complete)
	})
	result := settlePublic(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	if err := provider.ForceFlush(testContext(t)); !errors.Is(err, marker) || exporter.exports.Load() == 0 {
		t.Fatal("export failure was not exercised", err)
	}
	if result.Err() != nil {
		t.Fatal("optional export failure changed independent result")
	}
	ackPublic(t, inbox)
	if len(reports.Reports()) != 1 {
		t.Fatal("native scan report missing or repeated")
	}
	var data metricdata.ResourceMetrics
	if err = reader.Collect(testContext(t), &data); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == "iceberg.scan.result.data_files" {
				values, ok := metric.Data.(metricdata.Sum[int64])
				if !ok || len(values.DataPoints) != 1 || values.DataPoints[0].Value != 1 {
					t.Fatal("native instrument has incorrect data")
				}
				for _, attribute := range values.DataPoints[0].Attributes.ToSlice() {
					if attribute.Key == "iceberg.table.name" || attribute.Key == "iceberg.schema.id" {
						t.Fatal("attribute suppression ignored")
					}
				}
				found = true
			}
		}
	}
	if !found {
		t.Fatal("native OTel reporter produced no instrument")
	}
	if err := owner.Close(testContext(t)); err != nil || exporter.closed.Load() != 0 {
		t.Fatal("source closed caller-owned OTel provider", err)
	}
	if err = reader.Collect(testContext(t), &data); err != nil {
		t.Fatal("caller-owned meter provider was revoked", err)
	}
}
