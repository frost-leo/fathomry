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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"time"

	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("otel framework public consumer passed")
}

type observation struct{ source, kind, text string }

func peer(name string, records chan<- observation) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
		if err != nil {
			writer.WriteHeader(500)
			return
		}
		switch request.URL.Path {
		case "/logs":
			var data collectorlogs.ExportLogsServiceRequest
			if proto.Unmarshal(raw, &data) != nil {
				writer.WriteHeader(400)
				return
			}
			for _, res := range data.ResourceLogs {
				for _, scope := range res.ScopeLogs {
					for _, record := range scope.LogRecords {
						records <- observation{name, "log", record.Body.GetStringValue()}
					}
				}
			}
		case "/traces":
			var data collectortrace.ExportTraceServiceRequest
			if proto.Unmarshal(raw, &data) != nil {
				writer.WriteHeader(400)
				return
			}
			for _, res := range data.ResourceSpans {
				for _, scope := range res.ScopeSpans {
					for _, record := range scope.Spans {
						records <- observation{name, "span", record.Name}
					}
				}
			}
		default:
			writer.WriteHeader(404)
			return
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
}

type adopted struct {
	owner *otel.Owner
	loop  *otel.ExportLoop
}

func run(ctx context.Context) (result error) {
	records := make(chan observation, 16)
	first, second := peer("first", records), peer("second", records)
	defer first.Close()
	defer second.Close()
	prepare := func(name, endpoint string) (otel.Prepared, error) {
		return otel.Prepare(otel.Settings{Name: name, ServiceName: "consumer", LogsEndpoint: endpoint + "/logs", TracesEndpoint: endpoint + "/traces"})
	}
	original, err := prepare("first", first.URL)
	if err != nil {
		return err
	}
	replacement, err := prepare("second", second.URL)
	if err != nil {
		return err
	}
	policy, err := otel.Compose(original, original, replacement)
	if err != nil {
		return err
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		result = errors.Join(result, runtime.Close(cleanup))
	}()
	inbox, err := adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := otel.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	var mu sync.Mutex
	seen := map[uint64]adapters.Info{}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, snapshot adapters.Snapshot[otel.Result]) error {
		mu.Lock()
		defer mu.Unlock()
		info := snapshot.Info()
		if _, exists := seen[info.Sequence]; exists {
			return errors.New("duplicate receipt custody")
		}
		seen[info.Sequence] = info
		return nil
	})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		result = errors.Join(result, receiver.Close(cleanup))
	}()
	sources := map[string][]adopted{}
	bind := func(name string, mode resource.Policy) (resource.Ref[otel.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[int, otel.Handle]{Name: name, Policy: mode,
			Select: func(view settings.View) (int, error) {
				snapshot, err := settings.As[int](view)
				if err != nil {
					return 0, err
				}
				return snapshot.ValueCopy()
			},
			Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
			Build: func(lifetime context.Context, revision int) (*resource.Instance[otel.Handle], error) {
				prepared := original
				if revision == 1 {
					prepared = replacement
				}
				owner, err := prepared.Open(lifetime, dependencies)
				if owner == nil {
					return nil, err
				}
				instance := &resource.Instance[otel.Handle]{Value: owner.Handle(), Release: owner.Release}
				if err != nil {
					return instance, err
				}
				loop, err := owner.StartExport(lifetime, otel.ExportOptions{Interval: 10 * time.Millisecond, Timeout: time.Second})
				mu.Lock()
				sources[name] = append(sources[name], adopted{owner, loop})
				mu.Unlock()
				return instance, err
			},
		})
	}
	fixedRef, err := bind("fixed", resource.Fixed)
	if err != nil {
		return err
	}
	followRef, err := bind("follow", resource.Follow)
	if err != nil {
		return err
	}
	apply := func(revision int) error {
		snapshot, err := settings.New(revision, func(value int) int { return value })
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err := apply(0); err != nil {
		return err
	}
	fixed, err := otel.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := otel.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	originalStatus, err := followRef.Inspect()
	if err != nil {
		return err
	}
	associated, span, err := follow.Start(ctx, otel.SpanInput{Name: "old-held-span"})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := span.End(cleanup)
		result = errors.Join(result, err)
	}()
	if err := apply(1); err != nil {
		return err
	}
	newStatus, err := followRef.Inspect()
	if err != nil {
		return err
	}
	if newStatus.Generation == originalStatus.Generation || newStatus.Retiring != 1 {
		return errors.New("Follow did not retain old span generation")
	}
	mu.Lock()
	old := sources["follow"][0]
	mu.Unlock()
	if old.owner.ShutdownComplete() {
		return errors.New("held generation released prematurely")
	}
	await := func(receipt *adapters.Receipt[otel.Result], err error) (otel.Result, error) {
		if err != nil {
			return otel.Result{}, err
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil {
			return otel.Result{}, err
		}
		value, _ := snapshot.ValueCopy()
		return value, snapshot.Err()
	}
	receipt, err := old.owner.Client().Emit(associated, otel.LogRecord{Message: "old-after-update"})
	if _, err := await(receipt, err); err != nil {
		return err
	}
	receipt, err = follow.Emit(ctx, otel.LogRecord{Message: "follow-new"})
	value, err := await(receipt, err)
	if err != nil {
		return err
	}
	if value.Attribution().Source.Generation != newStatus.Generation {
		return errors.New("new record has stale generation")
	}
	receipt, err = fixed.Emit(ctx, otel.LogRecord{Message: "fixed-old"})
	value, err = await(receipt, err)
	if err != nil {
		return err
	}
	if value.Source().Name != "first" {
		return errors.New("Fixed source changed")
	}
	receipt, err = span.End(ctx)
	value, err = await(receipt, err)
	if err != nil {
		return err
	}
	if value.Attribution().Source.Generation != originalStatus.Generation {
		return errors.New("old span was retargeted")
	}
	wanted := map[observation]bool{{"first", "log", "old-after-update"}: false, {"second", "log", "follow-new"}: false, {"first", "log", "fixed-old"}: false, {"first", "span", "old-held-span"}: false}
	for left := len(wanted); left > 0; {
		select {
		case item := <-records:
			received, exists := wanted[item]
			if !exists || received {
				return errors.New("unexpected or duplicate exported record")
			}
			wanted[item] = true
			left--
		case <-ctx.Done():
			return errors.New("old/new endpoint progress missing")
		}
	}
	// Stop producers first; release sources while operation admission and the
	// independent evidence receiver remain available for final export/cleanup.
	if err := runtime.Resources().Close(ctx); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	if err := receiver.Finish(ctx); err != nil {
		return err
	}
	status, err := receiver.Status()
	if err != nil || status.Failures != 0 || status.Delivered < 7 {
		return errors.New("independent custody did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, entries := range sources {
		for _, entry := range entries {
			loop, err := entry.loop.Status()
			if err != nil || !loop.Stopped || !entry.owner.ShutdownComplete() {
				return errors.New("source or loop retained after cleanup")
			}
		}
	}
	if len(sources["fixed"]) != 1 || len(sources["follow"]) != 2 {
		return errors.New("unexpected physical generation count")
	}
	return nil
}
