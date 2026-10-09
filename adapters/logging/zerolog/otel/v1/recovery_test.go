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

package zerologotel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	bridge "github.com/frost-leo/fathomry/adapters/logging/zerolog/otel/v1"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	collog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logproto "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

type publicRecoveryFixture struct {
	telemetry                       *otel.Owner
	logger                          *zerolog.Owner
	telemetryRuntime, loggerRuntime *adapters.Runtime
	telemetryInbox                  *adapters.Inbox[otel.Result]
	loggerInbox                     *adapters.Inbox[zerolog.Result]
	local                           bytes.Buffer
	mu                              sync.Mutex
	received                        []*logproto.LogRecord
	peerEntered                     chan struct{}
	peerRelease                     <-chan struct{}
	peerOnce                        sync.Once
}

func recoveryPointer[T any](value T) *T { return &value }

func publicRecoverySetup(t *testing.T, options otel.Settings, legacy bool, route ...func(*publicRecoveryFixture, otel.Budget) *otel.Client) *publicRecoveryFixture {
	t.Helper()
	fixture := new(publicRecoveryFixture)
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(io.LimitReader(request.Body, 4<<20))
		if err != nil {
			t.Error(err)
			writer.WriteHeader(400)
			return
		}
		if request.URL.Path == "/logs" {
			var logs collog.ExportLogsServiceRequest
			if err := proto.Unmarshal(data, &logs); err != nil {
				t.Error(err)
				writer.WriteHeader(400)
				return
			}
			fixture.mu.Lock()
			for _, resource := range logs.ResourceLogs {
				for _, scope := range resource.ScopeLogs {
					fixture.received = append(fixture.received, scope.LogRecords...)
				}
			}
			fixture.mu.Unlock()
		}
		fixture.mu.Lock()
		entered, release := fixture.peerEntered, fixture.peerRelease
		fixture.mu.Unlock()
		if release != nil {
			fixture.peerOnce.Do(func() { close(entered) })
			<-release
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(peer.Close)
	options.Name, options.Version, options.ServiceName = "telemetry", 1, "public-recovery"
	if options.LogsEndpoint != "" {
		options.LogsEndpoint = peer.URL + "/logs"
	}
	if options.TracesEndpoint != "" {
		options.TracesEndpoint = peer.URL + "/traces"
	}
	policy, err := otel.Recommend(options)
	if err != nil {
		t.Fatal(err)
	}
	fixture.telemetryRuntime, err = adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	fixture.telemetryInbox, err = adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	fixture.telemetry, err = otel.Open(context.Background(), options, otel.Dependencies{Runtime: fixture.telemetryRuntime, Evidence: fixture.telemetryInbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := fixture.telemetry.Close(ctx); err != nil {
			t.Error(err)
		}
		if !fixture.telemetry.ShutdownComplete() {
			t.Error("telemetry ownership not released")
		}
		if err := fixture.telemetryRuntime.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := fixture.telemetryInbox.Seal(); err != nil {
			t.Error(err)
		}
		publicRecoveryDrainAll(t, fixture.telemetryInbox)
	})
	client := fixture.telemetry.Client()
	if len(route) != 0 {
		client = route[0](fixture, policy.Budget)
	}
	sink, err := bridge.New(client)
	if err != nil {
		t.Fatal(err)
	}
	kind := "managed-record"
	if legacy {
		kind = "record"
	}
	settings := zerolog.Settings{Name: "logging", Version: 1, Sinks: []zerolog.Sink{
		{Name: "remote", Kind: kind}, {Name: "local", Kind: "writer"},
	}}
	logPolicy, err := zerolog.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	fixture.loggerRuntime, err = adapters.New(context.Background(), logPolicy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	fixture.loggerInbox, err = adapters.NewInbox[zerolog.Result](logPolicy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := zerolog.Dependencies{Runtime: fixture.loggerRuntime, Evidence: fixture.loggerInbox, Writers: map[string]io.Writer{"local": &fixture.local}}
	if legacy {
		dependencies.Records = map[string]zerolog.RecordWriter{"remote": sink}
	} else {
		dependencies.ManagedRecords = map[string]zerolog.ManagedRecordWriter{"remote": sink}
	}
	fixture.logger, err = zerolog.Open(context.Background(), settings, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := fixture.logger.Close(ctx); err != nil {
			t.Error(err)
		}
		if !fixture.logger.ShutdownComplete() {
			t.Error("logging ownership not released")
		}
		if err := fixture.loggerRuntime.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := fixture.loggerInbox.Seal(); err != nil {
			t.Error(err)
		}
		publicRecoveryDrainAll(t, fixture.loggerInbox)
	})
	return fixture
}
func publicRecoveryDrainFinite[T any](t *testing.T, inbox *adapters.Inbox[T], retained int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		status, err := inbox.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if status.Outstanding <= retained {
			return
		}
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
func publicRecoveryDrainAll[T any](t *testing.T, inbox *adapters.Inbox[T]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		delivery, err := inbox.NextReleased(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	status, err := inbox.Inspect()
	if err != nil || status.Outstanding != 0 || status.Bytes != 0 {
		t.Fatal("evidence custody not released", err)
	}
}
func (fixture *publicRecoveryFixture) log(t *testing.T, message string, attrs ...slog.Attr) (zerolog.Result, error) {
	t.Helper()
	receipt, err := fixture.logger.Client().Log(context.Background(), zerolog.Info, message, attrs...)
	if err != nil || receipt == nil {
		t.Fatal("logging admission", err)
	}
	snapshot, err := receipt.WaitReleased(context.Background())
	if err != nil || !snapshot.Info().Released {
		t.Fatal("logging actual release", err)
	}
	result, found := snapshot.ValueCopy()
	if !found || !result.HasData() {
		t.Fatal("logging outcome absent", snapshot.Err())
	}
	publicRecoveryDrainFinite(t, fixture.loggerInbox, 2)
	return result, snapshot.Err()
}
func (fixture *publicRecoveryFixture) flush(t *testing.T) {
	t.Helper()
	publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
	receipt, err := fixture.telemetry.Client().Flush(context.Background())
	if err != nil || receipt == nil {
		t.Fatal("flush admission", err)
	}
	snapshot, err := receipt.WaitReleased(context.Background())
	if err != nil || snapshot.Err() != nil {
		t.Fatal("flush result", err, snapshot.Err())
	}
	publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
}
func (fixture *publicRecoveryFixture) messages() []string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	var values []string
	for _, record := range fixture.received {
		values = append(values, record.Body.GetStringValue())
	}
	return values
}
func publicRejected(t *testing.T, result zerolog.Result, err error) {
	t.Helper()
	sinks := result.SinksCopy()
	if err == nil || len(sinks) != 2 || !sinks[0].Attempted || sinks[0].Accepted || sinks[0].Stopped || sinks[0].BytesKnown || sinks[0].WriteError == nil || !sinks[1].Accepted {
		t.Fatal("event rejection hid original error or sibling result", err)
	}
}
func publicAccepted(t *testing.T, result zerolog.Result, err error) {
	t.Helper()
	sinks := result.SinksCopy()
	if err != nil || len(sinks) != 2 || !sinks[0].Attempted || !sinks[0].Accepted || sinks[0].Stopped || !sinks[1].Accepted {
		t.Fatal("independent legal record refused", err)
	}
}
func publicKey(values []*common.KeyValue, key string) *common.AnyValue {
	for _, value := range values {
		if value.Key == key {
			return value.Value
		}
	}
	return nil
}

func TestPublicManagedOverflowThenLegalRecordReachesActualOTLP(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled"}, false)
	first, firstErr := fixture.log(t, "overflow", slog.Uint64("number", math.MaxUint64))
	publicRejected(t, first, firstErr)
	var local map[string]any
	decoder := json.NewDecoder(bytes.NewReader(fixture.local.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(&local); err != nil {
		t.Fatal(err)
	}
	if local["attributes"].(map[string]any)["number"] != json.Number("18446744073709551615") {
		t.Fatal("full local uint64 changed")
	}
	next, err := fixture.log(t, "exact", slog.Int64("number", (1<<53)+1))
	publicAccepted(t, next, err)
	fixture.flush(t)
	if fmt.Sprint(fixture.messages()) != "[exact]" {
		t.Fatal("failed record replayed or valid event lost", fixture.messages())
	}
	fixture.mu.Lock()
	wire := fixture.received[0]
	number := publicKey(publicKey(wire.Attributes, "attributes").GetKvlistValue().Values, "number")
	correlation := publicKey(publicKey(wire.Attributes, "logging").GetKvlistValue().Values, "call").GetStringValue()
	fixture.mu.Unlock()
	if _, ok := number.Value.(*common.AnyValue_IntValue); !ok || number.GetIntValue() != (1<<53)+1 {
		t.Fatal("wire integer lost precision")
	}
	if correlation == "" || correlation != next.NativeCorrelation().Call {
		t.Fatal("receipt to wire correlation lost")
	}
	publicRejected(t, first, firstErr)
}

func TestPublicManagedFullQueueRecoversOnlyForNextEvent(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled", QueueItems: recoveryPointer(1)}, false)
	first, err := fixture.log(t, "queued")
	publicAccepted(t, first, err)
	refused, err := fixture.log(t, "queue-full")
	publicRejected(t, refused, err)
	if !errors.Is(err, otel.ErrLimit) {
		t.Fatal("actual queue refusal lost public identity", err)
	}
	fixture.flush(t)
	next, err := fixture.log(t, "after-flush")
	publicAccepted(t, next, err)
	fixture.flush(t)
	if fmt.Sprint(fixture.messages()) != "[queued after-flush]" {
		t.Fatal("queue saturation replayed or lost event", fixture.messages())
	}
}

func TestPublicManagedFullEvidenceRecoversAtUnchangedCapacity(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled", ActiveCalls: recoveryPointer(1)}, false)
	first, err := fixture.log(t, "queued")
	publicAccepted(t, first, err)
	status, _ := fixture.telemetryInbox.Inspect()
	if status.Outstanding != 2 {
		t.Fatal("did not retain source plus finite evidence")
	}
	refused, err := fixture.log(t, "evidence-full")
	publicRejected(t, refused, err)
	if !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("not actual public evidence saturation", err)
	}
	publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
	receipt, err := fixture.telemetry.Client().Emit(context.Background(), otel.LogRecord{Message: "direct-control"})
	if err != nil || receipt == nil {
		t.Fatal("direct healthy control", err)
	}
	snapshot, err := receipt.WaitReleased(context.Background())
	if err != nil || snapshot.Err() != nil {
		t.Fatal("healthy direct control failed", err, snapshot.Err())
	}
	publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
	next, err := fixture.log(t, "after-drain")
	publicAccepted(t, next, err)
	fixture.flush(t)
	if fmt.Sprint(fixture.messages()) != "[queued direct-control after-drain]" {
		t.Fatal("evidence refusal was retried", fixture.messages())
	}
}

func TestPublicManagedHeldSpanAdmissionRecoversAfterRelease(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled", TracesEndpoint: "enabled", ActiveCalls: recoveryPointer(1)}, false)
	_, span, err := fixture.telemetry.Client().Start(context.Background(), otel.SpanInput{Name: "held"})
	if err != nil || span == nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = span.End(context.Background()) })
	refused, err := fixture.log(t, "active-full")
	publicRejected(t, refused, err)
	if !errors.Is(err, adapters.ErrLimit) && !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("held span did not consume fixed public capacity", err)
	}
	if receipt, err := span.End(context.Background()); err != nil || receipt == nil {
		t.Fatal("span release", err)
	}
	publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
	next, err := fixture.log(t, "after-span")
	publicAccepted(t, next, err)
	fixture.flush(t)
	if fmt.Sprint(fixture.messages()) != "[after-span]" {
		t.Fatal("held-span refusal replayed", fixture.messages())
	}
}

func TestPublicManagedClosedOrDisabledDestinationStopsOnlyRemote(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprint(closed), func(t *testing.T) {
			options := otel.Settings{TracesEndpoint: "enabled"}
			if closed {
				options.LogsEndpoint = "enabled"
			}
			fixture := publicRecoverySetup(t, options, false)
			if closed {
				if err := fixture.telemetry.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			first, err := fixture.log(t, "terminal")
			reports := first.SinksCopy()
			if err == nil || !reports[0].Attempted || !reports[0].Stopped || reports[0].Accepted || !reports[1].Accepted {
				t.Fatal("terminal source did not stop only remote", err)
			}
			later, err := fixture.log(t, "later")
			reports = later.SinksCopy()
			if err == nil || reports[0].Attempted || !reports[0].Stopped || !reports[1].Accepted {
				t.Fatal("terminal source was retried", err)
			}
			if len(fixture.messages()) != 0 {
				t.Fatal("terminal target received data")
			}
		})
	}
}

func TestPublicLegacyRecordBridgeStillFailsStop(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled"}, true)
	first, err := fixture.log(t, "overflow", slog.Uint64("number", math.MaxUint64))
	if err == nil || !first.SinksCopy()[0].Stopped || !first.SinksCopy()[1].Accepted {
		t.Fatal("legacy failure-stop relaxed")
	}
	next, err := fixture.log(t, "legal")
	if err == nil || next.SinksCopy()[0].Attempted || !next.SinksCopy()[0].Stopped || !next.SinksCopy()[1].Accepted {
		t.Fatal("legacy rejected sink resumed")
	}
	fixture.flush(t)
	if len(fixture.messages()) != 0 {
		t.Fatal("legacy sink retried")
	}
}

func runPublicRecoveryReceiver[T any](inbox *adapters.Inbox[T]) <-chan error {
	done := make(chan error, 1)
	go func() {
		for {
			delivery, err := inbox.NextReleased(context.Background())
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			if err != nil {
				done <- err
				return
			}
			if err := delivery.Ack(); err != nil {
				done <- err
				return
			}
		}
	}()
	return done
}
func TestPublicManagedIndependentReceiversAndExportLoopProgress(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled", QueueItems: recoveryPointer(1)}, false)
	loggingDone := runPublicRecoveryReceiver(fixture.loggerInbox)
	telemetryDone := runPublicRecoveryReceiver(fixture.telemetryInbox)
	loop, err := fixture.telemetry.StartExport(context.Background(), otel.ExportOptions{Interval: time.Millisecond, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	receipt, err := fixture.logger.Client().Log(ctx, zerolog.Info, "loop-delivered")
	if err != nil || receipt == nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		t.Fatal(err, snapshot.Err())
	}
	for len(fixture.messages()) == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("controlled export did not progress", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := fixture.logger.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.loggerRuntime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.loggerInbox.Seal(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-loggingDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("logging receiver did not join")
	}
	if err := loop.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := loop.Status()
	if err != nil || !status.Stopped || status.Running || status.Succeeded < 1 {
		t.Fatal("export loop work did not join", err)
	}
	if err := fixture.telemetry.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.telemetryRuntime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.telemetryInbox.Seal(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-telemetryDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("telemetry receiver did not join")
	}
	if fmt.Sprint(fixture.messages()) != "[loop-delivered]" {
		t.Fatal("schedule replayed accepted record", fixture.messages())
	}
}

func TestPublicManagedFixedUnavailableAndBorrowerRefusalCanRecover(t *testing.T) {
	var scope *resource.Scope
	var ref resource.Ref[otel.Handle]
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled"}, false, func(fixture *publicRecoveryFixture, budget otel.Budget) *otel.Client {
		var err error
		scope, err = resource.New(context.Background(), resource.Options{Name: "fixed-recovery"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := scope.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		ref, err = resource.Bind(scope, resource.Binding[int, otel.Handle]{
			Name: "target", Policy: resource.Fixed, MaxBorrowers: 1,
			Select: func(settings.View) (int, error) { return 1, nil },
			Clone:  func(value int) int { return value },
			Build: func(context.Context, int) (*resource.Instance[otel.Handle], error) {
				return &resource.Instance[otel.Handle]{Value: fixture.telemetry.Handle()}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		client, err := otel.Using(context.Background(), ref, budget, otel.Dependencies{Runtime: fixture.telemetryRuntime, Evidence: fixture.telemetryInbox})
		if err != nil {
			t.Fatal(err)
		}
		return client
	})
	before, err := fixture.log(t, "before-ready")
	publicRejected(t, before, err)
	if !errors.Is(err, resource.ErrUnavailable) {
		t.Fatal("not actual unavailable fixed binding", err)
	}
	view, err := settings.New(1, func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(context.Background(), view.View())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := ref.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	full, err := fixture.log(t, "borrowers-full")
	publicRejected(t, full, err)
	if !errors.Is(err, resource.ErrLimit) {
		t.Fatal("not actual fixed borrower saturation", err)
	}
	if err := held.Release(); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.log(t, "after-ready")
	publicAccepted(t, after, err)
	fixture.flush(t)
	if fmt.Sprint(fixture.messages()) != "[after-ready]" {
		t.Fatal("binding refusal replayed or hid subsequent legal event", fixture.messages())
	}
	if err := scope.Close(ctx); err != nil {
		t.Fatal(err)
	}
	closed, err := fixture.log(t, "after-scope-close")
	if err == nil || !closed.SinksCopy()[0].Stopped || !closed.SinksCopy()[1].Accepted {
		t.Fatal("closed fixed binding was not terminal")
	}
	status, err := ref.Inspect()
	if err != nil || status.Borrowers != 0 || !status.Closed {
		t.Fatal("profile probe leaked source lease", err)
	}
}

func TestPublicManagedCanceledNativeGateAdmissionDoesNotStopLiveDestination(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled", ActiveCalls: recoveryPointer(2)}, false)
	seed, err := fixture.log(t, "seed")
	publicAccepted(t, seed, err)
	publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	fixture.mu.Lock()
	fixture.peerEntered, fixture.peerRelease = entered, release
	fixture.mu.Unlock()
	unblock := sync.OnceFunc(func() { close(release) })
	flushDone := make(chan struct{})
	var flushErr error
	go func() {
		defer close(flushDone)
		receipt, err := fixture.telemetry.Client().Flush(context.Background())
		if err != nil {
			flushErr = err
			return
		}
		snapshot, err := receipt.WaitReleased(context.Background())
		flushErr = errors.Join(err, snapshot.Err())
	}()
	t.Cleanup(func() {
		unblock()
		select {
		case <-flushDone:
		case <-time.After(3 * time.Second):
			t.Error("blocked export not actually joined")
		}
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("export never entered actual peer")
	}
	canceled, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	receipt, err := fixture.logger.Client().Log(canceled, zerolog.Info, "cancel-at-native-gate")
	if receipt == nil {
		t.Fatal("no admitted logging evidence", err)
	}
	snapshot, waitErr := receipt.WaitReleased(context.Background())
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	result, ok := snapshot.ValueCopy()
	if !ok || snapshot.Err() == nil || !errors.Is(snapshot.Err(), context.DeadlineExceeded) {
		t.Fatal("not actual canceled accepted operation", err, snapshot.Err())
	}
	remote := result.SinksCopy()[0]
	if !remote.Attempted || remote.Accepted || remote.Stopped {
		t.Error("canceled native gate admission incorrectly stopped a healthy destination")
	}
	unblock()
	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("export did not finish after peer release")
	}
	if flushErr != nil {
		t.Fatal("independent earlier export failed", flushErr)
	}
	publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
	publicRecoveryDrainFinite(t, fixture.loggerInbox, 2)
	next, err := fixture.log(t, "after-canceled-gate")
	publicAccepted(t, next, err)
	fixture.flush(t)
	if fmt.Sprint(fixture.messages()) != "[seed after-canceled-gate]" {
		t.Fatal("canceled event replayed or valid successor lost", fixture.messages())
	}
}

type publicShortWriter struct {
	calls, syncs, closes int
}

func (writer *publicShortWriter) Write(data []byte) (int, error) {
	writer.calls++
	return len(data) - 1, nil
}
func (writer *publicShortWriter) Sync() error  { writer.syncs++; return nil }
func (writer *publicShortWriter) Close() error { writer.closes++; return nil }

func TestPublicLegacyByteShortWriteStopsAndRemainsBorrowed(t *testing.T) {
	writer := new(publicShortWriter)
	settings := zerolog.Settings{Name: "short-writer", Version: 1, Sinks: []zerolog.Sink{{Name: "local", Kind: "writer"}}}
	policy, err := zerolog.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[zerolog.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := zerolog.Open(context.Background(), settings, zerolog.Dependencies{Runtime: runtime, Evidence: inbox, Writers: map[string]io.Writer{"local": writer}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if err := inbox.Seal(); err != nil {
			t.Error(err)
		}
		publicRecoveryDrainAll(t, inbox)
	})
	for index := 0; index < 2; index++ {
		receipt, err := owner.Client().Log(context.Background(), zerolog.Info, "short")
		if err != nil || receipt == nil {
			t.Fatal(err)
		}
		snapshot, err := receipt.WaitReleased(context.Background())
		if err != nil || !errors.Is(snapshot.Err(), io.ErrShortWrite) {
			t.Fatal("short-write cause lost", err, snapshot.Err())
		}
		result, _ := snapshot.ValueCopy()
		sink := result.SinksCopy()[0]
		if !sink.Stopped || sink.Accepted || sink.Attempted != (index == 0) {
			t.Fatal("ordinary byte stream became independent-event recoverable")
		}
		if index == 0 && (!sink.BytesKnown || sink.Written <= 0) {
			t.Fatal("actual partial byte fact lost")
		}
	}
	if receipt, err := owner.Client().Sync(context.Background()); receipt == nil && err == nil {
		t.Fatal("missing maintenance refusal")
	} else if receipt != nil {
		snapshot, waitErr := receipt.WaitReleased(context.Background())
		if waitErr != nil || !errors.Is(snapshot.Err(), zerolog.ErrUnsupported) {
			t.Fatal("borrowed writer maintenance became owned", waitErr, snapshot.Err())
		}
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if writer.calls != 1 || writer.syncs != 0 || writer.closes != 0 {
		t.Fatal("borrowed byte stream was retried, synced or closed")
	}
}
