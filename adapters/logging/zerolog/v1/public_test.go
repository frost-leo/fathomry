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

package zerolog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func ptr[T any](value T) *T { return &value }

type memoryWriter struct {
	mu            sync.Mutex
	buffer        bytes.Buffer
	syncs, closes int
}

func (writer *memoryWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.Write(data)
}
func (writer *memoryWriter) Sync() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.syncs++
	return nil
}
func (writer *memoryWriter) Close() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.closes++
	return nil
}
func (writer *memoryWriter) data() []byte {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return bytes.Clone(writer.buffer.Bytes())
}
func openPublic(t *testing.T, settings zerolog.Settings, dependencies zerolog.Dependencies) (*zerolog.Owner, zerolog.Dependencies) {
	t.Helper()
	prepared, err := zerolog.Prepare(settings)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
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
	dependencies.Runtime, dependencies.Evidence = runtime, inbox
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error("close", err)
		}
		if !owner.ShutdownComplete() {
			t.Error("retained source")
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		_ = inbox.Seal()
		for {
			delivery, err := inbox.NextReleased(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return owner, dependencies
}
func receiptResult(t *testing.T, deps zerolog.Dependencies, receipt *adapters.Receipt[zerolog.Result], submission error) (zerolog.Result, error) {
	t.Helper()
	if submission != nil {
		t.Fatal("submission", submission)
	}
	if receipt == nil {
		t.Fatal("missing receipt")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := deps.Evidence.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := observed.Snapshot()
	if actual.Info().Sequence != snapshot.Info().Sequence {
		t.Fatal("evidence identity")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present {
		t.Fatal("missing native data", snapshot.Err())
	}
	return value, snapshot.Err()
}
func TestPublicPreparationExactSettingsBeforeEffects(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	settings := zerolog.Settings{Name: "prepare", Version: 1, QueuedCalls: ptr(2), Sinks: []zerolog.Sink{{Name: "file", Kind: "file", File: &zerolog.File{Directory: directory}}}}
	prepared, err := zerolog.Prepare(settings)
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	if metadata.QueuedCalls != 2 || metadata.ActiveCalls != 1 || metadata.MaxRecordBytes != 16<<10 || metadata.Files != 1 || metadata.DerivationBytes != 8<<20 {
		t.Fatal("wrong native envelope", metadata)
	}
	*settings.QueuedCalls = 4
	settings.Sinks[0].File.Directory = "/not-the-frozen-path"
	if prepared.Metadata() != metadata {
		t.Fatal("mutable prepared settings")
	}
	entries, _ := os.ReadDir(directory)
	if len(entries) != 0 {
		t.Fatal("inert preparation opened files")
	}
	invalid := zerolog.Settings{Name: "zero", Version: 1, MaxRecordBytes: ptr(0), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}
	if _, err := zerolog.Prepare(invalid); !errors.Is(err, zerolog.ErrInput) {
		t.Fatal("explicit zero defaulted", err)
	}
	policy, _ := prepared.Policy()
	policy.Runtime.MaxWorkBytes--
	runtime, _ := adapters.New(context.Background(), policy.Runtime)
	defer runtime.Close(context.Background())
	inbox, _ := adapters.NewInbox[zerolog.Result](policy.Evidence)
	owner, err := prepared.Open(context.Background(), zerolog.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil || !errors.Is(err, zerolog.ErrLimit) {
		t.Fatal("effects before capacity refusal", err)
	}
	entries, _ = os.ReadDir(directory)
	if len(entries) != 0 {
		t.Fatal("capacity refusal created files")
	}
}
func TestPublicSevenSeveritiesClosedDataAndBorrowedMaintenance(t *testing.T) {
	writer := &memoryWriter{}
	owner, deps := openPublic(t, zerolog.Settings{Name: "direct", Version: 1, MinLevel: ptr(zerolog.Trace), Caller: ptr(true), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"out": writer}})
	client, err := owner.Client().WithID("public-identity")
	if err != nil {
		t.Fatal(err)
	}
	original := []byte{255, 0, 1}
	var last zerolog.Result
	var callerLine int
	for _, level := range []zerolog.Level{zerolog.Trace, zerolog.Debug, zerolog.Info, zerolog.Warn, zerolog.Error, zerolog.Fatal, zerolog.Panic} {
		_, _, callerLine, _ = runtime.Caller(0)
		receipt, err := client.Log(context.Background(), level, "levels", slog.Uint64("unsigned", math.MaxUint64), zerolog.Attribute("data", logging.Group(logging.Field{Key: "null", Value: logging.Null()}, logging.Field{Key: "array", Value: logging.Array(logging.Binary(original), logging.Float32(1.2))}, logging.Field{Key: "empty", Value: logging.Group()})))
		value, outcome := receiptResult(t, deps, receipt, err)
		last = value
		if outcome != nil || len(value.SinksCopy()) != 1 || !value.SinksCopy()[0].Accepted || !value.SinksCopy()[0].Attempted || !value.SinksCopy()[0].BytesKnown || value.SinksCopy()[0].Kind != "writer" {
			t.Fatal("severity outcome", level, outcome)
		}
	}
	original[0] = 0
	lines := bytes.Split(bytes.TrimSpace(writer.data()), []byte("\n"))
	if len(lines) != 7 {
		t.Fatal("seven severity count", len(lines))
	}
	for index, line := range lines {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		var attributes map[string]json.RawMessage
		_ = json.Unmarshal(record["attributes"], &attributes)
		if string(attributes["unsigned"]) != "18446744073709551615" || string(attributes["data"]) != `{"null":null,"array":["/wAB",1.2],"empty":{}}` {
			t.Fatal("closed fidelity", string(line))
		}
		if index == 6 {
			var caller struct {
				File     string
				Line     int
				Function string
			}
			_ = json.Unmarshal(record["caller"], &caller)
			if !strings.HasSuffix(caller.File, "public_test.go") || caller.Line != callerLine+1 {
				t.Fatal("wrapper caller", caller, callerLine)
			}
			var correlation struct{ Call string }
			_ = json.Unmarshal(record["correlation"], &correlation)
			if correlation.Call != last.NativeCorrelation().Call || last.Attribution().ID != "public-identity" {
				t.Fatal("evidence correlation")
			}
		}
	}
	for _, maintenance := range []func(context.Context) (*adapters.Receipt[zerolog.Result], error){client.Sync, client.Rotate} {
		receipt, err := maintenance(context.Background())
		if !errors.Is(err, zerolog.ErrUnsupported) || receipt == nil {
			t.Fatal("borrowed maintenance invented", err)
		}
		snapshot, _ := receipt.WaitReleased(context.Background())
		if !errors.Is(snapshot.Err(), zerolog.ErrUnsupported) {
			t.Fatal("maintenance refusal evidence")
		}
		delivery, _ := deps.Evidence.NextReleased(context.Background())
		_ = delivery.Ack()
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if writer.syncs != 0 || writer.closes != 0 {
		t.Fatal("borrowed authority closed/synced")
	}
}

func TestPublicFileRotateAndOriginalTime(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	writer := &memoryWriter{}
	owner, deps := openPublic(t, zerolog.Settings{Name: "file", Version: 1, Sinks: []zerolog.Sink{{Name: "file", Kind: "file", File: &zerolog.File{Directory: directory}}, {Name: "borrowed", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"borrowed": writer}})
	receipt, err := owner.Client().LogEntry(context.Background(), zerolog.Entry{Level: zerolog.Info, Message: "zero"})
	value, outcome := receiptResult(t, deps, receipt, err)
	if outcome != nil || !value.SinksCopy()[0].Accepted {
		t.Fatal(outcome)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(writer.data(), &decoded); err != nil {
		t.Fatal(err)
	}
	if _, present := decoded["time"]; present {
		t.Fatal("absent time fabricated")
	}
	receipt, err = owner.Client().Rotate(context.Background())
	value, outcome = receiptResult(t, deps, receipt, err)
	if outcome != nil || !value.SinksCopy()[0].Rotated || value.SinksCopy()[1].Rotated || value.SinksCopy()[1].Attempted {
		t.Fatal("maintenance applicability", outcome)
	}
	if _, err := os.Stat(filepath.Join(directory, "log-00000000000000000001.jsonl")); err != nil {
		t.Fatal(err)
	}
	receipt, err = owner.Client().Sync(context.Background())
	value, outcome = receiptResult(t, deps, receipt, err)
	if outcome != nil || !value.SinksCopy()[0].Synced || value.SinksCopy()[1].Synced {
		t.Fatal("sync applicability", outcome)
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(directory, ".fathomry.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("physical marker retained", err)
	}
	if writer.syncs != 0 || writer.closes != 0 {
		t.Fatal("borrowed output maintained")
	}
}
func TestPublicSlogSafeErrorsChronologyAndCancellation(t *testing.T) {
	writer := &memoryWriter{}
	owner, deps := openPublic(t, zerolog.Settings{Name: "slog", Version: 1, Caller: ptr(true), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"out": writer}})
	handler, err := owner.Client().Slog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close(context.Background())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	slog.New(handler).With("before", 1).WithGroup("later").With("bound", 2).InfoContext(canceled, "group", "event", 3)
	delivery, err := deps.Evidence.NextReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Ack()
	var first struct {
		Attributes map[string]json.RawMessage
		Caller     struct{ Function string }
	}
	lines := bytes.Split(bytes.TrimSpace(writer.data()), []byte("\n"))
	if len(lines) != 1 || json.Unmarshal(lines[0], &first) != nil {
		t.Fatal("slog record")
	}
	if string(first.Attributes["before"]) != "1" || string(first.Attributes["later"]) != `{"bound":2,"event":3}` || !strings.HasSuffix(first.Caller.Function, "TestPublicSlogSafeErrorsChronologyAndCancellation") {
		t.Fatal("slog chronology/caller")
	}
	if receipt, err := owner.Client().Log(canceled, zerolog.Info, "not sent"); receipt != nil || err == nil {
		t.Fatal("direct cancellation suppressed incorrectly")
	}
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "logging_zerolog", BaseLocale: "en", Resources: zerolog.Resources(), Directory: "resources", Definitions: zerolog.Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	var definition failure.Definition
	for _, item := range zerolog.Definitions() {
		if item.Code == zerolog.ErrWrite {
			definition = item
		}
	}
	original, _ := failure.New(definition, failure.Location{Operation: "business"}, errors.New("PRIVATE_CAUSE"))
	emitted := framework.NewErrorLog(slog.New(handler), presenter).Emit(canceled, original)
	if !emitted.Recognized {
		t.Fatal("framework error unrecognized")
	}
	delivery, err = deps.Evidence.NextReleased(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Ack()
	if bytes.Contains(writer.data(), []byte("PRIVATE_CAUSE")) || !bytes.Contains(writer.data(), []byte("日志")) {
		t.Fatal("safe localization")
	}
	invalid := handler.WithAttrs([]slog.Attr{slog.String("duplicate", "a"), slog.String("duplicate", "b")})
	slog.New(invalid).Info("not-a-valid-subset")
	status, _ := handler.Status()
	if status.InvalidDerivations == 0 || status.Refused == 0 || status.LastError == nil {
		t.Fatal("hidden invalid derivation")
	}
	if err := handler.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
