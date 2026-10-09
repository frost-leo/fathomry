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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func ownershipPointer[T any](value T) *T { return &value }

func ownershipWait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("ownership checkpoint did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}

func ownershipContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 3*time.Second)
}

func ownershipDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func ownershipSettings(directory, minimum string, records bool, queued int) zerolog.Settings {
	value := zerolog.Settings{Name: "ownership", Version: 1, MinLevel: ownershipPointer(zerolog.Level(minimum)), QueuedCalls: ownershipPointer(queued)}
	if directory != "" {
		value.Sinks = append(value.Sinks, zerolog.Sink{Name: "local", Kind: "file", MinLevel: ownershipPointer(zerolog.Level(minimum)), File: &zerolog.File{Directory: directory, MaxBytes: ownershipPointer(int64(1 << 20)), Backups: ownershipPointer(3)}})
	}
	if records {
		value.Sinks = append(value.Sinks, zerolog.Sink{Name: "borrowed", Kind: "record", MinLevel: ownershipPointer(zerolog.Level(minimum))})
	}
	return value
}

func ownershipPrepare(t *testing.T, value zerolog.Settings) zerolog.Prepared {
	t.Helper()
	prepared, err := zerolog.Prepare(value)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

type ownershipEnvironment struct {
	runtime  *adapters.Runtime
	inbox    *adapters.Inbox[zerolog.Result]
	receiver *framework.Receiver[zerolog.Result]
	deps     zerolog.Dependencies
	policy   zerolog.Policy
	mu       sync.Mutex
	owners   []*zerolog.Owner
	seen     map[uint64]adapters.Snapshot[zerolog.Result]
}

func newOwnershipEnvironment(t *testing.T, sink zerolog.RecordWriter, preparations ...zerolog.Prepared) *ownershipEnvironment {
	t.Helper()
	policy, err := zerolog.Compose(preparations...)
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
	environment := &ownershipEnvironment{runtime: runtime, inbox: inbox, policy: policy,
		deps: zerolog.Dependencies{Runtime: runtime, Evidence: inbox}, seen: make(map[uint64]adapters.Snapshot[zerolog.Result])}
	if sink != nil {
		environment.deps.Records = map[string]zerolog.RecordWriter{"borrowed": sink}
	}
	environment.receiver, err = framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, value adapters.Snapshot[zerolog.Result]) error {
		environment.mu.Lock()
		defer environment.mu.Unlock()
		if _, exists := environment.seen[value.Info().Sequence]; exists {
			return errors.New("duplicate test-owned evidence")
		}
		environment.seen[value.Info().Sequence] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := ownershipContext(t)
		defer cancel()
		environment.mu.Lock()
		owners := append([]*zerolog.Owner(nil), environment.owners...)
		environment.mu.Unlock()
		for index := len(owners) - 1; index >= 0; index-- {
			if err := owners[index].Close(ctx); err != nil {
				t.Error("policy cleanup", err)
			}
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error("runtime cleanup", err)
		}
		if err := environment.receiver.Finish(ctx); err != nil {
			t.Error("evidence finish", err)
			_ = environment.receiver.Close(ctx)
		}
		if status, err := inbox.Inspect(); err != nil || status.Outstanding != 0 {
			t.Error("independent custody leaked", err)
		}
	})
	return environment
}

func (environment *ownershipEnvironment) keep(owner *zerolog.Owner) {
	if owner != nil {
		environment.mu.Lock()
		environment.owners = append(environment.owners, owner)
		environment.mu.Unlock()
	}
}

func (environment *ownershipEnvironment) open(t *testing.T, prepared zerolog.Prepared) *zerolog.Owner {
	t.Helper()
	owner, err := prepared.Open(context.Background(), environment.deps)
	environment.keep(owner)
	if err != nil || owner == nil {
		t.Fatal("open", err)
	}
	return owner
}

func (environment *ownershipEnvironment) observed(t *testing.T, receipt *adapters.Receipt[zerolog.Result]) adapters.Snapshot[zerolog.Result] {
	t.Helper()
	if receipt == nil {
		t.Fatal("expected accepted public evidence")
	}
	ctx, cancel := ownershipContext(t)
	defer cancel()
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal("native work did not release", err)
	}
	var independent adapters.Snapshot[zerolog.Result]
	ownershipWait(t, func() bool {
		environment.mu.Lock()
		defer environment.mu.Unlock()
		var found bool
		independent, found = environment.seen[result.Info().Sequence]
		return found
	})
	if independent.Info() != result.Info() || (independent.Err() == nil) != (result.Err() == nil) {
		t.Fatal("direct and independent operation identities/outcomes differ")
	}
	return result
}

func (environment *ownershipEnvironment) log(t *testing.T, client *zerolog.Client, severity zerolog.Level, message string) zerolog.Result {
	t.Helper()
	receipt, err := client.Log(context.Background(), severity, message)
	if err != nil {
		t.Fatal(err)
	}
	result := environment.observed(t, receipt)
	value, present := result.ValueCopy()
	if !present || result.Err() != nil {
		t.Fatal("log outcome", result.Err())
	}
	return value
}

func ownershipRows(t *testing.T, directory string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(directory, "current.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var result []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}

func (environment *ownershipEnvironment) locked(t *testing.T, prepared zerolog.Prepared) {
	t.Helper()
	owner, err := prepared.Open(context.Background(), environment.deps)
	environment.keep(owner)
	if err == nil {
		t.Fatal("competing physical owner acquired a live directory")
	}
	if owner != nil {
		ctx, cancel := ownershipContext(t)
		defer cancel()
		if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
			t.Fatal("failed competing candidate retained resources", err)
		}
	}
}

func TestOwnershipLevelAdoptionKeepsOnePhysicalFile(t *testing.T) {
	directory := ownershipDirectory(t)
	oldPrepared := ownershipPrepare(t, ownershipSettings(directory, "error", false, 0))
	newPrepared := ownershipPrepare(t, ownershipSettings(directory, "debug", false, 0))
	environment := newOwnershipEnvironment(t, nil, oldPrepared)
	old := environment.open(t, oldPrepared)
	next, err := newPrepared.Adopt(context.Background(), old.Handle())
	environment.keep(next)
	if err != nil || next == nil {
		t.Fatal("same-directory level adoption", err)
	}
	if old.Info().Revision == "" || old.Info().Revision != next.Info().Revision {
		t.Fatal("level-only adoption replaced physical source identity")
	}
	if stats, err := environment.runtime.Inspect(); err != nil || stats.Active != 1 {
		t.Fatal("logical policy minted another physical root", err)
	}
	before := environment.log(t, old.Client(), zerolog.Info, "old-filtered")
	after := environment.log(t, next.Client(), zerolog.Info, "new-visible")
	if !before.SinksCopy()[0].Filtered || !after.SinksCopy()[0].Accepted ||
		before.PolicyRevision() == after.PolicyRevision() || before.Source().Revision != after.Source().Revision {
		t.Fatal("old/new frozen policy and physical identity conflated")
	}
	if rows := ownershipRows(t, directory); len(rows) != 1 || rows[0]["message"] != "new-visible" {
		t.Fatal("independently decoded local output disagrees with policy effects")
	}
	ctx, cancel := ownershipContext(t)
	defer cancel()
	if err := old.Close(ctx); err != nil {
		t.Fatal(err)
	}
	environment.locked(t, newPrepared)
	environment.log(t, next.Client(), zerolog.Debug, "after-old-close")
	invalid := ownershipSettings(directory, "debug", false, 0)
	invalid.Sinks[0].File.Compress = ownershipPointer(true)
	bad := ownershipPrepare(t, invalid)
	if candidate, err := bad.Adopt(context.Background(), next.Handle()); candidate != nil || !errors.Is(err, zerolog.ErrUnsupported) {
		t.Fatal("non-level physical change admitted", err)
	}
	environment.log(t, next.Client(), zerolog.Debug, "last-good")
	for index := range 20 {
		prepared := oldPrepared
		if index%2 == 0 {
			prepared = newPrepared
		}
		adopted, err := prepared.Adopt(context.Background(), next.Handle())
		environment.keep(adopted)
		if err != nil {
			t.Fatal("live policy bound became a cumulative update limit", err)
		}
		if err := next.Close(ctx); err != nil {
			t.Fatal(err)
		}
		next = adopted
	}
	if err := next.Close(ctx); err != nil || !next.ShutdownComplete() {
		t.Fatal("final physical cleanup", err)
	}
	reopened := environment.open(t, newPrepared)
	environment.log(t, reopened.Client(), zerolog.Debug, "reopened")
}

type ownershipBlockingSink struct {
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
	active     atomic.Int64
	entries    atomic.Int64
	syncs      atomic.Int64
	closes     atomic.Int64
	violations atomic.Int64
}

func (sink *ownershipBlockingSink) WriteRecord(context.Context, zerolog.Record) error {
	if sink.active.Add(1) != 1 {
		sink.violations.Add(1)
	}
	defer sink.active.Add(-1)
	sink.entries.Add(1)
	sink.once.Do(func() { close(sink.entered) })
	<-sink.release
	return nil
}

func (sink *ownershipBlockingSink) Sync(context.Context) error {
	if sink.active.Load() != 0 {
		sink.violations.Add(1)
	}
	sink.syncs.Add(1)
	return nil
}

func (sink *ownershipBlockingSink) Close() error { sink.closes.Add(1); return nil }

func TestOwnershipAdoptedPoliciesShareNativeQueueSyncAndRotate(t *testing.T) {
	for _, queue := range []int{0, 1} {
		t.Run(fmt.Sprintf("queue-%d", queue), func(t *testing.T) {
			sink := &ownershipBlockingSink{entered: make(chan struct{}), release: make(chan struct{})}
			directory := ownershipDirectory(t)
			oldPrepared := ownershipPrepare(t, ownershipSettings(directory, "info", true, queue))
			newSettings := ownershipSettings(directory, "debug", true, queue)
			newPrepared := ownershipPrepare(t, newSettings)
			environment := newOwnershipEnvironment(t, sink, oldPrepared, oldPrepared)
			old := environment.open(t, oldPrepared)
			next, err := newPrepared.Adopt(context.Background(), old.Handle())
			environment.keep(next)
			if err != nil {
				t.Fatal(err)
			}
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(sink.release) }) })
			type completion struct {
				receipt *adapters.Receipt[zerolog.Result]
				err     error
			}
			first := make(chan completion, 1)
			go func() {
				receipt, err := old.Client().Log(context.Background(), zerolog.Info, "held")
				first <- completion{receipt, err}
			}()
			<-sink.entered
			remaining := make(chan completion, 6)
			for index := range 6 {
				go func() {
					client := old.Client()
					if index%2 == 1 {
						client = next.Client()
					}
					var receipt *adapters.Receipt[zerolog.Result]
					var err error
					if index < 2 {
						receipt, err = client.Log(context.Background(), zerolog.Info, "waiting")
					} else if index < 4 {
						receipt, err = client.Sync(context.Background())
					} else {
						receipt, err = client.Rotate(context.Background())
					}
					remaining <- completion{receipt, err}
				}()
			}
			ctx, cancel := ownershipContext(t)
			defer cancel()
			for range 6 - queue {
				select {
				case result := <-remaining:
					observed := environment.observed(t, result.receipt)
					if !errors.Is(observed.Err(), adapters.ErrLimit) || !errors.Is(result.err, adapters.ErrLimit) {
						t.Fatal("policy obtained an extra native write/maintenance allowance", result.err, observed.Err())
					}
				case <-ctx.Done():
					t.Fatal("excess policy operations waited outside the declared physical queue")
				}
			}
			if sink.entries.Load() != 1 || sink.syncs.Load() != 0 || sink.violations.Load() != 0 {
				t.Fatal("native write/maintenance paths overlapped across views")
			}
			releaseOnce.Do(func() { close(sink.release) })
			result := <-first
			if result.err != nil || environment.observed(t, result.receipt).Err() != nil {
				t.Fatal("original native operation did not complete", result.err)
			}
			if queue == 1 {
				result = <-remaining
				if result.err != nil || environment.observed(t, result.receipt).Err() != nil {
					t.Fatal("one admitted waiter failed", result.err)
				}
			}
			if sink.violations.Load() != 0 {
				t.Fatal("shared physical admission was bypassed")
			}
		})
	}
}

func TestOwnershipRetainedViewKeepsFollowGenerationAndLastGood(t *testing.T) {
	firstDirectory, secondDirectory := ownershipDirectory(t), ownershipDirectory(t)
	first := ownershipPrepare(t, ownershipSettings(firstDirectory, "error", false, 0))
	second := ownershipPrepare(t, ownershipSettings(firstDirectory, "debug", false, 0))
	different := ownershipPrepare(t, ownershipSettings(secondDirectory, "debug", false, 0))
	invalidSettings := ownershipSettings(firstDirectory, "debug", false, 0)
	invalidSettings.Caller = ownershipPointer(true)
	invalid := ownershipPrepare(t, invalidSettings)
	environment := newOwnershipEnvironment(t, nil, first, different)
	scope, err := resource.New(context.Background(), resource.Options{Name: "ownership"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := ownershipContext(t)
		defer cancel()
		if err := scope.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	var mu sync.Mutex
	var current *zerolog.Owner
	owners := make(map[int]*zerolog.Owner)
	ref, err := resource.Bind(scope, resource.Binding[int, zerolog.Handle]{Name: "following", Policy: resource.Follow, MaxGenerations: 3,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return snapshot.ValueCopy()
		}, Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
		Build: func(ctx context.Context, revision int) (*resource.Instance[zerolog.Handle], error) {
			var owner *zerolog.Owner
			var err error
			mu.Lock()
			previous := current
			mu.Unlock()
			switch revision {
			case 0:
				owner, err = first.Open(ctx, environment.deps)
			case 1:
				owner, err = second.Adopt(ctx, previous.Handle())
			case 2:
				owner, err = invalid.Adopt(ctx, previous.Handle())
			case 3:
				owner, err = different.Open(ctx, environment.deps)
			}
			environment.keep(owner)
			if owner == nil {
				return nil, err
			}
			if err == nil {
				mu.Lock()
				current, owners[revision] = owner, owner
				mu.Unlock()
			}
			return &resource.Instance[zerolog.Handle]{Value: owner.Handle(), Release: owner.Release}, err
		}})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(value int) error {
		snapshot, err := settings.New(value, func(value int) int { return value })
		if err != nil {
			return err
		}
		ctx, cancel := ownershipContext(t)
		defer cancel()
		update, err := scope.Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err := apply(0); err != nil {
		t.Fatal(err)
	}
	before, _ := ref.Inspect()
	client, err := zerolog.Using(context.Background(), ref, environment.policy.Budget, environment.deps)
	if err != nil {
		t.Fatal(err)
	}
	held, err := client.With(context.Background(), slog.String("retained", "old"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := ownershipContext(t)
		defer cancel()
		if err := held.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := apply(1); err != nil {
		t.Fatal(err)
	}
	after, _ := ref.Inspect()
	if before.Generation == after.Generation || after.Retiring != 1 {
		t.Fatal("held view did not retain its original generation")
	}
	receipt, err := held.Log(context.Background(), zerolog.Info, "old-filtered")
	if err != nil {
		t.Fatal(err)
	}
	oldResult, present := environment.observed(t, receipt).ValueCopy()
	if !present || !oldResult.SinksCopy()[0].Filtered || oldResult.Attribution().Source.Generation != before.Generation {
		t.Fatal("retained view followed a replacement")
	}
	newResult := environment.log(t, client, zerolog.Info, "new-visible")
	if newResult.Attribution().Source.Generation != after.Generation || newResult.Source().Revision != oldResult.Source().Revision || newResult.PolicyRevision() == oldResult.PolicyRevision() {
		t.Fatal("logical/physical generation attribution changed")
	}
	if err := apply(2); !errors.Is(err, zerolog.ErrUnsupported) {
		t.Fatal("invalid non-level candidate did not reject", err)
	}
	lastGood, _ := ref.Inspect()
	if lastGood.Generation != after.Generation {
		t.Fatal("failed candidate replaced last-good generation")
	}
	environment.log(t, client, zerolog.Info, "last-good")
	if err := apply(3); err != nil {
		t.Fatal("different-directory replacement failed", err)
	}
	latest := environment.log(t, client, zerolog.Debug, "other-directory")
	if latest.Source().Revision == newResult.Source().Revision {
		t.Fatal("different directory reused physical source identity")
	}
	mu.Lock()
	oldOwner := owners[0]
	mu.Unlock()
	if oldOwner.ShutdownComplete() {
		t.Fatal("retained old family lost its physical source")
	}
	ctx, cancel := ownershipContext(t)
	defer cancel()
	if err := held.Close(ctx); err != nil {
		t.Fatal(err)
	}
	ownershipWait(t, oldOwner.ShutdownComplete)
	if rows := ownershipRows(t, firstDirectory); len(rows) != 2 || rows[0]["message"] != "new-visible" || rows[1]["message"] != "last-good" {
		t.Fatal("old directory output was retargeted")
	}
	if rows := ownershipRows(t, secondDirectory); len(rows) != 1 || rows[0]["message"] != "other-directory" {
		t.Fatal("new directory output missing")
	}
}

func TestOwnershipCanceledViewAndOwnerCloseWaitForNativeWork(t *testing.T) {
	directory := ownershipDirectory(t)
	sink := &ownershipBlockingSink{entered: make(chan struct{}), release: make(chan struct{})}
	prepared := ownershipPrepare(t, ownershipSettings(directory, "info", true, 0))
	environment := newOwnershipEnvironment(t, sink, prepared)
	owner := environment.open(t, prepared)
	view, err := owner.Client().With(context.Background(), slog.String("retained", "value"))
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(sink.release) }) })
	type completion struct {
		receipt *adapters.Receipt[zerolog.Result]
		err     error
	}
	done := make(chan completion, 1)
	go func() {
		receipt, err := view.Log(context.Background(), zerolog.Info, "entered")
		done <- completion{receipt, err}
	}()
	<-sink.entered
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if result := view.Release(canceled); result.Complete || !errors.Is(result.Err, context.Canceled) {
		t.Fatal("canceled family wait replaced actual native completion", result.Err)
	}
	if err := owner.Close(canceled); !errors.Is(err, context.Canceled) || owner.ShutdownComplete() {
		t.Fatal("canceled owner wait released physical resources", err)
	}
	environment.locked(t, prepared)
	if sink.syncs.Load() != 0 || sink.violations.Load() != 0 {
		t.Fatal("cleanup ran before entered native work returned")
	}
	releaseOnce.Do(func() { close(sink.release) })
	result := <-done
	if result.err != nil {
		t.Fatal("entered write lost its completion", result.err)
	}
	observation := environment.observed(t, result.receipt)
	value, present := observation.ValueCopy()
	if !present || len(value.SinksCopy()) != 2 || !value.SinksCopy()[0].Accepted || !value.SinksCopy()[1].Accepted {
		t.Fatal("entered partial/full effects were rewritten by close")
	}
	ctx, stop := ownershipContext(t)
	defer stop()
	if result := view.Release(ctx); result.Err != nil || !result.Complete {
		t.Fatal("retained family did not finish", result.Err)
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() || sink.violations.Load() != 0 || sink.syncs.Load() != 0 || sink.closes.Load() != 0 {
		t.Fatal("physical cleanup borrowed ownership or did not join native return", err)
	}
	environment.open(t, prepared)
}

func TestOwnershipLivePolicyAndFamilyLimitsRemainPhysical(t *testing.T) {
	released := make(chan struct{})
	close(released)
	sink := &ownershipBlockingSink{entered: make(chan struct{}), release: released}
	prepared := ownershipPrepare(t, ownershipSettings("", "info", true, 0))
	// Spare public capacity proves the physical limits are not merely an
	// incidental refusal by the enclosing shared operation runtime.
	environment := newOwnershipEnvironment(t, sink, prepared, prepared)
	first := environment.open(t, prepared)
	owners := []*zerolog.Owner{first}
	for range 15 {
		owner, err := prepared.Adopt(context.Background(), first.Handle())
		environment.keep(owner)
		if err != nil {
			t.Fatal("valid live policy refused", err)
		}
		owners = append(owners, owner)
	}
	if owner, err := prepared.Adopt(context.Background(), first.Handle()); owner != nil || !errors.Is(err, zerolog.ErrLimit) {
		t.Fatal("more than sixteen policies borrowed one physical source", err)
	}
	ctx, cancel := ownershipContext(t)
	defer cancel()
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	replacement, err := prepared.Adopt(context.Background(), owners[1].Handle())
	environment.keep(replacement)
	if err != nil {
		t.Fatal("released policy capacity did not become reusable", err)
	}
	var views []*zerolog.View
	for index := range 4 {
		view, err := owners[index+1].Client().Retain(context.Background())
		if err != nil {
			t.Fatal("valid retained family refused", err)
		}
		views = append(views, view)
		t.Cleanup(func() {
			ctx, cancel := ownershipContext(t)
			defer cancel()
			if err := view.Close(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	if view, err := replacement.Client().Retain(context.Background()); view != nil || !errors.Is(err, zerolog.ErrLimit) {
		t.Fatal("logical policy minted another physical retained-family quota", err)
	}
	if err := views[0].Close(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := replacement.Client().Retain(context.Background())
	if err != nil {
		t.Fatal("released family capacity did not become reusable", err)
	}
	if err := view.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type ownershipWriter struct {
	bytes.Buffer
	syncs, closes int
}

func (writer *ownershipWriter) Sync() error  { writer.syncs++; return nil }
func (writer *ownershipWriter) Close() error { writer.closes++; return nil }

func TestOwnershipSourceAndSinkThresholdsAreIndependentlyFrozen(t *testing.T) {
	released := make(chan struct{})
	close(released)
	records := &ownershipBlockingSink{entered: make(chan struct{}), release: released}
	low, high := &ownershipWriter{}, &ownershipWriter{}
	settings := zerolog.Settings{Name: "thresholds", Version: 1, MinLevel: ownershipPointer(zerolog.Trace), Sinks: []zerolog.Sink{
		{Name: "low", Kind: "writer", MinLevel: ownershipPointer(zerolog.Trace)},
		{Name: "high", Kind: "writer", MinLevel: ownershipPointer(zerolog.Error)},
		{Name: "borrowed", Kind: "record", MinLevel: ownershipPointer(zerolog.Warn)},
	}}
	prepared := ownershipPrepare(t, settings)
	environment := newOwnershipEnvironment(t, records, prepared)
	environment.deps.Writers = map[string]io.Writer{"low": low, "high": high}
	old := environment.open(t, prepared)
	settings.Sinks[0].MinLevel, settings.Sinks[1].MinLevel, settings.Sinks[2].MinLevel = ownershipPointer(zerolog.Error), ownershipPointer(zerolog.Info), ownershipPointer(zerolog.Info)
	changed := ownershipPrepare(t, settings)
	next, err := changed.Adopt(context.Background(), old.Handle())
	environment.keep(next)
	if err != nil {
		t.Fatal(err)
	}
	oldResult := environment.log(t, old.Client(), zerolog.Info, "old-levels").SinksCopy()
	newResult := environment.log(t, next.Client(), zerolog.Info, "new-levels").SinksCopy()
	if !oldResult[0].Accepted || !oldResult[1].Filtered || !oldResult[2].Filtered ||
		!newResult[0].Filtered || !newResult[1].Accepted || !newResult[2].Accepted {
		t.Fatal("per-sink policy changes mutated old thresholds or skipped source-independent filtering")
	}
	settings.MinLevel = ownershipPointer(zerolog.Panic)
	highSource := ownershipPrepare(t, settings)
	last, err := highSource.Adopt(context.Background(), next.Handle())
	environment.keep(last)
	if err != nil {
		t.Fatal(err)
	}
	for _, sink := range environment.log(t, last.Client(), zerolog.Fatal, "source-filtered").SinksCopy() {
		if !sink.Filtered || sink.Attempted || sink.Accepted {
			t.Fatal("source threshold was not applied before every sink")
		}
	}
	for _, sink := range environment.log(t, last.Client(), zerolog.Panic, "severity-only").SinksCopy() {
		if !sink.Accepted {
			t.Fatal("Panic severity failed to return ordinary accepted evidence")
		}
	}
	ctx, cancel := ownershipContext(t)
	defer cancel()
	for _, owner := range []*zerolog.Owner{old, next, last} {
		if err := owner.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if low.syncs != 0 || low.closes != 0 || high.syncs != 0 || high.closes != 0 || records.syncs.Load() != 0 || records.closes.Load() != 0 {
		t.Fatal("physical cleanup took ownership of borrowed byte/record outputs")
	}
	if low.Len() == 0 || high.Len() == 0 || bytes.Equal(low.Bytes(), high.Bytes()) {
		t.Fatal("independent byte outputs did not preserve distinct filtering")
	}
}
