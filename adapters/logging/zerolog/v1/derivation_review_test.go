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
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

type derivationError struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	cause   error
}

func (*derivationError) Error() string { panic("derivation must not format errors") }
func (value *derivationError) Unwrap() error {
	value.once.Do(func() { close(value.entered) })
	<-value.release
	return value.cause
}

func newDerivationError(t *testing.T) (*derivationError, func()) {
	t.Helper()
	cause, err := failure.New(zerolog.Definitions()[0], failure.Location{Operation: "derivation-review"})
	if err != nil {
		t.Fatal(err)
	}
	value := &derivationError{entered: make(chan struct{}), release: make(chan struct{}), cause: cause}
	return value, sync.OnceFunc(func() { close(value.release) })
}

func waitDerivation(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("derivation did not reach cooperative error projection")
	}
}

func TestDerivationCloseJoinsPreparationAndRetainsPhysicalOwner(t *testing.T) {
	directory := ownershipDirectory(t)
	writer := new(memoryWriter)
	owner, deps := openPublic(t, zerolog.Settings{Name: "derived-close", Version: 1, MaxRecordBytes: ptr(1 << 20), Sinks: []zerolog.Sink{
		{Name: "local", Kind: "file", File: &zerolog.File{Directory: directory}},
		{Name: "borrowed", Kind: "writer"},
	}}, zerolog.Dependencies{Writers: map[string]io.Writer{"borrowed": writer}})
	view, err := owner.Client().Retain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close(context.Background())
	gate, release := newDerivationError(t)
	defer release()
	done := make(chan error, 1)
	go func() {
		_, err := view.With(slog.Any("failure", gate))
		done <- err
	}()
	waitDerivation(t, gate.entered)
	before, err := deps.Runtime.Inspect()
	if err != nil || before.Active != 2 {
		t.Fatal("missing physical/retained source roots", err, before.Active)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := view.Close(canceled); err == nil {
		t.Fatal("Close claimed completed release while preparation was entered")
	}
	after, err := deps.Runtime.Inspect()
	if err != nil || after.Active != before.Active || after.WorkBytes != before.WorkBytes {
		t.Fatal("Close released family reservation before preparation ended", err)
	}
	if err := owner.Close(canceled); err == nil || owner.ShutdownComplete() {
		t.Fatal("source released a still-preparing family", err)
	}
	if _, err := os.Stat(filepath.Join(directory, ".fathomry.lock")); err != nil {
		t.Fatal("physical marker released before preparation completed", err)
	}
	payload := zerolog.Attribute("payload", logging.String(strings.Repeat("x", (1<<20)-1024)))
	allocated := measureAggregateAllocations(func() {
		for range 16 {
			if derived, err := view.With(payload); derived != nil || !errors.Is(err, zerolog.ErrState) {
				t.Fatal("closed family accepted derivation", err)
			}
		}
	})
	if allocated > 1<<20 {
		t.Fatalf("closed family copied payload before refusal: %d bytes", allocated)
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, zerolog.ErrState) {
			t.Fatal("closing preparation published a successful derived view", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("released preparation did not finish")
	}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := view.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		t.Fatal("actual source cleanup did not complete", err)
	}
	if _, err := os.Stat(filepath.Join(directory, ".fathomry.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("completed cleanup retained marker", err)
	}
	if writer.syncs != 0 || writer.closes != 0 {
		t.Fatal("preparation cleanup maintained a borrowed writer")
	}
}

func TestDerivationPreparationUsesExistingFamilyAllowance(t *testing.T) {
	owner, _ := openPublic(t, zerolog.Settings{Name: "derived-admission", Version: 1, MaxRecordBytes: ptr(1 << 20), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"out": io.Discard}})
	view, err := owner.Client().Retain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close(context.Background())
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	done := make(chan error, 4)
	for range 3 {
		gate, release := newDerivationError(t)
		releases = append(releases, release)
		go func() { _, err := view.With(slog.Any("failure", gate)); done <- err }()
		waitDerivation(t, gate.entered)
	}
	overflow, release := newDerivationError(t)
	releases = append(releases, release)
	go func() { _, err := view.With(slog.Any("failure", overflow)); done <- err }()
	select {
	case <-overflow.entered:
		t.Fatal("fourth 2*ViewBytes preparation exceeded existing 8 MiB family storage")
	case err := <-done:
		if !errors.Is(err, zerolog.ErrLimit) {
			t.Fatal("temporary derivation allowance refusal is not observable", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("saturated derivation did not refuse")
	}
	for _, release := range releases {
		release()
	}
	for range 3 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal("admitted independent preparation failed", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("admitted preparation did not complete")
		}
	}
	if _, err := view.With(slog.Int("next", 1)); err != nil {
		t.Fatal("temporary refusal damaged future valid derivation", err)
	}
}

func TestDerivationFailedPreparationRefundsAllowance(t *testing.T) {
	owner, _ := openPublic(t, zerolog.Settings{Name: "derived-refund", Version: 1, MaxRecordBytes: ptr(1 << 20), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"out": io.Discard}})
	view, err := owner.Client().Retain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close(context.Background())
	for range 160 {
		if derived, err := view.With(slog.Any("unsupported", struct{}{})); derived != nil || !errors.Is(err, zerolog.ErrUnsupported) {
			t.Fatal("unsupported preparation changed state or consumed lifetime allowance", err)
		}
	}
	if _, err := view.With(slog.String("after", "refusal")); err != nil {
		t.Fatal("failed preparation leaked temporary storage or view count", err)
	}
}

type interruptedDerivationError struct{ goexit bool }

func (*interruptedDerivationError) Error() string { panic("unexpected formatter invocation") }
func (value *interruptedDerivationError) Unwrap() error {
	if value.goexit {
		runtime.Goexit()
	}
	panic("derivation-interruption")
}

func TestDerivationExceptionalExitStillReleasesReservation(t *testing.T) {
	for _, exit := range []string{"panic", "goexit"} {
		t.Run(exit, func(t *testing.T) {
			owner, deps := openPublic(t, zerolog.Settings{Name: "derived-exit", Version: 1, MaxRecordBytes: ptr(1 << 20), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"out": io.Discard}})
			view, err := owner.Client().Retain(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := view.Close(ctx); err != nil {
					t.Error("exceptional preparation retained family", err)
				}
			}()
			type result struct {
				returned  bool
				recovered any
			}
			for range 16 {
				done := make(chan result, 1)
				go func() {
					returned := false
					defer func() { done <- result{returned: returned, recovered: recover()} }()
					_, _ = view.With(slog.Any("failure", &interruptedDerivationError{goexit: exit == "goexit"}))
					returned = true
				}()
				select {
				case outcome := <-done:
					if outcome.returned || exit == "panic" && outcome.recovered != "derivation-interruption" || exit == "goexit" && outcome.recovered != nil {
						t.Fatal("exceptional projection swallowed the exit or exhausted its reservation")
					}
				case <-time.After(3 * time.Second):
					t.Fatal("exceptional preparation did not terminate")
				}
			}
			if _, err := view.With(slog.Int("after", 1)); err != nil {
				t.Fatal("exceptional projection leaked temporary reservation", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := view.Close(ctx); err != nil {
				t.Fatal("exceptional projection retained the root guard", err)
			}
			if stats, err := deps.Runtime.Inspect(); err != nil || stats.Active != 1 {
				t.Fatal("closed derivation family still owns Runtime capacity", err)
			}
		})
	}
}
