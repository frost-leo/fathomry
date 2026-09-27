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

package project

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

func TestCreationConditionAndUnconfirmedClose(t *testing.T) {
	input := inputFor(t)
	plan, err := prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	closeCause := errors.New("close evidence")
	calls, closes := 0, 0
	result := plan.write(context.Background(), func(path string) (io.WriteCloser, error) {
		file, err := exclusiveFile(path)
		if err != nil {
			return nil, err
		}
		calls++
		last := calls == len(plan.files)
		return faultyFile{WriteCloser: file, close: func() error {
			closes++
			if last {
				return closeCause
			}
			return nil
		}}, nil
	})
	observed, err := result.observation, result.err
	current, ok := failure.Inspect(err)
	if !ok || current.Diagnostic().Condition != ErrCreation || !errors.Is(err, closeCause) || observed != partial || calls != 4 || closes != 4 {
		t.Fatal("close evidence changed", observed, err, calls, closes)
	}
	assertComplete(t, plan)
	if key, err := result.presentationKey(); err != nil || key != "creation_incomplete" {
		t.Fatal("present bytes falsely certified successful closes")
	}
}

func TestConditionBindingDoesNotSelectDescendants(t *testing.T) {
	exists := failed(ErrExists)
	source := failed(ErrSource, exists)
	for _, test := range []struct {
		err error
		key string
	}{
		{source, "source_unusable"}, {exists, "destination_exists"},
		{errors.Join(source, exists), "notStarted"}, {errors.Join(exists, source), "notStarted"},
		{fmt.Errorf("wrapper: %w", source), "notStarted"}, {ErrSource, "notStarted"},
	} {
		if got, err := (creationResult{observation: untouched, err: test.err}).presentationKey(); err != nil || got != test.key {
			t.Fatal("inferred another occurrence", got, test.key)
		}
		for _, state := range []effect{partial, complete} {
			wanted := "partial"
			if state == complete {
				wanted = "completeDelivery"
			}
			if key, err := (creationResult{observation: state, err: test.err}).presentationKey(); err != nil || key != wanted {
				t.Fatal("condition replaced operation observation", key, err)
			}
		}
	}
	if combine(source) != source {
		t.Fatal("singleton identity lost")
	}
	if _, ok := failure.Inspect(combine(source, exists)); ok {
		t.Fatal("aggregate acquired primary")
	}
}

func TestUnknownCreationProjection(t *testing.T) {
	for _, result := range []creationResult{
		{}, {err: failed(ErrPreparation)}, {observation: effect(255), err: failed(ErrCreation)},
		{observation: untouched}, {observation: partial},
	} {
		if key, err := result.presentationKey(); key != "" || !errors.Is(err, ErrPresentation) {
			t.Fatal("invalid observation falsely claimed creation state", key, err)
		}
	}
	if key, err := (creationResult{observation: complete}).presentationKey(); err != nil || key != "created" {
		t.Fatal("completed success rejected", key, err)
	}
}
