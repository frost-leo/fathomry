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

package viper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/configsource/internal/owned"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

// File is a sensitive bootstrap DTO. Name is a non-secret slot alias; Path is
// literal/absolute. Encoding is explicitly yaml or json, never filename inference.
type File struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Encoding string `json:"encoding"`
}

// Settings is plain, deliberately serializable bootstrap data. A zero interval
// selects Capture-only; Observe requires 1 second through 5 minutes, in nanoseconds.
type Settings struct {
	Name              string        `json:"name"`
	Documents         []File        `json:"documents"`
	ReconcileInterval time.Duration `json:"reconcile_interval"`
}
type selection struct {
	owned.Guard
	settings Settings
}

// Select validates and copies without stat, open, environment reads or watchers.
// Caller input must not change concurrently during this call.
func Select(input Settings) (source.Selection, error) {
	if !owned.Label(input.Name) || len(input.Documents) < 1 || len(input.Documents) > source.MaxDocuments ||
		input.ReconcileInterval != 0 && (input.ReconcileInterval < time.Second || input.ReconcileInterval > 5*time.Minute) {
		return nil, owned.Fail(ErrSettings)
	}
	total := 0
	seen := make(map[string]bool)
	for _, file := range input.Documents {
		if !owned.Label(file.Name) || seen[file.Name] || !filepath.IsAbs(file.Path) || len(file.Path) > 4096 ||
			!utf8.ValidString(file.Path) || strings.ContainsRune(file.Path, 0) || file.Encoding != "yaml" && file.Encoding != "json" {
			return nil, owned.Fail(ErrSettings)
		}
		seen[file.Name] = true
		total += len(file.Path) + len(file.Name)
	}
	if total > native.MaxBootstrapBytes {
		return nil, owned.Acquisition(ErrLimit, source.AcquisitionInfo{Source: input.Name, Phase: source.SelectPhase})
	}
	value := Settings{Name: strings.Clone(input.Name), ReconcileInterval: input.ReconcileInterval, Documents: make([]File, len(input.Documents))}
	for index, file := range input.Documents {
		value.Documents[index] = File{Name: strings.Clone(file.Name), Path: strings.Clone(file.Path), Encoding: strings.Clone(file.Encoding)}
	}
	return &selection{settings: value}, nil
}
func (selected *selection) Description() (source.Description, error) {
	if selected == nil {
		return source.Description{}, owned.Fail(source.ErrValue)
	}
	result := source.Description{Name: selected.settings.Name, Module: ModuleID, Observable: selected.settings.ReconcileInterval != 0}
	for _, file := range selected.settings.Documents {
		result.Documents = append(result.Documents, file.Name)
	}
	return result, nil
}
func (selected *selection) Capture(ctx context.Context) (source.Batch, error) {
	if selected == nil || owned.Nil(ctx) {
		return nil, owned.Fail(source.ErrValue)
	}
	return selected.capture(ctx, source.CapturePhase)
}

func (selected *selection) capture(ctx context.Context, phase source.Phase) (source.Batch, error) {
	nativeContext := owned.NativeContext(ctx)
	remaining := source.MaxBatchBytes
	entries := make([]owned.Entry, 0, len(selected.settings.Documents))
	for _, file := range selected.settings.Documents {
		raw, missing, err := native.RawFile(nativeContext, file.Path, min(source.MaxDocumentBytes, remaining))
		if err != nil {
			return nil, mapError(err, ctx, source.AcquisitionInfo{Source: selected.settings.Name, Document: file.Name, Phase: phase})
		}
		remaining -= len(raw)
		presence := source.Present
		if missing {
			presence = source.Missing
		}
		entries = append(entries, owned.Entry{Name: file.Name, Presence: presence, Raw: raw})
	}
	if ctx.Err() != nil {
		return nil, owned.Acquisition(ErrRead, source.AcquisitionInfo{Source: selected.settings.Name, Phase: phase}, ctx.Err(), context.Cause(ctx))
	}
	return owned.NewBatch(entries)
}
func (selected *selection) Observe(ctx context.Context) (source.Observer, error) {
	if selected == nil || owned.Nil(ctx) || selected.settings.ReconcileInterval == 0 {
		return nil, owned.Fail(source.ErrValue)
	}
	return observe(ctx, selected.settings.ReconcileInterval, func(lifetime context.Context) (source.Batch, error) {
		return selected.capture(lifetime, source.ObservePhase)
	}), nil
}

func observe(ctx context.Context, interval time.Duration, capture func(context.Context) (source.Batch, error)) source.Observer {
	return owned.Observe(ctx, func(lifetime context.Context, publish func(source.Batch, error)) error {
		for lifetime.Err() == nil {
			batch, err := capture(lifetime)
			publish(batch, err)
			if lifetime.Err() != nil {
				// Publication is fenced, but an owned file's actual close failure
				// still belongs to terminal cleanup. A borrowed cause is not proof.
				if current, ok := failure.Inspect(err); ok && current.Diagnostic().Condition == ErrClose {
					return err
				}
				return nil
			}
			timer := time.NewTimer(interval)
			select {
			case <-timer.C:
			case <-lifetime.Done():
				timer.Stop()
				return nil
			}
		}
		return nil
	})
}
func mapError(err error, ctx context.Context, info source.AcquisitionInfo) error {
	condition := ErrRead
	switch {
	case errors.Is(err, native.ErrClose):
		condition = ErrClose
	case errors.Is(err, native.ErrLimit):
		condition = ErrLimit
	case errors.Is(err, native.ErrDecode):
		condition = ErrEncoding
	}
	// Filesystem cause access is deliberately sensitive; private fault graphs and
	// arbitrary parser text are not public causes.
	var pathError *os.PathError
	var causes []error
	if errors.As(err, &pathError) {
		causes = append(causes, pathError)
	}
	if errors.Is(err, context.Canceled) {
		causes = append(causes, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		causes = append(causes, context.DeadlineExceeded)
	}
	if ctx.Err() != nil {
		causes = append(causes, ctx.Err(), context.Cause(ctx))
	}
	return owned.Acquisition(condition, info, causes...)
}
