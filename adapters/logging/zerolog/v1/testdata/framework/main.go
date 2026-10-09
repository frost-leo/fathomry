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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("zerolog framework public consumer passed")
}

func ptr[T any](value T) *T { return &value }

type generation struct {
	owner *zerolog.Owner
	done  chan struct{}
	once  sync.Once
}

func (generation *generation) release(ctx context.Context) resource.ReleaseResult {
	result := generation.owner.Release(ctx)
	if result.Complete {
		generation.once.Do(func() { close(generation.done) })
	}
	return result
}

func (generation *generation) wait(ctx context.Context) error {
	select {
	case <-generation.done:
		if !generation.owner.ShutdownComplete() {
			return errors.New("resource release did not join actual logical ownership")
		}
		return nil
	case <-ctx.Done():
		return errors.New("retired resource did not release after its last borrower")
	}
}

func markerState(directory string, owned bool) error {
	info, err := os.Stat(filepath.Join(directory, ".fathomry.lock"))
	if !owned && errors.Is(err, os.ErrNotExist) {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			path, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if path == directory || strings.HasPrefix(path, directory+string(filepath.Separator)) {
				return errors.New("closed physical owner retained a file or directory descriptor")
			}
		}
		return nil
	}
	if err != nil || !owned || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("exclusive ownership marker state was not preserved")
	}
	return nil
}

func await(ctx context.Context, receipt *adapters.Receipt[zerolog.Result], err error, filtered bool) (zerolog.Result, error) {
	if err != nil {
		return zerolog.Result{}, err
	}
	if receipt == nil {
		return zerolog.Result{}, errors.New("accepted logging operation has no receipt")
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return zerolog.Result{}, err
	}
	if err := snapshot.Err(); err != nil {
		return zerolog.Result{}, err
	}
	value, present := snapshot.ValueCopy()
	if !present || len(value.SinksCopy()) != 1 || value.SinksCopy()[0].Filtered != filtered || value.SinksCopy()[0].Accepted == filtered || value.SinksCopy()[0].Stopped {
		return zerolog.Result{}, errors.New("logical threshold changed per-destination facts")
	}
	return value, nil
}

func run(ctx context.Context) (result error) {
	base, err := os.MkdirTemp("", "fathomry-zerolog-framework-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(base)
	fixedDirectory, oldDirectory, newDirectory := filepath.Join(base, "fixed"), filepath.Join(base, "old"), filepath.Join(base, "new")
	for _, directory := range []string{fixedDirectory, oldDirectory, newDirectory} {
		if err := os.Mkdir(directory, 0700); err != nil {
			return err
		}
	}
	prepare := func(name, directory, minimum string) (zerolog.Prepared, error) {
		return zerolog.Prepare(zerolog.Settings{Name: name, Version: 1, MinLevel: ptr(zerolog.Trace), Sinks: []zerolog.Sink{{Name: "file", Kind: "file", File: &zerolog.File{Directory: directory}, MinLevel: ptr(zerolog.Level(minimum))}}})
	}
	fixedPreparation, err := prepare("fixed", fixedDirectory, "info")
	if err != nil {
		return err
	}
	first, err := prepare("follow", oldDirectory, "warn")
	if err != nil {
		return err
	}
	levelOnly, err := prepare("follow", oldDirectory, "debug")
	if err != nil {
		return err
	}
	different, err := prepare("follow", newDirectory, "info")
	if err != nil {
		return err
	}
	policy, err := zerolog.Compose(fixedPreparation, first, different)
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
	inbox, err := adapters.NewInbox[zerolog.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := zerolog.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	var mu sync.Mutex
	seen := make(map[uint64]bool)
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, snapshot adapters.Snapshot[zerolog.Result]) error {
		mu.Lock()
		defer mu.Unlock()
		if seen[snapshot.Info().Sequence] || !snapshot.Info().Released || snapshot.Err() != nil {
			return errors.New("independent custody lost operation identity/release/outcome")
		}
		seen[snapshot.Info().Sequence] = true
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
	sources := map[string][]*generation{}
	bind := func(name string, mode resource.Policy) (resource.Ref[zerolog.Handle], error) {
		var ref resource.Ref[zerolog.Handle]
		var err error
		ref, err = resource.Bind(runtime.Resources(), resource.Binding[int, zerolog.Handle]{Name: name, Policy: mode,
			Select: func(view settings.View) (int, error) {
				value, err := settings.As[int](view)
				if err != nil {
					return 0, err
				}
				return value.ValueCopy()
			},
			Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
			Build: func(lifetime context.Context, revision int) (*resource.Instance[zerolog.Handle], error) {
				selected := first
				if name == "fixed" {
					selected = fixedPreparation
				} else if revision == 1 {
					selected = levelOnly
				} else if revision == 2 {
					selected = different
				}
				var owner *zerolog.Owner
				var setup error
				previous, acquire := ref.Acquire(lifetime)
				if acquire != nil && !errors.Is(acquire, resource.ErrUnavailable) {
					return nil, acquire
				}
				if acquire == nil {
					defer previous.Release()
				}
				if name == "follow" && revision == 1 {
					if acquire != nil {
						return nil, errors.New("level-only update had no original physical source")
					}
					old, err := previous.Value()
					if err != nil {
						return nil, err
					}
					owner, setup = selected.Adopt(lifetime, old)
				} else {
					owner, setup = selected.Open(lifetime, dependencies)
				}
				if owner == nil {
					return nil, setup
				}
				entry := &generation{owner: owner, done: make(chan struct{})}
				mu.Lock()
				sources[name] = append(sources[name], entry)
				mu.Unlock()
				return &resource.Instance[zerolog.Handle]{Value: owner.Handle(), Release: entry.release}, setup
			},
		})
		return ref, err
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
	fixed, err := zerolog.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := zerolog.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	original, err := followRef.Inspect()
	if err != nil {
		return err
	}
	held, err := follow.With(ctx, slog.String("view", "original"))
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, held.Close(ctx)) }()
	marker, err := os.Stat(filepath.Join(oldDirectory, ".fathomry.lock"))
	if err != nil {
		return err
	}
	active, err := os.Stat(filepath.Join(oldDirectory, "current.jsonl"))
	if err != nil {
		return err
	}
	if err := apply(1); err != nil {
		return err
	}
	updatedMarker, err := os.Stat(filepath.Join(oldDirectory, ".fathomry.lock"))
	if err != nil || !os.SameFile(marker, updatedMarker) {
		return errors.New("level-only adoption replaced the physical ownership marker")
	}
	updatedActive, err := os.Stat(filepath.Join(oldDirectory, "current.jsonl"))
	if err != nil || !os.SameFile(active, updatedActive) {
		return errors.New("level-only adoption replaced the physical active file")
	}
	updated, err := followRef.Inspect()
	if err != nil || updated.Generation == original.Generation || updated.Retiring != 1 {
		return errors.New("level-only Follow lost the retained original generation")
	}
	receipt, err := held.Log(ctx, zerolog.Info, "old-filtered")
	oldResult, err := await(ctx, receipt, err, true)
	if err != nil {
		return err
	}
	receipt, err = held.Log(ctx, zerolog.Warn, "old-warn")
	if _, err := await(ctx, receipt, err, false); err != nil {
		return err
	}
	receipt, err = follow.Log(ctx, zerolog.Debug, "new-debug")
	newResult, err := await(ctx, receipt, err, false)
	if err != nil {
		return err
	}
	if oldResult.Source().Revision != newResult.Source().Revision || oldResult.PolicyRevision() == newResult.PolicyRevision() ||
		oldResult.Attribution().Source.Generation != original.Generation || newResult.Attribution().Source.Generation != updated.Generation {
		return errors.New("physical source, immutable policy and borrowed generation were conflated")
	}
	mu.Lock()
	oldOwner := sources["follow"][0]
	mu.Unlock()
	if oldOwner.owner.ShutdownComplete() {
		return errors.New("held old policy released prematurely")
	}
	if err := held.Close(ctx); err != nil {
		return err
	}
	if err := oldOwner.wait(ctx); err != nil {
		return err
	}
	if err := markerState(oldDirectory, true); err != nil {
		return err
	}
	receipt, err = follow.Log(ctx, zerolog.Debug, "after-old-close")
	if _, err := await(ctx, receipt, err, false); err != nil {
		return err
	}
	oldPhysical, err := follow.Retain(ctx)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, oldPhysical.Close(ctx)) }()
	if err := apply(2); err != nil {
		return err
	}
	receipt, err = oldPhysical.Log(ctx, zerolog.Warn, "retired-other-directory")
	if _, err := await(ctx, receipt, err, false); err != nil {
		return err
	}
	receipt, err = follow.Log(ctx, zerolog.Info, "different-directory")
	if _, err := await(ctx, receipt, err, false); err != nil {
		return err
	}
	receipt, err = fixed.Log(ctx, zerolog.Info, "fixed-original")
	if _, err := await(ctx, receipt, err, false); err != nil {
		return err
	}
	receipt, err = fixed.Log(ctx, zerolog.Debug, "fixed-filtered")
	if _, err := await(ctx, receipt, err, true); err != nil {
		return err
	}
	if err := markerState(oldDirectory, true); err != nil {
		return err
	}
	if err := markerState(newDirectory, true); err != nil {
		return err
	}
	if err := oldPhysical.Close(ctx); err != nil {
		return err
	}
	mu.Lock()
	previous := sources["follow"][1]
	mu.Unlock()
	if err := previous.wait(ctx); err != nil {
		return err
	}
	if err := markerState(oldDirectory, false); err != nil {
		return err
	}
	if err := runtime.Resources().Close(ctx); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	if err := receiver.Finish(ctx); err != nil {
		return err
	}
	if status, err := receiver.Status(); err != nil || status.Failures != 0 || !status.Stopped || status.Delivered < 10 {
		return errors.New("independent evidence receiver did not finish after producer cleanup")
	}
	for _, directory := range []string{fixedDirectory, oldDirectory, newDirectory} {
		if err := markerState(directory, false); err != nil {
			return err
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sources["fixed"]) != 1 || len(sources["follow"]) != 3 {
		return errors.New("Fixed/Follow construction count changed")
	}
	for _, entries := range sources {
		for _, entry := range entries {
			if !entry.owner.ShutdownComplete() {
				return errors.New("logical owner retained after staged cleanup")
			}
		}
	}
	for directory, expected := range map[string][]string{
		fixedDirectory: {"fixed-original"},
		oldDirectory:   {"old-warn", "new-debug", "after-old-close", "retired-other-directory"},
		newDirectory:   {"different-directory"},
	} {
		if err := checkMessages(directory, expected); err != nil {
			return err
		}
	}
	return nil
}

func checkMessages(directory string, expected []string) error {
	file, err := os.Open(filepath.Join(directory, "current.jsonl"))
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	index := 0
	for scanner.Scan() {
		var record struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}
		if index >= len(expected) || record.Message != expected[index] {
			return errors.New("local records changed frozen thresholds, destination or physical order")
		}
		index++
	}
	if index != len(expected) {
		return errors.New("local output lost an accepted generation's record")
	}
	return scanner.Err()
}
