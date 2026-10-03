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

package nacos

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestResourceComposition(t *testing.T) {
	for _, policy := range []resource.Policy{resource.Fixed, resource.Follow} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			fixture := newService(t)
			fixture.values[Key{Group: "DEFAULT_GROUP", DataID: "second"}] = "value: second\n"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			runtime, err := adapters.New(ctx, adapters.Options{MaxWorkBytes: 256 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			inbox, err := adapters.NewInbox[Evidence](adapters.EvidenceOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dependencies := Dependencies{Runtime: runtime, Evidence: inbox}
			scope, err := resource.New(ctx, resource.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close(context.Background())
			released := make(chan int, 4)
			ref, err := resource.Bind(scope, resource.Binding[int, Handle]{
				Name: "configuration", Policy: policy,
				Select: func(view settings.View) (int, error) {
					snapshot, err := settings.As[int](view)
					if err != nil {
						return 0, err
					}
					return snapshot.ValueCopy()
				},
				Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
				Build: func(ctx context.Context, version int) (*resource.Instance[Handle], error) {
					selected := fixture.settings()
					if version == 2 {
						selected.Keys = []Key{{DataID: "second"}}
					}
					owner, err := Open(ctx, selected, dependencies)
					if owner == nil {
						return nil, err
					}
					return &resource.Instance[Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
						result := owner.Release(ctx)
						if result.Complete {
							released <- version
						}
						return result
					}}, err
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			apply := func(version int) {
				snapshot, err := settings.New(version, func(value int) int { return value })
				if err != nil {
					t.Fatal(err)
				}
				update, err := scope.Apply(ctx, snapshot.View())
				if err != nil {
					t.Fatal(err)
				}
				if err := update.Wait(ctx); err != nil {
					t.Fatal(err)
				}
			}
			apply(1)
			client, err := Using(ref, dependencies)
			if err != nil {
				t.Fatal(err)
			}
			observation, err := client.ObserveRaw(ctx, ObserveOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer observation.Close(context.Background())
			initial, err := observation.Next(ctx)
			if err != nil || initial.Err() != nil {
				t.Fatal(err)
			}
			original, _ := observation.state.receipt.Snapshot()
			if original.Info().Source.Name != "configuration" || original.Info().Source.Generation != 1 || original.Info().Released {
				t.Fatal("subscription did not borrow generation")
			}
			apply(2)
			result, err := client.ReadAll(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected := "main"
			if policy == resource.Follow {
				expected = "second"
			}
			if result[0].Key().DataID != expected {
				t.Fatal("adoption policy lost")
			}
			select {
			case <-released:
				t.Fatal("native generation released while observation borrowed it")
			default:
			}
			if string(initial.DocumentsCopy()[0].RawCopy()) != "value: initial\n" {
				t.Fatal("old observation retargeted")
			}
			if err := observation.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if policy == resource.Follow {
				select {
				case version := <-released:
					if version != 1 {
						t.Fatal("wrong retired generation")
					}
				case <-ctx.Done():
					t.Fatal("old generation not cleaned")
				}
			}
			if err := scope.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Close(ctx); err != nil {
				t.Fatal(err)
			}
			status, _ := inbox.Inspect()
			for range status.Queued {
				if err := inbox.DeliverOne(ctx, func(_ context.Context, value adapters.Snapshot[Evidence]) error { return value.Err() }); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRawCaptureRetainsOneGeneration(t *testing.T) {
	fixture := newService(t)
	entered := make(chan string, 1)
	release := make(chan struct{})
	fixture.mu.Lock()
	fixture.queryEntered, fixture.queryRelease, fixture.namespaceContent = entered, release, true
	fixture.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := adapters.New(ctx, adapters.Options{MaxWorkBytes: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[Evidence](adapters.EvidenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: runtime, Evidence: inbox}
	scope, err := resource.New(ctx, resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	ref, err := resource.Bind(scope, resource.Binding[string, Handle]{
		Name: "source", Policy: resource.Follow,
		Select: func(view settings.View) (string, error) {
			snapshot, err := settings.As[string](view)
			if err != nil {
				return "", err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value string) string { return value }, Equal: func(left, right string) bool { return left == right },
		Build: func(ctx context.Context, namespace string) (*resource.Instance[Handle], error) {
			options := fixture.settings()
			options.Namespace = namespace
			options.RequestTimeout = 5 * time.Second
			owner, err := Open(ctx, options, dependencies)
			if owner == nil {
				return nil, err
			}
			return &resource.Instance[Handle]{Value: owner.Handle(), Release: owner.Release}, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(namespace string) {
		snapshot, _ := settings.New(namespace, func(value string) string { return value })
		update, err := scope.Apply(ctx, snapshot.View())
		if err != nil {
			t.Fatal(err)
		}
		if err := update.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	apply("first")
	client, err := Using(ref, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	source, err := client.Source(ObserveOptions{}, Key{DataID: "main"}, Key{DataID: "second"})
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		batch, _, err := source.Capture(ctx)
		if err == nil {
			values, copyErr := batch.DocumentsCopy()
			err = copyErr
			if err == nil && (len(values) != 2 || string(values[0].Content) != "first" || string(values[1].Content) != "first") {
				err = errors.New("capture mixed resource generations")
			}
		}
		completed <- err
	}()
	select {
	case namespace := <-entered:
		if namespace != "first" {
			t.Fatal(namespace)
		}
	case <-ctx.Done():
		t.Fatal("first query not dispatched")
	}
	apply("second")
	unblock()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("capture not completed")
	}
}
