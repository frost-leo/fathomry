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
	"slices"
	"sync"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

// Source is an explicitly selected original-file profile. Native defaults and
// environment queries are deliberately absent; layer policy belongs to callers.
type Source struct {
	private
	client   *Client
	settings WatchSettings
}

// Source freezes paths/observation options without opening files or starting a
// worker. Capture/Observe admit their applicable native options when used.
func (client *Client) Source(settings WatchSettings) (*Source, error) {
	if client == nil || len(settings.Paths) == 0 || len(settings.Paths) > MaxSources {
		return nil, fail(ErrInput, "source")
	}
	settings.Paths = slices.Clone(settings.Paths)
	return &Source{client: client, settings: settings}, nil
}

// Capture reads all paths under one public operation/evidence reservation.
// Missing and empty remain distinct; an error returns no usable prefix.
func (source *Source) Capture(ctx context.Context) (configsource.Batch, int, error) {
	if source == nil || source.client == nil {
		return configsource.Batch{}, -1, fail(ErrInput, "capture")
	}
	var batch configsource.Batch
	failed := -1
	err := source.client.run(ctx, "capture", func(ctx context.Context) (Evidence, error) {
		var err error
		batch, failed, err = source.capture(ctx)
		return Evidence{Documents: batch.Len()}, err
	})
	if err != nil {
		return configsource.Batch{}, failed, err
	}
	return batch, failed, nil
}
func (source *Source) capture(ctx context.Context) (configsource.Batch, int, error) {
	values := make([]configsource.Raw, 0, len(source.settings.Paths))
	remaining := MaxTotalBytes
	for index, path := range source.settings.Paths {
		raw, missing, err := native.RawFile(ctx, path, min(MaxDocumentBytes, remaining))
		if err != nil {
			return configsource.Batch{}, index, translate(err, "capture")
		}
		remaining -= len(raw)
		values = append(values, configsource.Raw{Content: raw, Missing: missing})
	}
	batch, err := configsource.NewBatch(values)
	return batch, -1, err
}

// Observe owns one native invalidation worker and one bounded acquisition/handoff
// worker. It adds no timer or polling/recovery engine. A single latest complete
// observation is retained; replacing an undelivered observation makes Gap sticky.
// Its lifetime reservation includes all acquisitions; Next only waits for data.
func (source *Source) Observe(ctx context.Context) (configsource.Observer, error) {
	if source == nil || source.client == nil {
		return nil, fail(ErrInput, "observe")
	}
	var observer *fileObserver
	var setup error
	declared := request("observe_raw")
	declared.WorkBytes = 24 << 20
	receipt, err := source.client.endpoint.Run(ctx, declared, func(call *adapters.Call[Evidence]) {
		guard, err := call.Hold()
		if err != nil {
			setup = err
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: err})
			return
		}
		settings := source.settings
		subscription, err := native.Watch(call.Context(), native.WatchOptionsV1{Paths: settings.Paths, Interval: settings.Interval, QueueCapacity: settings.QueueCapacity})
		if err != nil {
			setup = translate(err, "observe")
			_ = call.Resolve(adapters.Outcome[Evidence]{Primary: setup})
			_ = guard.Release()
			return
		}
		observer = &fileObserver{call: call, receipt: call.Receipt(), changed: make(chan struct{})}
		go observer.run(source, subscription, guard)
	})
	if err != nil {
		return nil, err
	}
	if observer == nil {
		if setup != nil {
			return nil, setup
		}
		value, _ := receipt.Snapshot()
		return nil, value.Err()
	}
	return observer, nil
}

type fileObserver struct {
	private
	call     *adapters.Call[Evidence]
	receipt  *adapters.Receipt[Evidence]
	mu       sync.Mutex
	pending  *configsource.Observation
	changed  chan struct{}
	closing  bool
	terminal error
}

func (observer *fileObserver) run(source *Source, subscription *native.Subscription, guard adapters.Guard) {
	var terminal error
	for observer.call.Context().Err() == nil {
		change, err := subscription.Next(observer.call.Context())
		if err != nil {
			if observer.call.Context().Err() == nil {
				terminal = translate(err, "observe")
			}
			break
		}
		value := configsource.Observation{FailedIndex: change.Index(), Err: translate(change.Err(), "observe"), Gap: change.Resync()}
		if value.Err == nil {
			value.Batch, value.FailedIndex, value.Err = source.capture(observer.call.Context())
		}
		observer.mu.Lock()
		if !observer.closing && observer.call.Context().Err() == nil {
			if observer.pending != nil {
				value.Gap = true
			}
			observer.pending = &value
			close(observer.changed)
			observer.changed = make(chan struct{})
		}
		observer.mu.Unlock()
	}
	cleanup := translate(subscription.Close(context.Background()), "close")
	observer.mu.Lock()
	observer.closing = true
	observer.pending = nil
	observer.terminal = terminal
	close(observer.changed)
	observer.mu.Unlock()
	_ = observer.call.Resolve(adapters.Outcome[Evidence]{Value: Evidence{}, Present: true, Primary: terminal, Cleanup: cleanup})
	_ = guard.Release()
}
func (observer *fileObserver) Next(ctx context.Context) (configsource.Observation, error) {
	if observer == nil || ctx == nil {
		return configsource.Observation{}, fail(ErrInput, "next")
	}
	for {
		if ctx.Err() != nil {
			return configsource.Observation{}, fail(ErrRead, "next", ctx.Err(), context.Cause(ctx))
		}
		observer.mu.Lock()
		if observer.closing || observer.call.Context().Err() != nil {
			terminal := observer.terminal
			observer.mu.Unlock()
			return configsource.Observation{}, fail(ErrClosed, "next", terminal, observer.call.Context().Err(), context.Cause(observer.call.Context()))
		}
		if observer.pending != nil {
			result := *observer.pending
			observer.pending = nil
			observer.mu.Unlock()
			return result, nil
		}
		changed := observer.changed
		observer.mu.Unlock()
		select {
		case <-changed:
		case <-observer.call.Context().Done():
		case <-ctx.Done():
			return configsource.Observation{}, fail(ErrRead, "next", ctx.Err(), context.Cause(ctx))
		}
	}
}
func (observer *fileObserver) Close(ctx context.Context) error {
	if observer == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	observer.mu.Lock()
	observer.closing = true
	observer.pending = nil
	observer.mu.Unlock()
	_ = observer.call.Cancel(nil)
	value, err := observer.receipt.WaitReleased(ctx)
	if err != nil {
		return fail(ErrState, "close", err)
	}
	return value.Err()
}

var _ configsource.Source = (*Source)(nil)
var _ configsource.Observer = (*fileObserver)(nil)
