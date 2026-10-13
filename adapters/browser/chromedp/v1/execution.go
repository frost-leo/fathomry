/*
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

package chromedp

import (
	"context"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// ExecutionContext is an observed, unique V8 context in its original page and
// source generation. It is borrowed, not an owner or a reusable numeric-ID grant.
type ExecutionContext struct {
	private
	native *native.ExecutionContext
}

// DataCopy contains numeric and unique identity only. It does not claim that an
// unobserved context name, frame association or default-world property is known.
func (execution *ExecutionContext) DataCopy() []byte {
	if execution == nil || execution.native == nil {
		return nil
	}
	return execution.native.DataCopy()
}

// ExecutionContexts snapshots the SDK's bounded identity table. It includes
// worlds created during attachment, without re-enabling Runtime or running JS.
// This method does not create a producer or keep any execution context alive.
func (p *Page) ExecutionContexts(ctx context.Context) (contexts []*ExecutionContext, receipt *adapters.Receipt[Result], err error) {
	_, receipt, err = runPageOperation(ctx, p, "execution_contexts", func(ctx context.Context) ([]jsonv2.Value, error) {
		values, err := p.native.ExecutionContexts(ctx)
		if err != nil {
			return nil, err
		}
		contexts = make([]*ExecutionContext, len(values))
		data := make([]jsonv2.Value, len(values))
		for index, value := range values {
			contexts[index] = &ExecutionContext{native: value}
			data[index] = jsonv2.Value(value.DataCopy())
		}
		return data, nil
	})
	return contexts, receipt, err
}

// EvaluateIn preserves native evaluation parameters within this group's object
// and result custody. Context identity and group name are filled only in a copy.
// A retired, foreign or zero grant cannot fall back to the default context.
// The native system-unique context ID also rejects reuse across process changes.
func (group *ObjectGroup) EvaluateIn(ctx context.Context, execution *ExecutionContext, params runtime.EvaluateParams) (*Object, *adapters.Receipt[Result], error) {
	return group.run(ctx, "execution_evaluate", func(ctx context.Context) (*native.Object, error) {
		if execution == nil {
			return nil, fail(ErrInput, "execution-context")
		}
		return group.state.native.EvaluateIn(ctx, execution.native, params)
	})
}

// Frame is a borrowed document grant from a specific target's frame tree.
// DataCopy exposes frozen native frame metadata, never a mutable SDK owner.
type Frame struct {
	private
	native *native.Frame
}

func (frame *Frame) DataCopy() []byte {
	if frame == nil || frame.native == nil {
		return nil
	}
	return frame.native.DataCopy()
}

// Frames returns the native tree in preorder. Metadata for an out-of-process
// iframe does not attach that target. Use Context.Targets/Attach for that step.
func (p *Page) Frames(ctx context.Context) (frames []*Frame, receipt *adapters.Receipt[Result], err error) {
	_, receipt, err = runPageOperation(ctx, p, "frames", func(ctx context.Context) ([]jsonv2.Value, error) {
		values, err := p.native.Frames(ctx)
		if err != nil {
			return nil, err
		}
		frames = make([]*Frame, len(values))
		data := make([]jsonv2.Value, len(values))
		for index, value := range values {
			frames[index] = &Frame{native: value}
			data[index] = jsonv2.Value(value.DataCopy())
		}
		return data, nil
	})
	return frames, receipt, err
}

// CreateWorld creates or reuses a native isolated world in this exact document.
// Native options retain their meanings, including an explicit universal-access
// choice. Parameters stay immutable until return and are copied after admission.
// World lifetime belongs to the frame. CDP has no independent destroy-world call.
// A world created in a borrowed page can therefore outlive provider detach.
// Creation racing with navigation is not atomic: a stale result is refused, but
// that does not prove that no world was created. The page still owns its cleanup.
// MaxEvents also bounds cumulative world-creation attempts, including unknown
// outcomes. Repeated creation of an existing named world uses an attempt too.
func (p *Page) CreateWorld(ctx context.Context, frame *Frame, params page.CreateIsolatedWorldParams) (execution *ExecutionContext, receipt *adapters.Receipt[Result], err error) {
	_, receipt, err = runPageOperation(ctx, p, "create_world", func(ctx context.Context) (jsonv2.Value, error) {
		if frame == nil {
			return nil, fail(ErrInput, "frame")
		}
		value, err := p.native.CreateWorld(ctx, frame.native, params)
		if value == nil {
			return nil, err
		}
		execution = &ExecutionContext{native: value}
		return jsonv2.Value(value.DataCopy()), err
	})
	return execution, receipt, err
}
