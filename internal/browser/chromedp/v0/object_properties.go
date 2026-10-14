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
	"errors"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// Properties retains native property descriptors and their exact reference
// identities. Metadata is data, not an independently transferable capability.
type Properties struct {
	value runtime.GetPropertiesResult
	data  []byte
}

func (properties Properties) NativeValue() runtime.GetPropertiesResult { return properties.value }
func (properties Properties) DataCopy() []byte                         { return append([]byte(nil), properties.data...) }

// Properties delegates Runtime.getProperties without invoking JavaScript
// getters or reconstructing descriptors. All returned references inherit this
// object's group. Parameters are borrowed until return and copied before filling
// ObjectID. A conflicting ObjectID is invalid, not additional authority.
func (object *Object) Properties(ctx context.Context, params runtime.GetPropertiesParams) (result Properties, err error) {
	if object == nil || object.state == nil || ctx == nil {
		return result, failure(ErrInput, "object-properties")
	}
	entry, group := object.state, object.state.group
	session, own := group.session, group.session.owner
	defer func() { session.remember(err) }()
	if err := object.valid(); err != nil {
		return result, err
	}
	if entry.id == "" || params.ObjectID != "" && params.ObjectID != entry.id {
		return result, failure(ErrInput, "object-properties-origin")
	}
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return result, err
	}
	defer leave()
	select {
	case group.gate <- struct{}{}:
		defer func() { <-group.gate }()
	case <-work.Done():
		return result, failure(ErrState, "object-properties-wait", work.Err(), context.Cause(work))
	}
	if err := object.valid(); err != nil {
		return result, err
	}
	// The native method has no count limit. Reserve the remaining reference
	// capacity before dispatch and refuse an oversized reply before retention.
	own.mu.Lock()
	reserved := own.settings.MaxObjects - session.browserContext.objectCount
	if reserved <= 0 {
		own.mu.Unlock()
		return result, failure(ErrLimit, "objects")
	}
	session.browserContext.objectCount += reserved
	own.mu.Unlock()
	kept := 0
	admitted, terminal, returned := false, false, false
	defer func() {
		own.mu.Lock()
		session.browserContext.objectCount -= reserved - kept
		own.mu.Unlock()
		if !returned {
			err = failure(ErrPanic, "object-properties-did-not-return")
		}
		group.mu.Lock()
		if admitted && (!terminal || !entry.grouped && err != nil) {
			group.unknown = true
		}
		if err != nil && group.primary == nil {
			group.primary = err
		}
		group.mu.Unlock()
		if admitted && err != nil {
			group.cancel(err)
		}
	}()
	if err = session.reserveCommand(); err != nil {
		returned = true
		return result, err
	}
	work, cancel := context.WithCancelCause(work)
	stop := context.AfterFunc(group.life, func() { cancel(context.Cause(group.life)) })
	defer stop()
	defer cancel(nil)
	copied := params
	copied.ObjectID = entry.id
	work = sdk.WithCallLimits(work, sdk.CallLimits{
		MaxInputBytes: own.settings.MaxCommandBytes, MaxOutputBytes: own.settings.MaxResultBytes,
		Before: func(string) error {
			if err := object.valid(); err != nil {
				return err
			}
			admitted = true
			return nil
		},
		Result: func(_ string, size int64) error {
			terminal = true
			record := session.accounting()
			record.mu.Lock()
			defer record.mu.Unlock()
			if size > own.settings.MaxResultBytes-record.delivered {
				return failure(ErrLimit, "object-output")
			}
			record.delivered += size
			return nil
		},
	})
	value, err := cdp.Call(work, sdk.FromContext(session.native).Target, runtime.GetProperties, copied)
	if err == nil {
		references, collectErr := propertyObjects(value, reserved)
		err = collectErr
		if err == nil {
			buffer := boundedBuffer{limit: own.settings.MaxResultBytes}
			err = WriteNativeResult(&buffer, value)
			group.mu.Lock()
			if err == nil && int64(len(buffer.data)) > own.settings.MaxResultBytes-group.bytes {
				err = failure(ErrLimit, "object-metadata")
			}
			group.mu.Unlock()
			if err == nil {
				entries := make([]*objectState, 0, len(references))
				var size int64
				for _, reference := range references {
					metadata := boundedBuffer{limit: own.settings.MaxResultBytes - size}
					if err = WriteNativeResult(&metadata, reference); err != nil {
						break
					}
					size += int64(len(metadata.data))
					entries = append(entries, &objectState{group: group, value: reference, id: reference.ObjectID, data: metadata.data, epoch: entry.epoch, grouped: entry.grouped, execution: entry.execution, fence: entry.fence})
				}
				group.mu.Lock()
				if err == nil && size > own.settings.MaxResultBytes-group.bytes {
					err = failure(ErrLimit, "object-metadata")
				}
				if err == nil {
					for _, reference := range entries {
						group.objects[reference] = struct{}{}
					}
					group.bytes += size
					kept = len(entries)
				}
				group.mu.Unlock()
				if err == nil {
					own.mu.Lock()
					for _, reference := range entries {
						session.browserContext.objectRefs[reference.value] = reference
					}
					own.mu.Unlock()
					result = Properties{value: value, data: buffer.data}
				}
			}
		}
	}
	err = errors.Join(err, entry.fence.valid(), ctx.Err(), context.Cause(ctx), group.life.Err(), context.Cause(group.life))
	if err != nil {
		err = failure(ErrNative, "object-properties", err)
	}
	returned = true
	return result, err
}

func propertyObjects(value runtime.GetPropertiesResult, limit int) ([]*runtime.RemoteObject, error) {
	var objects []*runtime.RemoteObject
	add := func(object *runtime.RemoteObject) error {
		if object == nil {
			return nil
		}
		if len(objects) >= limit {
			return failure(ErrLimit, "object-properties-count")
		}
		objects = append(objects, object)
		return nil
	}
	for _, property := range value.Result {
		if property == nil {
			return nil, failure(ErrNative, "object-property")
		}
		for _, object := range []*runtime.RemoteObject{property.Value, property.Get, property.Set, property.Symbol} {
			if err := add(object); err != nil {
				return nil, err
			}
		}
	}
	for _, property := range value.InternalProperties {
		if property == nil {
			return nil, failure(ErrNative, "object-property")
		}
		if err := add(property.Value); err != nil {
			return nil, err
		}
	}
	for _, property := range value.PrivateProperties {
		if property == nil {
			return nil, failure(ErrNative, "object-property")
		}
		for _, object := range []*runtime.RemoteObject{property.Value, property.Get, property.Set} {
			if err := add(object); err != nil {
				return nil, err
			}
		}
	}
	if value.ExceptionDetails != nil {
		if err := add(value.ExceptionDetails.Exception); err != nil {
			return nil, err
		}
	}
	return objects, nil
}
