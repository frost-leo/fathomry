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
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/indexeddb"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// IndexedDBData retains the native result and independently copied metadata.
// Its remote objects belong to the ObjectGroup that performed the read.
type IndexedDBData struct {
	value indexeddb.RequestDataResult
	data  []byte
}

func (data IndexedDBData) NativeValue() indexeddb.RequestDataResult { return data.value }
func (data IndexedDBData) DataCopy() []byte                         { return append([]byte(nil), data.data...) }

// ReadIndexedDB reserves three remote-value slots per requested row before
// copying parameters or issuing IndexedDB.requestData. Native PageSize zero is
// preserved; negative or over-capacity sizes are refused. The scope and group
// must belong to the same Session. Unknown/oversized native results retain
// cleanup custody until positive session/resource teardown, never a guessed ID.
func (group *ObjectGroup) ReadIndexedDB(ctx context.Context, scope *StorageScope, params indexeddb.RequestDataParams) (result IndexedDBData, err error) {
	if ctx == nil || group == nil || group.state == nil || scope == nil || scope.session == nil || scope.session.sessionState != group.state.session.sessionState {
		return result, failure(ErrInput, "indexeddb-origin")
	}
	state, session := group.state, group.state.session
	own := session.owner
	defer func() { session.remember(err) }()
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return result, err
	}
	defer leave()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-work.Done():
		return result, failure(ErrState, "indexeddb-wait", work.Err(), context.Cause(work))
	}
	state.mu.Lock()
	closed := state.closed || state.sealed || state.life.Err() != nil
	state.mu.Unlock()
	if closed {
		return result, failure(ErrState, "indexeddb-group")
	}
	if _, err := session.storageContext(); err != nil {
		return result, err
	}
	if params.PageSize < 0 || params.SkipCount < 0 || params.PageSize > int64(own.settings.MaxObjects/3) {
		return result, failure(ErrLimit, "indexeddb-page")
	}
	reserved := int(params.PageSize) * 3
	own.mu.Lock()
	if reserved > own.settings.MaxObjects-session.browserContext.objectCount {
		own.mu.Unlock()
		return result, failure(ErrLimit, "objects")
	}
	session.browserContext.objectCount += reserved
	own.mu.Unlock()
	kept := 0
	admitted, returned := false, false
	defer func() {
		own.mu.Lock()
		session.browserContext.objectCount -= reserved - kept
		own.mu.Unlock()
		if !returned {
			err = failure(ErrPanic, "indexeddb-did-not-return")
		}
		if admitted && err != nil {
			state.mu.Lock()
			state.unknown = true
			if state.primary == nil {
				state.primary = err
			}
			state.mu.Unlock()
			state.cancel(err)
		}
	}()
	if err := session.reserveCommand(); err != nil {
		returned = true
		return result, err
	}
	buffer := boundedBuffer{limit: own.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		returned = true
		return result, err
	}
	var copied indexeddb.RequestDataParams
	if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
		returned = true
		return result, err
	}
	if err := scope.fillParams(&copied); err != nil {
		returned = true
		return result, err
	}
	work, cancel := context.WithCancelCause(work)
	stop := context.AfterFunc(state.life, func() { cancel(context.Cause(state.life)) })
	defer stop()
	defer cancel(nil)
	epoch := session.objectOrigin.epoch.Load()
	work = session.commandWork(work, false, func() error {
		if state.life.Err() != nil || session.objectOrigin.epoch.Load() != epoch {
			return failure(ErrState, "indexeddb-document")
		}
		admitted = true
		return nil
	})
	value, err := cdp.Call(work, sdk.FromContext(session.native).Target, indexeddb.RequestData, copied)
	if err == nil {
		var references []*runtime.RemoteObject
		references, err = indexedDBObjects(value, int(params.PageSize))
		if err == nil {
			buffer = boundedBuffer{limit: own.settings.MaxResultBytes}
			err = WriteNativeResult(&buffer, value)
		}
		var entries []*objectState
		var size int64
		if err == nil {
			for _, reference := range references {
				metadata := boundedBuffer{limit: own.settings.MaxResultBytes - size}
				if err = WriteNativeResult(&metadata, reference); err != nil {
					break
				}
				size += int64(len(metadata.data))
				entries = append(entries, &objectState{group: state, value: reference, id: reference.ObjectID, data: metadata.data, epoch: epoch})
			}
		}
		state.mu.Lock()
		if err == nil && size > own.settings.MaxResultBytes-state.bytes {
			err = failure(ErrLimit, "indexeddb-metadata")
		}
		if err == nil {
			for _, entry := range entries {
				state.objects[entry] = struct{}{}
			}
			state.bytes += size
			kept = len(entries)
		}
		state.mu.Unlock()
		if err == nil {
			own.mu.Lock()
			if session.browserContext.objectRefs == nil {
				session.browserContext.objectRefs = make(map[*runtime.RemoteObject]*objectState)
			}
			for _, entry := range entries {
				session.browserContext.objectRefs[entry.value] = entry
			}
			own.mu.Unlock()
			result = IndexedDBData{value: value, data: buffer.data}
		}
	}
	if err == nil && session.objectOrigin.epoch.Load() != epoch {
		err = failure(ErrState, "indexeddb-document")
	}
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), state.life.Err(), context.Cause(state.life))
	if err != nil {
		err = failure(ErrNative, "indexeddb-read", err)
	}
	returned = true
	return result, err
}

func indexedDBObjects(value indexeddb.RequestDataResult, pageSize int) ([]*runtime.RemoteObject, error) {
	if len(value.ObjectStoreDataEntries) > pageSize {
		return nil, failure(ErrLimit, "indexeddb-rows")
	}
	references := make([]*runtime.RemoteObject, 0, len(value.ObjectStoreDataEntries)*3)
	for _, entry := range value.ObjectStoreDataEntries {
		if entry == nil || entry.Key == nil || entry.PrimaryKey == nil || entry.Value == nil {
			return nil, failure(ErrNative, "indexeddb-entry")
		}
		references = append(references, entry.Key, entry.PrimaryKey, entry.Value)
	}
	return references, nil
}
