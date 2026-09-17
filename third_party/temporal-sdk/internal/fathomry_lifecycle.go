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

package internal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type fathomryWorkerLifetime struct {
	work           sync.WaitGroup
	externalMu     sync.Mutex
	externalClosed bool
	stopStarted    atomic.Bool
	stopReturned   chan struct{}
	joinOnce       sync.Once
	joined         chan struct{}
}

func (l *fathomryWorkerLifetime) enterExternal() bool {
	if l == nil {
		return true
	}
	l.externalMu.Lock()
	defer l.externalMu.Unlock()
	if l.externalClosed {
		return false
	}
	l.add()
	return true
}

func (l *fathomryWorkerLifetime) closeExternal() {
	l.externalMu.Lock()
	l.externalClosed = true
	l.externalMu.Unlock()
}

func newFathomryWorkerLifetime() *fathomryWorkerLifetime {
	return &fathomryWorkerLifetime{stopReturned: make(chan struct{}), joined: make(chan struct{})}
}

func (l *fathomryWorkerLifetime) add() {
	if l != nil {
		l.work.Add(1)
	}
}

func (l *fathomryWorkerLifetime) end() {
	if l != nil {
		l.work.Done()
	}
}

// FathomryWaitStoppedV1 observes local termination after Stop, not remote execution
// completion. It never forces a callback to return or changes its retry outcome.
// A canceled wait retains the join obligation; a later call may continue waiting.
func (aw *AggregatedWorker) FathomryWaitStoppedV1(ctx context.Context) error {
	lifetime := aw.executionParams.fathomryLifetime
	if lifetime == nil || ctx == nil {
		return errors.New("temporal: managed worker lifecycle was not enabled")
	}
	select {
	case <-lifetime.stopReturned:
	case <-ctx.Done():
		return errors.Join(ctx.Err(), context.Cause(ctx))
	}
	lifetime.joinOnce.Do(func() {
		go func() {
			if aw.workflowWorker != nil {
				aw.workflowWorker.worker.stopWG.Wait()
				aw.workflowWorker.localActivityWorker.stopWG.Wait()
			}
			if aw.activityWorker != nil {
				aw.activityWorker.worker.stopWG.Wait()
			}
			if aw.sessionWorker != nil {
				aw.sessionWorker.creationWorker.worker.stopWG.Wait()
				aw.sessionWorker.activityWorker.worker.stopWG.Wait()
			}
			if aw.nexusWorker != nil {
				aw.nexusWorker.worker.stopWG.Wait()
			}
			aw.executionParams.cache.fathomryEvictOwned()
			lifetime.work.Wait()
			aw.executionParams.cache.close(&sharedWorkerCacheLock)
			close(lifetime.joined)
		}()
	})
	select {
	case <-lifetime.joined:
		return nil
	case <-ctx.Done():
		return errors.Join(ctx.Err(), context.Cause(ctx))
	}
}

func (wc *WorkerCache) fathomryRetain(runID string, context *workflowExecutionContextImpl) {
	if wc.fathomryLifetime == nil {
		return
	}
	wc.fathomryMu.Lock()
	defer wc.fathomryMu.Unlock()
	if _, exists := wc.fathomryEntries[context]; !exists {
		wc.fathomryLifetime.add()
		wc.fathomryEntries[context] = runID
	}
}

func (wc *WorkerCache) fathomryEvicted(context *workflowExecutionContextImpl) {
	if wc.fathomryLifetime == nil {
		return
	}
	wc.fathomryMu.Lock()
	defer wc.fathomryMu.Unlock()
	if _, exists := wc.fathomryEntries[context]; exists {
		delete(wc.fathomryEntries, context)
		wc.fathomryLifetime.end()
	}
}

func (wc *WorkerCache) fathomryEvictOwned() {
	wc.fathomryMu.Lock()
	defer wc.fathomryMu.Unlock()
	for context, runID := range wc.fathomryEntries {
		wc.getWorkflowCache().DeleteIf(runID, context)
	}
}
