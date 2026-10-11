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

package iceberg

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/table"
)

// ManagedPlan retains exact originating scan, schema, credentials and task state.
// Detached PlanFiles output is inspection/adoption data, not this capability.
type ManagedPlan struct {
	private
	scope  *ManagedSession
	scan   *table.Scan
	tasks  []table.FileScanTask
	mu     sync.Mutex
	closed bool
}

func (s *ManagedScan) Plan() (*ManagedPlan, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	return managedCall(s.scope, "owned-plan", grantRead, func(ctx context.Context) (*ManagedPlan, error) {
		if err := s.scope.handle(); err != nil {
			return nil, err
		}
		scan := s.scan.UseRowLimit(s.limit)
		tasks, err := scan.PlanFiles(ctx)
		if err != nil {
			s.scope.noteCleanup(scan.Close())
			return nil, err
		}
		if err = s.scope.validateScanTasks(tasks); err != nil {
			s.scope.noteCleanup(scan.Close())
			return nil, err
		}
		plan := &ManagedPlan{scope: s.scope, scan: scan, tasks: tasks}
		s.scope.closers = append(s.scope.closers, plan)
		return plan, nil
	})
}
func (p *ManagedPlan) Records(visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if p == nil || p.scope == nil || visit == nil {
		return ScanSummary{}, failure(ErrInput, "plan")
	}
	return managedCall(p.scope, "planned-records", grantRead, func(ctx context.Context) (ScanSummary, error) {
		if !p.mu.TryLock() {
			return ScanSummary{}, failure(ErrAuthority, "plan-busy")
		}
		defer p.mu.Unlock()
		if p.closed {
			return ScanSummary{}, failure(ErrAuthority, "plan-closed")
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		_, records, err := p.scan.ReadTasks(child, p.tasks)
		if err != nil {
			return ScanSummary{}, err
		}
		return p.scope.consume(child, cancel, records, visit)
	})
}
func (p *ManagedPlan) Close() error {
	if p == nil || p.scope == nil {
		return failure(ErrInput, "plan")
	}
	if !p.mu.TryLock() {
		return failure(ErrAuthority, "plan-busy")
	}
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.tasks = nil
	err := p.scan.Close()
	p.scope.mu.Lock()
	if err != nil && !p.scope.closed {
		p.scope.cleanup = append(p.scope.cleanup, err)
	}
	p.scope.mu.Unlock()
	return failureIf(err)
}
