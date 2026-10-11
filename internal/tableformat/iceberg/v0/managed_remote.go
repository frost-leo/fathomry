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
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

type managedRemotePlanKey struct{}

func copyManagedScalar[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// Every reservation belongs to the admitted family, including plans whose
// results are ignored. Cancellation has a separate, pre-reserved request slot.
type managedRemotePlan struct {
	scope      *ManagedSession
	identifier table.Identifier
	id, path   string
	tasks      map[string]bool
	attempted  bool
	done       bool
	err        error
}

func (s *managedSource) planningContext(ctx context.Context, id table.Identifier) (context.Context, error) {
	f, _ := ctx.Value(managedFamilyKey{}).(*ManagedSession)
	if f == nil || f.source != s {
		return nil, failure(ErrAuthority, "planning-origin")
	}
	if err := f.identifier(id); err != nil {
		return nil, err
	}
	if err := f.handle(); err != nil {
		return nil, err
	}
	plan := &managedRemotePlan{scope: f, identifier: slices.Clone(id), tasks: map[string]bool{}}
	f.mu.Lock()
	f.remotePlans = append(f.remotePlans, plan)
	f.mu.Unlock()
	return context.WithValue(ctx, managedRemotePlanKey{}, plan), nil
}

func (f *ManagedSession) remotePlan(id table.Identifier, planID string, cleanup bool) (*managedRemotePlan, error) {
	if err := f.identifier(id); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, plan := range f.remotePlans {
		if plan.id == planID && plan.id != "" && slices.Equal(plan.identifier, id) {
			if !cleanup && (f.closed || plan.attempted) {
				return nil, failure(ErrAuthority, "plan-closed")
			}
			return plan, nil
		}
	}
	return nil, failure(ErrAuthority, "plan-origin")
}

func (f *ManagedSession) taskPlan(id table.Identifier, handle string) (*managedRemotePlan, error) {
	if err := f.identifier(id); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, plan := range f.remotePlans {
		if !f.closed && !plan.attempted && plan.tasks[handle] && slices.Equal(plan.identifier, id) {
			return plan, nil
		}
	}
	return nil, failure(ErrAuthority, "plan-task-origin")
}

func (f *ManagedSession) trackPlanningReply(request *http.Request, sent, raw []byte) error {
	plan, _ := request.Context().Value(managedRemotePlanKey{}).(*managedRemotePlan)
	path := request.URL.EscapedPath()
	if plan == nil {
		f.mu.Lock()
		plan = f.remoteByPath[path]
		if plan == nil && request.Method == http.MethodPost && strings.HasSuffix(path, "/tasks") {
			var input rest.FetchScanTasksRequest
			if err := json.Unmarshal(sent, &input); err == nil {
				for _, candidate := range f.remotePlans {
					if candidate.tasks[input.PlanTask] && strings.HasPrefix(candidate.path, strings.TrimSuffix(path, "/tasks")+"/plan/") {
						plan = candidate
						break
					}
				}
			}
		}
		f.mu.Unlock()
	}
	if plan == nil {
		return nil
	}
	if _, err := decodeObjectBounded(raw, f.source.prepared.settings.MaxListEntries); err != nil {
		return err
	}
	var response struct {
		ID *string `json:"plan-id"`
		rest.ScanTasks
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	f.mu.Lock()
	if response.ID != nil && *response.ID != "" && request.Method == http.MethodPost && strings.HasSuffix(path, "/plan") {
		escaped := url.PathEscape(*response.ID)
		if escaped == "." || escaped == ".." {
			escaped = strings.ReplaceAll(escaped, ".", "%2E")
		}
		cancelPath := request.URL.JoinPath(escaped).EscapedPath()
		if existing := f.remoteByPath[cancelPath]; existing == nil {
			plan.id, plan.path = *response.ID, cancelPath
			f.remoteByPath[cancelPath] = plan
		} else {
			plan = existing
		}
	}
	f.mu.Unlock()
	limit := f.source.prepared.settings.MaxListEntries
	if len(response.PlanTasks) > limit || len(response.FileScanTasks) > limit || len(response.DeleteFiles) > limit {
		return failure(ErrLimit, "planning-tasks")
	}
	for _, task := range response.FileScanTasks {
		if task.DataFile == nil {
			return failure(ErrProtocol, "planning-file")
		}
		if err := f.location(task.DataFile.FilePath); err != nil {
			return err
		}
		if task.DataFile.FileSizeInBytes < 0 || task.DataFile.FileSizeInBytes > f.source.prepared.settings.MaxObjectBytes {
			return failure(ErrLimit, "planning-file")
		}
	}
	for _, file := range response.DeleteFiles {
		if err := f.location(file.FilePath); err != nil {
			return err
		}
		if file.ReferencedDataFile != nil {
			if err := f.location(*file.ReferencedDataFile); err != nil {
				return err
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, handle := range response.PlanTasks {
		if len(plan.tasks) >= limit && !plan.tasks[handle] {
			return failure(ErrLimit, "planning-handles")
		}
		plan.tasks[handle] = true
	}
	return nil
}

func (f *ManagedSession) cleanupPlan(request *http.Request) *managedRemotePlan {
	if request.Method != http.MethodDelete || request.URL == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remoteByPath[request.URL.EscapedPath()]
}

func (p *managedRemotePlan) close() error {
	p.scope.mu.Lock()
	if p.id == "" || p.done {
		err := p.err
		p.scope.mu.Unlock()
		return err
	}
	p.scope.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(p.scope.ctx), min(p.scope.source.prepared.settings.Timeout, 5*time.Second))
	defer cancel()
	err := p.scope.source.catalog.CancelPlanning(ctx, p.identifier, p.id)
	p.scope.mu.Lock()
	p.done = true
	p.err = errors.Join(p.err, err)
	p.scope.mu.Unlock()
	return err
}

func (p *managedRemotePlan) beginCancel() (int, bool, error) {
	p.scope.mu.Lock()
	defer p.scope.mu.Unlock()
	if p.attempted || p.done {
		return -1, false, p.err
	}
	p.attempted = true
	index := len(p.scope.data.effects)
	p.scope.data.effects = append(p.scope.data.effects, ManagedEffect{Operation: "cancel-planning", Effect: Unknown, Submitted: true})
	return index, true, nil
}

func (p *managedRemotePlan) finishCancel(index int, response *http.Response, err error) {
	if err == nil && response != nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
		err = failure(ErrProtocol, "plan-cancel-response", rest.ErrRESTError)
	}
	p.scope.mu.Lock()
	effect := &p.scope.data.effects[index]
	effect.Complete = err == nil
	effect.Err = failureIf(err)
	if err == nil {
		effect.Effect = Acknowledged
	} else if response != nil && response.StatusCode >= 400 && response.StatusCode < 500 {
		effect.Effect = Rejected
	}
	p.err, p.done = err, true
	p.scope.mu.Unlock()
	p.scope.noteCleanup(err)
}
