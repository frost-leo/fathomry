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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

func TestManagedRemotePlansKeepTheirOwnCredentialsAfterReplanning(t *testing.T) {
	peer, options := newPeer(t)
	setup := bindManagedFixture(t, managedSettingsFor(options), 1)
	record := makeBatch(t, 0, 17, "remote")
	var data sdk.DataFile
	setup.run(t, "setup", func(s *ManagedSession) error {
		if _, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data")); err != nil {
			return err
		}
		tx, err := stageManagedAppend(s, "data", record)
		if err != nil {
			return err
		}
		tbl, err := tx.Commit()
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(ManagedScanOptions{Planning: table.ScanPlanningLocal})
		if err != nil {
			return err
		}
		tasks, err := scan.PlanFiles()
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return fmt.Errorf("fixture must have one data file")
		}
		data = tasks[0].File
		return nil
	})
	if err := setup.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	var plans, cancels atomic.Int32
	var expectedKey atomic.Value
	expectedKey.Store("test-access")
	peer.mu.Lock()
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case strings.HasSuffix(r.URL.Path, "/config"):
			writeJSON(w, map[string]any{"defaults": map[string]string{"prefix": "warehouse"}, "endpoints": []string{
				"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
				"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
				"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
				"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
				"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
			}})
			return true
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/plan"):
			id := fmt.Sprintf("plan-%d", plans.Add(1))
			task := rest.RESTFileScanTask{DataFile: &rest.RESTDataFile{RESTContentFile: rest.RESTContentFile{
				SpecID: int(data.SpecID()), Partition: []json.RawMessage{}, Content: "data", FilePath: data.FilePath(), FileFormat: "PARQUET", FileSizeInBytes: data.FileSizeBytes(), RecordCount: data.Count(),
			}}}
			writeJSON(w, map[string]any{"status": "completed", "plan-id": id, "file-scan-tasks": []rest.RESTFileScanTask{task},
				"storage-credentials": []any{map[string]any{"prefix": options.Location, "config": map[string]string{"s3.access-key-id": id, "s3.secret-access-key": "synthetic"}}},
			})
			return true
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/plan/"):
			cancels.Add(1)
			w.WriteHeader(204)
			return true
		case strings.HasPrefix(r.URL.Path, "/fixture/"):
			w.Header().Set("Connection", "close")
			if !strings.Contains(r.Header.Get("Authorization"), "Credential="+expectedKey.Load().(string)+"/") {
				t.Error("replanning rebound an earlier plan's storage identity")
			}
		}
		return false
	}
	peer.mu.Unlock()
	settings := managedSettingsFor(options)
	settings.VendedCredentials = true
	f := bindManagedFixture(t, settings, 1)
	f.run(t, "remote-credentials", func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", "data"})
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(ManagedScanOptions{Planning: table.ScanPlanningRemote, BatchSize: 3})
		if err != nil {
			return err
		}
		first, err := scan.Plan()
		if err != nil {
			return err
		}
		second, err := scan.Plan()
		if err != nil {
			return err
		}
		if err = scan.Close(); err != nil {
			return err
		}
		if cancels.Load() != 0 {
			return fmt.Errorf("parent scan closed retained plans")
		}
		for index, plan := range []*ManagedPlan{first, second} {
			expectedKey.Store(fmt.Sprintf("plan-%d", index+1))
			actual := map[int64]cell{}
			summary, err := plan.Records(func(record arrow.RecordBatch) (bool, error) {
				ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
				for row := range ids.Len() {
					if _, exists := actual[ids.Value(row)]; exists {
						return false, fmt.Errorf("duplicated remote row")
					}
					actual[ids.Value(row)] = cell{value: values.Value(row), valid: values.IsValid(row)}
				}
				return true, nil
			})
			if err != nil {
				return err
			}
			if !summary.Complete || !reflect.DeepEqual(actual, expectedRows(0, 17, "remote")) {
				return fmt.Errorf("remote exact data mismatch")
			}
			if index == 0 {
				if err = plan.Close(); err != nil {
					return err
				}
				if cancels.Load() != 1 {
					return fmt.Errorf("first plan did not release independently")
				}
			}
		}
		return nil
	})
	if plans.Load() != 2 || cancels.Load() != 2 {
		t.Fatal("ignored remote plan owner was not joined/canceled")
	}
}

func installManagedRemotePeer(peer *testPeer, hook func(http.ResponseWriter, *http.Request) bool) {
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/catalog/v1/config" {
			writeJSON(w, map[string]any{"defaults": map[string]string{"prefix": "warehouse"}, "endpoints": []string{
				"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
				"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
				"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
				"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
			}})
			return true
		}
		return hook(w, r)
	}
}

func TestManagedRemotePlanReservedCleanupAndFailure(t *testing.T) {
	for _, mode := range []string{"ignored", "ignored-new-connection", "explicit", "cleanup-refused", "poll-exhausted"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			var plans, polls, cancels atomic.Int32
			installManagedRemotePeer(peer, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/plan") {
					plans.Add(1)
					if mode == "ignored-new-connection" {
						w.Header().Set("Connection", "close")
					}
					writeJSON(w, map[string]any{"status": "submitted", "plan-id": "../opaque-plan"})
					return true
				}
				if strings.Contains(r.URL.Path, "/plan/") {
					if r.Method == http.MethodDelete {
						cancels.Add(1)
						if mode == "cleanup-refused" {
							catalogError(w, 503)
						} else {
							w.WriteHeader(204)
						}
					} else {
						polls.Add(1)
						writeJSON(w, map[string]any{"status": "submitted"})
					}
					return true
				}
				return false
			})
			settings := managedSettingsFor(options)
			settings.MaxFileOps = 1
			settings.MaxOperations = 1
			if mode == "poll-exhausted" {
				settings.MaxFileOps = 3
				settings.MaxOperations = 2
			}
			f := bindManagedFixture(t, settings, 1)
			result, err := f.outcome(t, "remote-reservation", func(s *ManagedSession) error {
				planned, err := s.PlanTableScan([]string{"isolated", "data"}, rest.PlanTableScanRequest{})
				if err != nil {
					return err
				}
				if planned.PlanID == nil {
					return fmt.Errorf("native plan ID missing")
				}
				switch mode {
				case "explicit":
					if err = s.CancelPlanning([]string{"isolated", "data"}, *planned.PlanID); err != nil {
						return err
					}
					return s.CancelPlanning([]string{"isolated", "data"}, *planned.PlanID)
				case "poll-exhausted":
					_, err = s.WaitForPlan([]string{"isolated", "data"}, *planned.PlanID, rest.WaitForPlanOptions{MinDelay: time.Millisecond, MaxDelay: time.Millisecond, MaxRetries: 1})
					return err
				}
				return nil
			})
			switch mode {
			case "cleanup-refused":
				if !errors.Is(err, rest.ErrRESTError) {
					t.Fatalf("discarded native cleanup failure: %s", causes(err))
				}
			case "poll-exhausted":
				if !errors.Is(err, rest.ErrPlanPollExhausted) || polls.Load() != 2 {
					t.Fatalf("poll/cancel fidelity: %s polls=%d", causes(err), polls.Load())
				}
			default:
				if err != nil {
					t.Fatal(causes(err))
				}
			}
			if plans.Load() != 1 || cancels.Load() != 1 || !result.Released() {
				t.Fatalf("unjoined or repeated plan cleanup: plans=%d cancels=%d released=%t", plans.Load(), cancels.Load(), result.Released())
			}
			found := false
			for _, effect := range result.Effects() {
				if effect.Operation == "cancel-planning" {
					found = true
					if !effect.Submitted || effect.Complete != (mode != "cleanup-refused") {
						t.Error("false cancellation evidence")
					}
				}
			}
			if !found {
				t.Fatal("plan cleanup missing from independent evidence")
			}
		})
	}
}

func TestManagedRemotePlanPaginationAndOrigin(t *testing.T) {
	peer, options := newPeer(t)
	var polls, fetches, cancels atomic.Int32
	installManagedRemotePeer(peer, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/plan"):
			writeJSON(w, map[string]any{"status": "submitted", "plan-id": "owned"})
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/plan/"):
			if polls.Add(1) == 1 {
				writeJSON(w, map[string]any{"status": "submitted"})
			} else {
				writeJSON(w, map[string]any{"status": "completed", "plan-tasks": []string{"first"}})
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tasks"):
			if fetches.Add(1) == 1 {
				writeJSON(w, map[string]any{"plan-tasks": []string{"second"}})
			} else {
				writeJSON(w, map[string]any{"file-scan-tasks": []any{}})
			}
		case r.Method == http.MethodDelete:
			cancels.Add(1)
			w.WriteHeader(204)
		default:
			return false
		}
		return true
	})
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	f.run(t, "remote-tasks", func(s *ManagedSession) error {
		response, err := s.PlanTableScan([]string{"isolated", "data"}, rest.PlanTableScanRequest{})
		if err != nil {
			return err
		}
		completed, err := s.WaitForPlan([]string{"isolated", "data"}, *response.PlanID, rest.WaitForPlanOptions{MinDelay: time.Millisecond, MaxDelay: time.Millisecond})
		if err != nil || len(completed.PlanTasks) != 1 {
			return fmt.Errorf("asynchronous plan incomplete: %w", err)
		}
		next, err := s.FetchScanTasks([]string{"isolated", "data"}, rest.FetchScanTasksRequest{PlanTask: completed.PlanTasks[0]})
		if err != nil || len(next.PlanTasks) != 1 {
			return fmt.Errorf("first task frontier incomplete: %w", err)
		}
		_, err = s.FetchScanTasks([]string{"isolated", "data"}, rest.FetchScanTasksRequest{PlanTask: next.PlanTasks[0]})
		return err
	})
	if polls.Load() != 2 || fetches.Load() != 2 || cancels.Load() != 1 {
		t.Fatal("remote task expansion or cleanup not executed")
	}
	for _, operation := range []string{"fetch-plan", "fetch-task", "cancel"} {
		_, err := f.outcome(t, operation, func(s *ManagedSession) error {
			switch operation {
			case "fetch-plan":
				_, err := s.FetchPlanningResult([]string{"isolated", "data"}, "owned", rest.FetchPlanningResultOptions{})
				return err
			case "fetch-task":
				_, err := s.FetchScanTasks([]string{"isolated", "data"}, rest.FetchScanTasksRequest{PlanTask: "first"})
				return err
			default:
				return s.CancelPlanning([]string{"isolated", "data"}, "owned")
			}
		})
		if !errors.Is(err, ErrAuthority) {
			t.Fatal("plan handle silently adopted by a different family")
		}
	}
	if polls.Load() != 2 || fetches.Load() != 2 || cancels.Load() != 1 {
		t.Fatal("origin refusal occurred after remote I/O")
	}
}
