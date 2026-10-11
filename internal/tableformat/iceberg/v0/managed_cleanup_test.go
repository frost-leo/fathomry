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
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedOrphanCustomCallbackAndDryRun(t *testing.T) {
	for _, mode := range []string{"normal", "error", "panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			peer.objects["owned/data/orphan-one"] = []byte("one")
			peer.objects["owned/data/orphan-two"] = []byte("two")
			nativeDeletes := installManagedMaintenancePeer(peer, nil)
			settings := managedSettingsFor(options)
			settings.Workers = 2
			f := bindManagedFixture(t, settings, 1)
			var calls atomic.Int32
			marker := errors.New("custom deletion failure")
			result, err := f.outcome(t, "custom-delete", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
				if err != nil {
					return err
				}
				plan, err := tbl.PlanOrphanFiles(OrphanOptions{OlderThan: 72 * time.Hour})
				if err != nil {
					return err
				}
				custom := func(uri string) error {
					calls.Add(1)
					if !strings.HasPrefix(uri, options.Location+"data/orphan-") {
						return fmt.Errorf("uninspected callback target")
					}
					switch mode {
					case "error":
						return marker
					case "panic":
						panic(marker)
					case "goexit":
						runtime.Goexit()
					}
					peer.mu.Lock()
					delete(peer.objects, strings.TrimPrefix(uri, "s3://fixture/"))
					peer.mu.Unlock()
					return nil
				}
				dry, err := tbl.ExecuteOrphanCleanupWithOptions(plan, OrphanExecutionOptions{DryRun: true, Delete: custom})
				if err != nil {
					return err
				}
				if len(dry.OrphanFiles) != 2 || len(dry.DeletedFiles) != 0 || calls.Load() != 0 {
					return fmt.Errorf("dry-run executed or consumed a deletion callback")
				}
				actual, err := tbl.ExecuteOrphanCleanupWithOptions(plan, OrphanExecutionOptions{Quiescent: true, Delete: custom})
				if mode == "normal" && len(actual.DeletedFiles) != 2 {
					return fmt.Errorf("custom acknowledgements missing")
				}
				return err
			})
			if mode == "normal" {
				if err != nil {
					t.Fatal(causes(err))
				}
			} else if mode == "goexit" {
				if !errors.Is(err, ErrOperation) {
					t.Fatalf("callback exit hidden: %s", causes(err))
				}
			} else if !errors.Is(err, marker) {
				t.Fatalf("callback error identity lost: %s", causes(err))
			}
			if !result.Released() || calls.Load() != 2 || nativeDeletes.Load() != 0 {
				t.Fatal("callback ownership or alternate native deletion changed")
			}
			facts := 0
			for _, event := range result.Files() {
				if event.Operation == "custom-delete" {
					facts++
					if mode != "normal" && event.Effect == Acknowledged {
						t.Fatal("failed callback claimed a deletion acknowledgement")
					}
				}
			}
			if facts != 4 {
				t.Fatalf("custom deletion evidence=%d", facts)
			}
		})
	}
}

func installManagedMaintenancePeer(peer *testPeer, refuse func(string) bool) *atomic.Int32 {
	deletes := new(atomic.Int32)
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/fixture/") {
			deletes.Add(1)
			key := strings.TrimPrefix(r.URL.Path, "/fixture/")
			if refuse != nil && refuse(key) {
				w.WriteHeader(503)
				_, _ = w.Write([]byte("<Error><Code>ServiceUnavailable</Code></Error>"))
				return true
			}
			peer.mu.Lock()
			delete(peer.objects, key)
			peer.mu.Unlock()
			w.WriteHeader(204)
			return true
		}
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			return false
		}
		type object struct {
			Key          string
			Size         int64
			LastModified string
		}
		type directory struct{ Prefix string }
		page := struct {
			XMLName        xml.Name `xml:"ListBucketResult"`
			IsTruncated    bool
			Contents       []object
			CommonPrefixes []directory
		}{}
		prefix := r.URL.Query().Get("prefix")
		peer.mu.Lock()
		directories := map[string]bool{}
		for key, data := range peer.objects {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			rest := strings.TrimPrefix(key, prefix)
			if before, _, nested := strings.Cut(rest, "/"); nested {
				directories[prefix+before+"/"] = true
				continue
			}
			modified := time.Now()
			if strings.Contains(key, "/orphan-") {
				modified = modified.Add(-100 * time.Hour)
			}
			page.Contents = append(page.Contents, object{key, int64(len(data)), modified.UTC().Format(time.RFC3339)})
		}
		peer.mu.Unlock()
		for prefix := range directories {
			page.CommonPrefixes = append(page.CommonPrefixes, directory{prefix})
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(page)
		return true
	}
	return deletes
}

func TestManagedPurgeFilesRequiresDropAndQuiescence(t *testing.T) {
	for _, mode := range []string{"success", "active", "not-quiescent", "partial"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			peer.objects["outside/keep"] = []byte("unrelated")
			deletes := installManagedMaintenancePeer(peer, func(key string) bool { return mode == "partial" && strings.HasSuffix(key, ".parquet") })
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			batch := makeBatch(t, 0, 7, "purge")
			result, err := f.outcome(t, "purge-files", func(s *ManagedSession) error {
				if _, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data")); err != nil {
					return err
				}
				tx, err := stageManagedAppend(s, "data", batch)
				if err != nil {
					return err
				}
				tbl, err := tx.Commit()
				if err != nil {
					return err
				}
				if mode != "active" {
					if err = s.DropTable(tbl.Identifier()); err != nil {
						return err
					}
				}
				return tbl.PurgeFiles(mode != "not-quiescent")
			})
			if mode == "active" || mode == "not-quiescent" {
				if !errors.Is(err, ErrAuthority) || deletes.Load() != 0 {
					t.Fatal("purge crossed a refusing boundary")
				}
				return
			}
			peer.mu.Lock()
			var remaining []string
			for key := range peer.objects {
				remaining = append(remaining, key)
			}
			peer.mu.Unlock()
			slices.Sort(remaining)
			if mode == "success" {
				if err != nil || !reflect.DeepEqual(remaining, []string{"outside/keep"}) {
					t.Fatalf("native bounded purge: %s remaining=%d", causes(err), len(remaining))
				}
			} else {
				if err == nil || len(remaining) != 2 || remaining[0] != "outside/keep" || !strings.HasSuffix(remaining[1], ".parquet") {
					t.Fatal("partial physical deletion was hidden or unrelated objects changed")
				}
				unknown := false
				for _, file := range result.Files() {
					if file.Operation == "delete" && file.Err != nil && file.Effect == Unknown {
						unknown = true
					}
				}
				if !unknown {
					t.Fatal("unknown object deletion missing from evidence")
				}
			}
			if deletes.Load() == 0 {
				t.Fatal("purge did not execute physical deletion")
			}
		})
	}
}

func TestManagedOrphanPlanRefusalsPartialDeleteAndNoReplay(t *testing.T) {
	for _, mode := range []string{"success", "not-quiescent", "stale-base", "partial", "repeat"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			peer.objects["owned/data/orphan-one"] = []byte("one")
			peer.objects["owned/data/orphan-two"] = []byte("two")
			peer.objects["outside/keep"] = []byte("unrelated")
			deletes := installManagedMaintenancePeer(peer, func(key string) bool { return mode == "partial" && strings.HasSuffix(key, "/orphan-one") })
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			deleted := 0
			_, err := f.outcome(t, "orphan-execution", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
				if err != nil {
					return err
				}
				plan, err := tbl.PlanOrphanFiles(OrphanOptions{OlderThan: 72 * time.Hour})
				if err != nil {
					return err
				}
				if len(plan.Files()) != 2 {
					return fmt.Errorf("native eligibility mismatch: %d", len(plan.Files()))
				}
				if mode == "stale-base" {
					if _, err = s.UpdateTable(tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(sdk.Properties{"concurrent": "writer"})}); err != nil {
						return err
					}
				}
				result, executeErr := tbl.ExecuteOrphanCleanup(plan, mode != "not-quiescent")
				deleted = len(result.DeletedFiles)
				if mode == "partial" || mode == "repeat" {
					before := deletes.Load()
					_, repeatErr := tbl.ExecuteOrphanCleanup(plan, true)
					if !errors.Is(repeatErr, ErrAuthority) || deletes.Load() != before {
						return fmt.Errorf("consumed cleanup plan retried physical deletion")
					}
				}
				return executeErr
			})
			switch mode {
			case "not-quiescent", "stale-base":
				if !errors.Is(err, ErrAuthority) || deletes.Load() != 0 {
					t.Fatalf("orphan refusal happened after deletion: %s", causes(err))
				}
			case "partial":
				if err == nil || deleted != 1 || deletes.Load() != 2 {
					t.Fatalf("partial orphan result lost: %s deleted=%d calls=%d", causes(err), deleted, deletes.Load())
				}
			case "repeat":
				if !errors.Is(err, ErrAuthority) || deleted != 2 || deletes.Load() != 2 {
					t.Fatalf("orphan cleanup replay: %s", causes(err))
				}
			default:
				if err != nil || deleted != 2 || deletes.Load() != 2 {
					t.Fatalf("orphan execution: %s", causes(err))
				}
			}
			peer.mu.Lock()
			unrelated := string(peer.objects["outside/keep"])
			peer.mu.Unlock()
			if unrelated != "unrelated" {
				t.Fatal("orphan deletion crossed the owned root")
			}
		})
	}
}
