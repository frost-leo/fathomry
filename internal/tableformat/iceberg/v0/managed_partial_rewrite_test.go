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
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedPartialRewriteCommitFacts(t *testing.T) {
	for _, mode := range []string{"success", "second-response-lost", "second-conflict", "conflict-cleanup-failed"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			deletes := installManagedMaintenancePeer(peer, func(string) bool { return mode == "conflict-cleanup-failed" })
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			f.run(t, "partial-setup", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
				if err != nil {
					return err
				}
				for index := range 2 {
					input := makeBatch(t, index*10, 3, "partial")
					reader, err := array.NewRecordReader(input.Schema(), []arrow.RecordBatch{input})
					if err != nil {
						return err
					}
					tx, err := tbl.NewTransaction("")
					if err == nil {
						err = tx.Append(reader, nil)
					}
					reader.Release()
					if err != nil {
						return err
					}
					tbl, err = tx.Commit()
					if err != nil {
						return err
					}
				}
				return nil
			})
			var calls atomic.Int32
			peer.mu.Lock()
			hook := peer.hook
			peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tables/data") {
					attempt := calls.Add(1)
					if attempt == 2 && (mode == "second-conflict" || mode == "conflict-cleanup-failed") {
						catalogError(w, http.StatusConflict)
						return true
					}
				}
				return hook(w, r)
			}
			peer.afterCommit = func(w http.ResponseWriter, _ *http.Request, _ []byte) bool {
				if mode == "second-response-lost" && calls.Load() == 2 {
					w.WriteHeader(503)
					_, _ = w.Write([]byte("ambiguous-response"))
					return true
				}
				return false
			}
			peer.mu.Unlock()
			var rewritten *ManagedRewriteResult
			receipt, err := f.client.Use(deadline(t), correlation(mode), func(s *ManagedSession) error {
				tbl, err := s.LoadTable([]string{"isolated", "data"})
				if err != nil {
					return err
				}
				scan, err := tbl.Scan(ManagedScanOptions{})
				if err != nil {
					return err
				}
				tasks, err := scan.PlanFiles()
				if err != nil {
					return err
				}
				groups := make([]table.CompactionTaskGroup, len(tasks))
				for index, task := range tasks {
					groups[index] = table.CompactionTaskGroup{PartitionKey: fmt.Sprint(index), Tasks: []table.FileScanTask{task}, TotalSizeBytes: task.File.FileSizeBytes()}
				}
				rewritten, err = tbl.RewriteDataFiles(groups, table.RewriteDataFilesOptions{PartialProgress: true, MaxCommits: 2, MaxFailedCommits: 1})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := receipt.WaitReleased(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			_ = delivery.Release()
			if rewritten == nil || calls.Load() != 2 {
				t.Fatal("partial rewrite not exercised")
			}
			if mode == "success" && (outcome.Err() != nil || rewritten.RewrittenGroups != 2) {
				t.Fatal("successful partial rewrite", causes(outcome.Err()))
			}
			if mode != "success" && rewritten.RewrittenGroups != 1 {
				t.Fatal("acknowledged progress discarded")
			}
			if mode == "second-response-lost" && (outcome.Err() == nil || deletes.Load() != 0) {
				t.Fatal("unknown commit succeeded or deleted its outputs")
			}
			if strings.Contains(mode, "conflict") {
				if len(rewritten.FailedGroups) != 1 || !errors.Is(rewritten.FailedGroups[0].Err, table.ErrCommitFailed) || deletes.Load() == 0 {
					t.Fatal("conflict result/cleanup missing")
				}
				if mode == "conflict-cleanup-failed" && (!errors.Is(outcome.Err(), table.ErrCommitFailed) || outcome.Outcome.Cleanup == nil) {
					t.Fatal("independent cleanup failure discarded", causes(outcome.Err()))
				}
			}
			acknowledged := 0
			var submissions []ManagedCommitSubmission
			for _, effect := range outcome.Outcome.Value.Effects() {
				for _, submission := range effect.Submissions {
					submissions = append(submissions, submission)
					if submission.Effect == Acknowledged && submission.CommittedSnapshot != 0 {
						if submission.CommittedSnapshot != submission.IntendedSnapshot || submission.MetadataLocation == "" || submission.TableUUID == "" {
							t.Fatal("acknowledgement lost exact intent")
						}
						acknowledged++
					}
				}
			}
			if len(submissions) != 2 || submissions[1].BaseSnapshot != submissions[0].CommittedSnapshot {
				t.Fatal("durable partial-commit ancestry lost")
			}
			if mode == "second-response-lost" && (submissions[1].Effect != Unknown || submissions[1].CommittedSnapshot != 0) {
				t.Fatal("unknown partial batch became acknowledged")
			}
			if strings.Contains(mode, "conflict") && (submissions[1].Effect != Rejected || submissions[1].HTTPStatus != 409) {
				t.Fatal("partial conflict identity lost")
			}
			if acknowledged != rewritten.RewrittenGroups {
				t.Fatalf("confirmed per-commit identities=%d, durable rewrite groups=%d", acknowledged, rewritten.RewrittenGroups)
			}
			f.run(t, "partial-exact", func(s *ManagedSession) error {
				tbl, err := s.LoadTable([]string{"isolated", "data"})
				if err != nil {
					return err
				}
				scan, err := tbl.Scan(ManagedScanOptions{})
				if err != nil {
					return err
				}
				expected := expectedRows(0, 3, "partial")
				for id, value := range expectedRows(10, 3, "partial") {
					expected[id] = value
				}
				managedExactRows(t, scan, expected)
				return nil
			})
		})
	}
}
