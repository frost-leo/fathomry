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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

func TestManagedMultiTableDTOFactsAndReferenceCopies(t *testing.T) {
	peer, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	batch := makeBatch(t, 0, 5, "dto")
	f.run(t, "setup", func(s *ManagedSession) error {
		for _, name := range []string{"first", "second"} {
			if _, err := s.CreateTable([]string{"isolated", name}, testSchema(), catalog.WithLocation(options.Location+name)); err != nil {
				return err
			}
			tx, err := stageManagedAppend(s, name, batch)
			if err != nil {
				return err
			}
			if _, err = tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	})
	submissions := installManagedMultiTablePeer(peer, "success")
	result := f.run(t, "dto-commit", func(s *ManagedSession) error {
		var commits []table.TableCommit
		for _, name := range []string{"first", "second"} {
			tbl, err := s.LoadTable([]string{"isolated", name})
			if err != nil {
				return err
			}
			commits = append(commits, table.TableCommit{
				Identifier: tbl.Identifier(), Requirements: []table.Requirement{table.AssertTableUUID(tbl.table.Metadata().TableUUID())},
				Updates: []table.Update{table.NewSetSnapshotRefUpdate("saved", tbl.table.CurrentSnapshot().SnapshotID, table.TagRef, 60000, 0, 0)},
			})
		}
		return s.CommitTableChanges(commits)
	})
	var index int
	for i, effect := range result.Effects() {
		if effect.Operation == "commit-table-changes" {
			index = i
		}
	}
	facts := result.Effects()[index]
	if facts.Effect != Acknowledged || !facts.Submitted || !facts.Staged || len(facts.Tables) != 2 || submissions.Load() != 1 {
		t.Fatal("DTO route omitted per-table facts")
	}
	for _, fact := range facts.Tables {
		if len(fact.References) != 1 || fact.References[0].Name != "saved" || fact.References[0].Before != nil || fact.References[0].After == nil {
			t.Fatal("DTO route lost added reference identity")
		}
		if fact.References[0].After.SnapshotID != fact.IntendedSnapshot || fact.Effect != Acknowledged {
			t.Fatal("reference intent does not match submitted base")
		}
		fact.References[0].After.SnapshotID = -1
		if fact.References[0].After.MaxRefAgeMs != nil {
			*fact.References[0].After.MaxRefAgeMs = -1
		}
	}
	for _, fact := range result.Effects()[index].Tables {
		if fact.References[0].After.SnapshotID < 0 || fact.References[0].After.MaxRefAgeMs == nil || *fact.References[0].After.MaxRefAgeMs != 60000 {
			t.Fatal("independent evidence reference containers alias caller")
		}
	}
}

func TestManagedMultiTableReloadCannotInventCommitIdentity(t *testing.T) {
	for _, replaceUUID := range []bool{false, true} {
		t.Run(fmt.Sprint(replaceUUID), func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			f.run(t, "setup", func(s *ManagedSession) error {
				for _, name := range []string{"first", "second"} {
					if _, err := s.CreateTable([]string{"isolated", name}, testSchema(), catalog.WithLocation(options.Location+name)); err != nil {
						return err
					}
				}
				return nil
			})
			submissions := installManagedMultiTablePeer(peer, "success")
			peer.mu.Lock()
			handler := peer.hook
			replaced := false
			peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/tables/first") && submissions.Load() > 0 {
					peer.mu.Lock()
					if !replaced {
						replaced = true
						base, err := table.ParseMetadataBytes(peer.tables["first"])
						if err == nil {
							var next table.Metadata
							if replaceUUID {
								next, err = table.NewMetadata(testSchema(), sdk.UnpartitionedSpec, table.UnsortedSortOrder, options.Location+"first", nil)
							} else {
								snapshot := *base.CurrentSnapshot()
								parent := snapshot.SnapshotID
								snapshot.SnapshotID += 100
								snapshot.ParentSnapshotID = &parent
								snapshot.SequenceNumber++
								next, err = table.UpdateTableMetadata(base, []table.Update{table.NewAddSnapshotUpdate(&snapshot), table.NewSetSnapshotRefUpdate(table.MainBranch, snapshot.SnapshotID, table.BranchRef, 0, 0, 0)}, peer.locations["first"])
							}
							if err == nil {
								raw, _ := json.Marshal(next)
								peer.persist("first", raw)
							}
						}
						if err != nil {
							peer.mu.Unlock()
							catalogError(w, 500)
							return true
						}
					}
					peer.mu.Unlock()
				}
				return handler(w, r)
			}
			peer.mu.Unlock()
			batch := makeBatch(t, 0, 5, "reload")
			result, err := f.outcome(t, "reload", func(s *ManagedSession) error {
				first, err := stageManagedAppend(s, "first", batch)
				if err != nil {
					return err
				}
				second, err := stageManagedAppend(s, "second", batch)
				if err != nil {
					return err
				}
				tables, err := s.CommitTransactionAndReload(first, second)
				if replaceUUID && len(tables) == 2 && tables[0] != nil {
					return fmt.Errorf("reloaded a replacement table as original generation")
				}
				return err
			})
			if replaceUUID {
				if !errors.Is(err, ErrAuthority) {
					t.Fatalf("replacement table accepted: %s", causes(err))
				}
			} else if err != nil {
				t.Fatal(causes(err))
			}
			var intended, reloaded int64
			for _, effect := range result.Effects() {
				if effect.Operation == "commit-transaction" {
					if effect.Effect != Acknowledged || !effect.Complete {
						t.Fatal("reload failure rewrote acknowledged commit fact")
					}
					intended = effect.Tables[0].IntendedSnapshot
				}
				if effect.Operation == "reload-transaction-table" && effect.Reloaded && reloaded == 0 {
					reloaded = effect.ReloadedSnapshot
					if effect.CommittedSnapshot != 0 || effect.Effect != NotSubmitted {
						t.Fatal("reload was presented as a second confirmed commit")
					}
				}
			}
			if intended == 0 || !replaceUUID && reloaded != intended+100 {
				t.Fatal("later head was confused with submitted identity")
			}
		})
	}
}

func installManagedMultiTablePeer(peer *testPeer, mode string) *atomic.Int32 {
	submissions := new(atomic.Int32)
	peer.mu.Lock()
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/transactions/commit") {
			return false
		}
		submissions.Add(1)
		var input struct {
			Changes []struct {
				Identifier struct {
					Namespace []string
					Name      string
				}
				Requirements table.Requirements
				Updates      table.Updates
			} `json:"table-changes"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			catalogError(w, 400)
			return true
		}
		peer.mu.Lock()
		defer peer.mu.Unlock()
		prospective := make(map[string][]byte, len(input.Changes))
		for _, change := range input.Changes {
			if len(change.Identifier.Namespace) != 1 || change.Identifier.Namespace[0] != "isolated" {
				catalogError(w, 400)
				return true
			}
			meta, err := table.ParseMetadataBytes(peer.tables[change.Identifier.Name])
			if err != nil {
				catalogError(w, 404)
				return true
			}
			for _, requirement := range change.Requirements {
				if err = requirement.Validate(meta); err != nil {
					catalogError(w, 409)
					return true
				}
			}
			next, err := table.UpdateTableMetadata(meta, change.Updates, peer.locations[change.Identifier.Name])
			if err != nil {
				catalogError(w, 400)
				return true
			}
			prospective[change.Identifier.Name], _ = json.Marshal(next)
		}
		if mode == "conflict" {
			catalogError(w, 409)
			return true
		}
		for name, raw := range prospective {
			peer.persist(name, raw)
		}
		if mode == "lost" {
			catalogError(w, 503)
		} else {
			w.WriteHeader(204)
		}
		return true
	}
	peer.mu.Unlock()
	return submissions
}

func stageManagedAppend(s *ManagedSession, name string, batch arrow.RecordBatch) (*ManagedTransaction, error) {
	tbl, err := s.LoadTable([]string{"isolated", name})
	if err != nil {
		return nil, err
	}
	tx, err := tbl.NewTransaction("")
	if err != nil {
		return nil, err
	}
	reader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	if err = tx.Append(reader, nil); err != nil {
		return nil, err
	}
	return tx, nil
}

func TestManagedMultiTableCommitFactsAndNoResend(t *testing.T) {
	for _, mode := range []string{"success", "conflict", "lost"} {
		t.Run(mode, func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			f.run(t, "setup", func(s *ManagedSession) error {
				for _, name := range []string{"first", "second"} {
					if _, err := s.CreateTable([]string{"isolated", name}, testSchema(), catalog.WithLocation(options.Location+name)); err != nil {
						return err
					}
				}
				return nil
			})
			submissions := installManagedMultiTablePeer(peer, mode)
			first, second := makeBatch(t, 0, 7, "first"), makeBatch(t, 100, 9, "second")
			result, err := f.outcome(t, "multi-table", func(s *ManagedSession) error {
				a, err := stageManagedAppend(s, "first", first)
				if err != nil {
					return err
				}
				b, err := stageManagedAppend(s, "second", second)
				if err != nil {
					return err
				}
				commitErr := s.CommitTransaction(a, b)
				if _, err = a.TableCommit(); !errors.Is(err, ErrAuthority) {
					return fmt.Errorf("consumed transaction retained commit authority: %w", err)
				}
				if _, err = b.TableCommit(); !errors.Is(err, ErrAuthority) {
					return fmt.Errorf("second consumed transaction retained commit authority: %w", err)
				}
				return commitErr
			})
			expectedEffect := Acknowledged
			switch mode {
			case "success":
				if err != nil {
					t.Fatal(causes(err))
				}
			case "conflict":
				expectedEffect = Rejected
				if !errors.Is(err, rest.ErrCommitFailed) {
					t.Fatalf("native conflict missing: %s", causes(err))
				}
			case "lost":
				expectedEffect = Unknown
				if !errors.Is(err, rest.ErrCommitStateUnknown) {
					t.Fatalf("unknown commit missing: %s", causes(err))
				}
			}
			var commit *ManagedEffect
			for _, effect := range result.Effects() {
				if effect.Operation == "commit-transaction" {
					commit = &effect
				}
			}
			if commit == nil || !commit.Staged || !commit.Submitted || commit.Effect != expectedEffect || len(commit.Tables) != 2 {
				t.Fatal("missing aggregate or per-table commit facts")
			}
			for _, fact := range commit.Tables {
				if fact.TableUUID == "" || fact.BaseSnapshot != 0 || fact.IntendedSnapshot == 0 || fact.Effect != expectedEffect {
					t.Fatal("per-table identity changed")
				}
			}
			if submissions.Load() != 1 {
				t.Fatal("multi-table uncertainty retried")
			}
			f.run(t, "read-back", func(s *ManagedSession) error {
				for index, name := range []string{"first", "second"} {
					tbl, err := s.LoadTable([]string{"isolated", name})
					if err != nil {
						return err
					}
					scan, err := tbl.Scan(ManagedScanOptions{})
					if err != nil {
						return err
					}
					expected := map[int64]cell{}
					if mode != "conflict" {
						if index == 0 {
							expected = expectedRows(0, 7, "first")
						} else {
							expected = expectedRows(100, 9, "second")
						}
					}
					managedExactRows(t, scan, expected)
				}
				return nil
			})
		})
	}
}
