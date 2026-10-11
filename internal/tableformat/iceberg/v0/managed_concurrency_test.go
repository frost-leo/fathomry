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
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestManagedConcurrentAppendBoundedNativeRetry(t *testing.T) {
	for _, retries := range []int{0, 1} {
		t.Run(fmt.Sprint(retries), func(t *testing.T) {
			peer, options := newPeer(t)
			settings := managedSettingsFor(options)
			settings.MaxCommitRetries = retries
			f := bindManagedFixture(t, settings, 3)
			peer.mu.Lock()
			peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/fixture/") {
					peer.mu.Lock()
					delete(peer.objects, strings.TrimPrefix(r.URL.Path, "/fixture/"))
					peer.mu.Unlock()
					w.WriteHeader(204)
					return true
				}
				return false
			}
			peer.mu.Unlock()
			f.run(t, "setup", func(s *ManagedSession) error {
				_, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"commit.retry.num-retries": "4", "commit.retry.min-wait-ms": "1", "commit.retry.max-wait-ms": "1"}))
				return err
			})
			records := []arrow.RecordBatch{makeBatch(t, 100, 9, "first"), makeBatch(t, 200, 9, "second")}
			loaded := make(chan struct{}, 2)
			commit := make(chan struct{})
			var wait sync.WaitGroup
			results := make([]invocation.Result[ManagedResult], 2)
			for i := range records {
				wait.Add(1)
				go func(index int) {
					defer wait.Done()
					receipt, err := f.client.Use(deadline(t), correlation(fmt.Sprintf("writer-%d", index)), func(s *ManagedSession) error {
						tbl, err := s.LoadTable([]string{"isolated", "data"})
						if err != nil {
							return err
						}
						tx, err := tbl.NewTransaction("")
						if err != nil {
							return err
						}
						reader, _ := array.NewRecordReader(records[index].Schema(), []arrow.RecordBatch{records[index]})
						err = tx.Append(reader, nil)
						reader.Release()
						if err != nil {
							return err
						}
						loaded <- struct{}{}
						<-commit
						_, err = tx.Commit()
						return err
					})
					if err != nil {
						t.Error(err)
						return
					}
					results[index], err = receipt.WaitReleased(deadline(t))
					if err != nil {
						t.Error(err)
					}
				}(i)
			}
			for range records {
				select {
				case <-loaded:
				case <-deadline(t).Done():
					t.Fatal("writers failed to stage")
				}
			}
			close(commit)
			wait.Wait()
			expected := map[int64]cell{}
			success := 0
			for i, result := range results {
				if result.Err() == nil {
					success++
					label := "first"
					start := 100
					if i == 1 {
						label = "second"
						start = 200
					}
					for id, value := range expectedRows(start, 9, label) {
						expected[id] = value
					}
				} else if !errors.Is(result.Err(), table.ErrCommitFailed) {
					t.Fatalf("unexpected native result: %s", causes(result.Err()))
				}
				delivery, err := f.inbox.Next(deadline(t))
				if err != nil {
					t.Fatal(err)
				}
				_ = delivery.Release()
			}
			if success != 1+retries {
				t.Fatalf("successes=%d retries=%d", success, retries)
			}
			peer.mu.Lock()
			attempts := peer.commits
			peer.mu.Unlock()
			if attempts != 2+retries {
				t.Fatalf("attempts=%d retries=%d", attempts, retries)
			}
			f.run(t, "read", func(s *ManagedSession) error {
				tbl, err := s.LoadTable([]string{"isolated", "data"})
				if err != nil {
					return err
				}
				scan, err := tbl.Scan(ManagedScanOptions{})
				if err != nil {
					return err
				}
				managedExactRows(t, scan, expected)
				return nil
			})
		})
	}
}
