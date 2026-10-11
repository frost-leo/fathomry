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

	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

func TestManagedStagedCreateAndCommitIdentity(t *testing.T) {
	for _, wrongUUID := range []bool{false, true} {
		t.Run(fmt.Sprint(wrongUUID), func(t *testing.T) {
			peer, options := newPeer(t)
			var staged table.Metadata
			var stageCalls, commitCalls atomic.Int32
			peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodPost || !strings.Contains(r.URL.Path, "/tables") {
					return false
				}
				if strings.HasSuffix(r.URL.Path, "/tables") {
					var input struct {
						Name, Location string
						Schema         *sdk.Schema
						Properties     sdk.Properties
						Stage          bool `json:"stage-create"`
					}
					if json.NewDecoder(r.Body).Decode(&input) != nil || !input.Stage {
						catalogError(w, 400)
						return true
					}
					meta, err := table.NewMetadata(input.Schema, sdk.UnpartitionedSpec, table.UnsortedSortOrder, input.Location, input.Properties)
					if err != nil {
						catalogError(w, 400)
						return true
					}
					staged = meta
					stageCalls.Add(1)
					writeJSON(w, map[string]any{"metadata": meta})
					return true
				}
				var input struct {
					Requirements table.Requirements
					Updates      table.Updates
				}
				if json.NewDecoder(r.Body).Decode(&input) != nil || staged == nil {
					catalogError(w, 400)
					return true
				}
				for _, requirement := range input.Requirements {
					if err := requirement.Validate(nil); err != nil {
						catalogError(w, 409)
						return true
					}
				}
				next, err := table.UpdateTableMetadata(staged, input.Updates, "")
				if err != nil {
					catalogError(w, 400)
					return true
				}
				raw, _ := json.Marshal(next)
				if wrongUUID {
					var object map[string]json.RawMessage
					_ = json.Unmarshal(raw, &object)
					object["table-uuid"], _ = json.Marshal(uuid.NewString())
					raw, _ = json.Marshal(object)
				}
				peer.mu.Lock()
				peer.persist("data", raw)
				reply := peer.tableResponse("data", raw)
				peer.mu.Unlock()
				commitCalls.Add(1)
				writeJSON(w, reply)
				return true
			}
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			result, err := f.outcome(t, "staged-create", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithStagedUpdates(table.NewSetPropertiesUpdate(sdk.Properties{"stage": "intent"})))
				if err != nil {
					return err
				}
				meta, err := tbl.Metadata()
				if err == nil && meta.Properties()["stage"] != "intent" {
					return fmt.Errorf("staged property intent lost")
				}
				return err
			})
			if stageCalls.Load() != 1 || commitCalls.Load() != 1 {
				t.Fatalf("staging protocol did not complete both phases: stage=%d commit=%d: %s", stageCalls.Load(), commitCalls.Load(), causes(err))
			}
			effect := result.Effects()[0]
			if wrongUUID {
				if !errors.Is(err, rest.ErrCommitStateUnknown) || effect.Effect != Unknown {
					t.Fatal("staged create confirmed the wrong UUID")
				}
			} else if err != nil || !effect.Staged || !effect.Submitted || effect.Effect != Acknowledged {
				t.Fatalf("staged create facts or validation: %s", causes(err))
			}
		})
	}
}

func TestManagedStagedAuthorityPrecedesBothPhases(t *testing.T) {
	peer, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	peer.mu.Lock()
	before := peer.requests
	peer.mu.Unlock()
	_, err := f.outcome(t, "invalid-stage", func(s *ManagedSession) error {
		_, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithStagedUpdates(table.NewSetPropertiesUpdate(sdk.Properties{"write.data.path": "s3://fixture/outside/"})))
		return err
	})
	peer.mu.Lock()
	after := peer.requests
	peer.mu.Unlock()
	if !errors.Is(err, ErrAuthority) || after != before {
		t.Fatalf("staged authority was checked after I/O: before=%d after=%d: %s", before, after, causes(err))
	}
}
