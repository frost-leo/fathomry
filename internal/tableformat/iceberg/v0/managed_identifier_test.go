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
	"fmt"
	"net/http"
	"strings"
	"testing"

	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedIdentifierDoesNotSelectControlEndpoint(t *testing.T) {
	for _, name := range []string{"tables", "views", "functions", "config", "metrics", "plan", "plans", "tasks"} {
		t.Run(name, func(t *testing.T) {
			_, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			result := f.run(t, "opaque-endpoint", func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", name}, testSchema(), catalog.WithLocation(options.Location+"data"))
				if err != nil {
					return err
				}
				tbl, err = s.LoadTable(tbl.Identifier())
				if err != nil {
					return err
				}
				tx, err := tbl.NewTransaction("")
				if err != nil {
					return err
				}
				if err = tx.SetProperties(sdk.Properties{"exact": name}); err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				meta, err := tbl.Metadata()
				if err != nil {
					return err
				}
				if meta.Properties()["exact"] != name {
					return fmt.Errorf("opaque identifier did not commit its own metadata")
				}
				return nil
			})
			found := false
			for _, effect := range result.Effects() {
				if effect.Operation == "commit" {
					found = true
					if !effect.Submitted || effect.Effect != Acknowledged || effect.MetadataLocation == "" {
						t.Fatal("identifier changed mutation classification")
					}
				}
			}
			if !found {
				t.Fatal("commit not observed")
			}
		})
	}
}

func TestManagedRESTIdentifiersPreserveLiteralEscapes(t *testing.T) {
	peer, options := newPeer(t)
	name := "literal%2Ftable"
	meta, err := table.NewMetadata(testSchema(), sdk.UnpartitionedSpec, table.UnsortedSortOrder, options.Location+"data", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	peer.persist("safe", raw)
	peer.tables[name] = raw
	peer.locations[name] = peer.locations["safe"]
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	f.run(t, "literal-identifier", func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", name})
		if err != nil {
			return err
		}
		if tbl.Identifier()[1] != name {
			return fmt.Errorf("opaque table identifier was reinterpreted")
		}
		return nil
	})
}

func TestManagedRESTDotNamespaceDoesNotRetargetParent(t *testing.T) {
	peer, options := newPeer(t)
	var actual string
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/config") {
			return false
		}
		actual = r.URL.EscapedPath()
		writeJSON(w, map[string]any{"namespace": []string{"."}, "properties": map[string]string{"exact": "dot"}})
		return true
	}
	settings := managedSettingsFor(options)
	settings.Namespace = []string{"."}
	f := bindManagedFixture(t, settings, 1)
	f.run(t, "dot-namespace", func(s *ManagedSession) error {
		props, err := s.LoadNamespaceProperties([]string{"."})
		if err != nil {
			return err
		}
		if props["exact"] != "dot" {
			return fmt.Errorf("namespace properties changed")
		}
		return nil
	})
	if actual != "/catalog/v1/warehouse/namespaces/%2E" {
		t.Fatalf("namespace path was retargeted: %q", actual)
	}
}
