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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagedReadsImmutableV060Format2Fixture(t *testing.T) {
	const directory = "testdata/format2-v060"
	peer, options := newPeer(t)
	var index struct {
		MetadataLocation string `json:"metadata-location"`
		Rows             int    `json:"rows"`
	}
	raw, err := os.ReadFile(filepath.Join(directory, "fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &index); err != nil {
		t.Fatal(err)
	}
	if index.Rows != 65 {
		t.Fatal("fixture row identity changed")
	}
	err = filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "fixture.json" || entry.Name() == "README.md" {
			return nil
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		peer.objects["owned/historical/"+filepath.ToSlash(relative)] = raw
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	key := strings.TrimPrefix(index.MetadataLocation, "s3://fixture/")
	original := string(peer.objects[key])
	if original == "" {
		t.Fatal("fixture metadata object missing")
	}
	peer.tables["historical"] = []byte(original)
	peer.locations["historical"] = index.MetadataLocation
	settings := managedSettingsFor(options)
	settings.Permissions = Grants{Read: true}
	f := bindManagedFixture(t, settings, 1)
	result := f.run(t, "v060-read", func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", "historical"})
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		if meta.Version() != 2 || meta.CurrentSchema().Field(0).ID != 1 || meta.CurrentSchema().Field(1).ID != 2 {
			t.Error("binary upgrade changed persisted format/field IDs")
		}
		scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 9})
		if err != nil {
			return err
		}
		managedExactRows(t, scan, expectedRows(0, 65, "historical"))
		return nil
	})
	for _, effect := range result.Effects() {
		if effect.Submitted {
			t.Fatal("reading old data submitted a format upgrade")
		}
	}
	peer.mu.Lock()
	changed := string(peer.objects[key]) != original
	commits := peer.commits
	peer.mu.Unlock()
	if changed || commits != 0 {
		t.Fatal("old format-2 metadata was modified")
	}
}
