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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"testing"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

func TestCatalogContainsNativeChartsTableLayoutsAndInteractions(t *testing.T) {
	card, post, image, table, err := report("")
	if err != nil {
		t.Fatal(err)
	}
	if string(table) != "period,items\nJan,80\nFeb,120\nMar,100\nApr,160\n" {
		t.Fatal("independent dataset differs")
	}
	if post.Type() != "post" {
		t.Fatal("rich-post example lost")
	}
	var decoded map[string]json.RawMessage
	_ = json.Unmarshal(card.JSON().Bytes(), &decoded)
	var body struct {
		Elements []map[string]json.RawMessage `json:"elements"`
	}
	_ = json.Unmarshal(decoded["body"], &body)
	kinds := map[string]int{}
	for _, element := range body.Elements {
		var tag string
		_ = json.Unmarshal(element["tag"], &tag)
		kinds[tag]++
		if tag == "chart" {
			var spec struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(element["chart_spec"], &spec)
			kinds[spec.Type]++
		}
	}
	for _, tag := range []string{"line", "bar", "pie", "table", "column_set", "collapsible_panel", "button"} {
		if kinds[tag] != 1 {
			t.Fatalf("missing native component %s", tag)
		}
	}
	bitmap, err := png.Decode(bytes.NewReader(image))
	if err != nil || bitmap.Bounds().Dx() != 640 || bitmap.Bounds().Dy() != 300 {
		t.Fatal("static PNG alternative malformed")
	}
	var source catalog
	if json.Unmarshal(catalogBytes, &source) != nil {
		t.Fatal("fixture")
	}
	if _, err := lark.Card(source.Interaction); err != nil {
		t.Fatal(err)
	}
}
func TestPreviewDoesNotReadCredentialsOrOverwrite(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "new-preview")
	if err := run(context.Background(), []string{"-preview", directory}, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"card.json"} {
		if _, err := os.Stat(filepath.Join(directory, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := run(context.Background(), []string{"-preview", directory}, io.Discard); err == nil {
		t.Fatal("preview overwrote files")
	}
	if err := run(context.Background(), []string{"-preview", directory, "-config", "not-readable"}, io.Discard); err == nil {
		t.Fatal("preview accepted credentials")
	}
}
func TestPrivateConfigurationBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("feishuid: app_fixture\nfeishusecret: secret-fixture\nunrelated: private\n"), 0600); err != nil {
		t.Fatal(err)
	}
	options, err := readSettings(path)
	if err != nil || options.AppID != "app_fixture" {
		t.Fatal("explicit private configuration rejected")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = readSettings(path); err == nil {
		t.Fatal("publicly readable credential file accepted")
	}
}
