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

package rest

import (
	"net/url"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
)

func TestRESTObjectPathIdentifiersAreOpaque(t *testing.T) {
	base, err := url.Parse("https://catalog.example/v1/warehouse")
	if err != nil {
		t.Fatal(err)
	}
	catalog := &Catalog{}
	for _, endpoint := range []endpoint{endpointLoadTable, endpointUpdateTable, endpointDeleteTable, endpointLoadView, endpointUpdateView, endpointLoadFunction, endpointPlanTableScan, endpointTableCredentials, endpointReportMetrics} {
		for _, name := range []string{"literal%2Fname", "literal%2e%2e", "a?b", "a#b", "snow 雪"} {
			path, err := endpoint.reqPath(catalog.encodeNamespace(table.Identifier{"isolated"}), name)
			if err != nil {
				t.Fatal(err)
			}
			uri := base.JoinPath(path...)
			segments := strings.Split(uri.EscapedPath(), "/")
			index := 6
			if len(segments) <= index {
				t.Fatal("object endpoint missing path segment")
			}
			decoded, err := url.PathUnescape(segments[index])
			if err != nil || decoded != name {
				t.Fatalf("object name changed for %s: got %q want %q", endpoint, decoded, name)
			}
		}
	}
}
func TestRESTNamespaceDotLevelsDoNotCleanParentPath(t *testing.T) {
	base, _ := url.Parse("https://catalog.example/v1/warehouse")
	catalog := &Catalog{}
	for _, name := range []string{".", "..", "%2E", "雪"} {
		path, err := endpointLoadNamespace.reqPath(catalog.encodeNamespace(table.Identifier{name}))
		if err != nil {
			t.Fatal(err)
		}
		uri := base.JoinPath(path...)
		parts := strings.Split(uri.EscapedPath(), "/")
		if len(parts) != 5 {
			t.Fatalf("namespace traversal changed path depth: %s", uri.EscapedPath())
		}
		decoded, err := url.PathUnescape(parts[4])
		if err != nil || decoded != name {
			t.Fatal("opaque namespace level changed")
		}
	}
}
