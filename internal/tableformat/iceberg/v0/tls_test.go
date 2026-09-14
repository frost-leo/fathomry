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
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestExplicitTLSRootsAndCleanup(t *testing.T) {
	peer := &testPeer{tables: map[string][]byte{}, locations: map[string]string{}, objects: map[string][]byte{}}
	server := httptest.NewTLSServer(http.HandlerFunc(peer.serve))
	defer server.Close()
	roots := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	options := OptionsV1{Name: "tls", CatalogURI: server.URL + "/catalog", CatalogPrefix: "warehouse", Warehouse: "test",
		Namespace: "isolated", Location: "s3://fixture/owned/", RootCAPEM: roots, StorageEndpoint: server.URL, StorageRegion: "us-east-1",
		StorageAccessKey: "test-access", StorageSecretKey: "test-secret", Writes: true}
	peer.options = options
	f := bindFixture(t, options, 8)
	setupTable(t, f)
	appendBatch(t, f, 0, 17, "tls")
	inspectRows(t, readAll(t, f), expectedRows(0, 17, "tls"))
}
func TestInvalidTrustDoesNotConstruct(t *testing.T) {
	_, options := newPeer(t)
	options.Plaintext = false
	options.CatalogURI = "https://127.0.0.1:1/catalog"
	options.StorageEndpoint = "https://127.0.0.1:1"
	for _, roots := range []string{"", " \n\t", "not a certificate"} {
		options.RootCAPEM = roots
		if _, err := Select(options); err == nil {
			t.Fatal("invalid trust accepted")
		}
	}
	// No network or constructor can run through a failed selection.
	selected, err := Select(options)
	if err == nil {
		t.Fatal("bad control")
	}
	assembly, err := resource.Assemble(deadline(t), deadline(t), "invalid", selected)
	if assembly != nil || err == nil {
		t.Fatal("failed selection constructed resources")
	}
}
