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

package minio

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestListingShapeBeforeNativeDecode(t *testing.T) {
	const entry = "<Contents><Key>owned/key</Key><Size>0</Size></Contents>"
	for _, test := range []struct {
		name, content string
		want          error
	}{
		{"valid", entry, nil},
		{"page-cap", strings.Repeat(entry, maxNativeListEntries+1), ErrLimit},
		{"prefix", "<CommonPrefixes><Prefix>owned/subdir/</Prefix></CommonPrefixes>", ErrProtocol},
		{"grant", "<Contents><Key>owned/key</Key><Size>0</Size><Grant/></Contents>", ErrProtocol},
		{"metadata", "<Contents><Key>owned/key</Key><Size>0</Size><UserMetadata><key>value</key></UserMetadata></Contents>", ErrProtocol},
		{"tags", "<Contents><Key>owned/key</Key><Size>0</Size><UserTags>key=value</UserTags></Contents>", ErrProtocol},
		{"duplicate-key", "<Contents><Key>other</Key><Key>owned/key</Key><Size>0</Size></Contents>", ErrProtocol},
		{"missing-size", "<Contents><Key>owned/key</Key></Contents>", ErrProtocol},
		{"nested-scalar", "<Contents><Key><Value>owned/key</Value></Key><Size>0</Size></Contents>", ErrProtocol},
		{"empty", "", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := &controlResponse{statusCode: http.StatusOK, readErr: io.EOF}
			response.body.WriteString("<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated>" + test.content + "</ListBucketResult>")
			_, err := response.listPage("ListBucketResult")
			if !errors.Is(err, test.want) {
				t.Fatal("unexpected page admission", err)
			}
		})
	}
	// A valid native page can exceed an integration output chunk. The independent
	// page cap and envelope bound SDK retention without losing legacy partial output.
	body := "<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated>" + strings.Repeat(entry, maxNativeListEntries) + "</ListBucketResult>"
	response := &controlResponse{statusCode: http.StatusOK, readErr: io.EOF, body: *bytes.NewBufferString(body)}
	if _, err := response.listPage("ListBucketResult"); err != nil {
		t.Fatal("bounded native page rejected", err)
	}
	if nativeListWorkBytes < maxNativeListEntries*4096 {
		t.Fatal("native page envelope missing")
	}
}

func TestObjectListingsRejectInvalidPageIdentityAndPrefixes(t *testing.T) {
	for _, test := range []struct{ name, identity, content string }{
		{"wrong-bucket", "<Name>other</Name>", "<Contents><Key>owned/key</Key><Size>0</Size></Contents>"},
		{"missing-bucket", "", ""},
		{"duplicate-bucket", "<Name>other</Name><Name>fixture</Name>", ""},
		{"prefix", "<Name>fixture</Name>", "<CommonPrefixes><Prefix>owned/subdir/</Prefix></CommonPrefixes>"},
		{"unrequested-grant", "<Name>fixture</Name>", "<Contents><Key>owned/key</Key><Size>0</Size><Grant/></Contents>"},
	} {
		for _, cursorMode := range []bool{false, true} {
			name := test.name + "/finite"
			if cursorMode {
				name = test.name + "/cursor"
			}
			t.Run(name, func(t *testing.T) {
				server, options := newPeer(t)
				fixture := bindFixture(t, options, 2)
				server.mu.Lock()
				server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
					_, _ = io.WriteString(writer, "<ListBucketResult>"+test.identity+"<IsTruncated>false</IsTruncated>"+test.content+"</ListBucketResult>")
					return true
				}
				server.mu.Unlock()
				if cursorMode {
					cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/"})
					if err != nil {
						t.Fatal(err)
					}
					next, err := cursor.Next(deadline(t), childID("next", "root"))
					result := settle(t, next, err)
					if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() || len(result.Outcome.Value.ObjectsCopy()) != 0 {
						t.Fatal("invalid cursor data accepted", result.Err())
					}
					if final := settle(t, root, nil); !errors.Is(final.Err(), ErrProtocol) {
						t.Fatal("root lost protocol failure")
					}
				} else {
					receipt, err := fixture.client.List(deadline(t), correlation("list"), ListRequest{Prefix: "owned/"})
					result := settle(t, receipt, err)
					if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() || len(result.Outcome.Value.ObjectsCopy()) != 0 {
						t.Fatal("invalid finite data accepted", result.Err())
					}
				}
			})
		}
	}
}

func TestMultipartInspectionRequiresUnambiguousIdentity(t *testing.T) {
	for _, identity := range []string{
		"<Bucket>other</Bucket><Bucket>fixture</Bucket><Key>owned/key</Key><UploadId>upload</UploadId>",
		"<Bucket>fixture</Bucket><Key>other</Key><Key>owned/key</Key><UploadId>upload</UploadId>",
		"<Bucket>fixture</Bucket><Key>owned/key</Key><UploadId>other</UploadId><UploadId>upload</UploadId>",
	} {
		server, options := newPeer(t)
		fixture := bindFixture(t, options, 1)
		server.mu.Lock()
		server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
			_, _ = io.WriteString(writer, "<ListPartsResult>"+identity+"<IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>1</Size></Part></ListPartsResult>")
			return true
		}
		server.mu.Unlock()
		receipt, err := fixture.client.ListParts(deadline(t), correlation("parts"), Upload{Key: "owned/key", ID: "upload"}, 0)
		result := settle(t, receipt, err)
		if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() || len(result.Outcome.Value.PartsCopy()) != 0 {
			t.Fatal("ambiguous part identity accepted", result.Err())
		}
	}
}
