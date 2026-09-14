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
	"net/url"
	"strings"
	"testing"

	native "github.com/minio/minio-go/v7"
)

func TestCopyOpaqueVersionAndSourceConditionOnWire(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	version := "opaque+/=&versionId=other"
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "HEAD" {
			objectHeaders(writer, storedObject{body: []byte("abc"), version: version, etag: "etag"})
			return true
		}
		if request.Method == "PUT" {
			source, err := url.Parse("/" + request.Header.Get("X-Amz-Copy-Source"))
			if err != nil || source.Query().Get("versionId") != version || len(source.Query()) != 1 || request.Header.Get("X-Amz-Copy-Source-If-Match") != "\"etag\"" {
				errorResponse(writer, 400, "InvalidRequest")
				return true
			}
			_, _ = io.WriteString(writer, "<CopyObjectResult><ETag>copied</ETag></CopyObjectResult>")
			return true
		}
		return false
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Copy(deadline(t), correlation("opaque-version"), CopyRequest{Source: Address{Key: "owned/source", VersionID: version}, Key: "owned/target"})
	result := settle(t, receipt, err)
	if result.Err() != nil || result.Outcome.Value.Transfer().Effect != Acknowledged {
		t.Fatal("opaque version or source ETag was not preserved", result.Err())
	}
}
func TestPutMissingETagDoesNotManufactureObjectEvidence(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "PUT" {
			_, _ = io.Copy(io.Discard, request.Body)
			writer.WriteHeader(200)
			return true
		}
		return false
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("no-etag"), WriteRequest{Key: "owned/no-etag", Size: 3}, bytes.NewReader([]byte("abc")))
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Transfer().Effect != Unknown {
		t.Fatal("incomplete native response manufactured an object acknowledgement")
	}
}
func TestVersionListingAndDeleteMarkerEvidence(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if _, ok := request.URL.Query()["versions"]; ok {
			_, _ = io.WriteString(writer, "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>owned/a</Key><VersionId>old</VersionId><IsLatest>false</IsLatest><Size>3</Size><ETag>etag</ETag><LastModified>2026-09-14T00:00:00Z</LastModified></Version><DeleteMarker><Key>owned/a</Key><VersionId>marker</VersionId><IsLatest>true</IsLatest><LastModified>2026-09-14T00:00:00Z</LastModified></DeleteMarker></ListVersionsResult>")
			return true
		}
		if request.Method == "HEAD" {
			writer.Header().Set("X-Amz-Version-Id", "marker")
			writer.Header().Set("X-Amz-Delete-Marker", "true")
			errorResponse(writer, 404, "NoSuchKey")
			return true
		}
		if request.Method == "DELETE" {
			writer.Header().Set("X-Amz-Delete-Marker", "true")
			writer.Header().Set("X-Amz-Version-Id", "new-marker")
			writer.WriteHeader(204)
			return true
		}
		return false
	}
	server.mu.Unlock()
	receipt, err := fixture.client.List(deadline(t), correlation("versions"), ListRequest{Prefix: "owned/", Versions: true})
	versions := settle(t, receipt, err)
	if versions.Err() != nil || !versions.Outcome.Value.Complete() || len(versions.Outcome.Value.ObjectsCopy()) != 2 {
		t.Fatal("version enumeration failed", versions.Err())
	}
	markerFound := false
	for _, object := range versions.Outcome.Value.ObjectsCopy() {
		if object.DeleteMarker && object.Address.VersionID == "marker" {
			markerFound = true
		}
	}
	if !markerFound {
		t.Fatal("delete marker omitted")
	}
	receipt, err = fixture.client.Stat(deadline(t), correlation("marker-stat"), Address{Key: "owned/a"})
	stat := settle(t, receipt, err)
	object, present := stat.Outcome.Value.Object()
	if !errors.Is(stat.Err(), ErrMissing) || !present || !object.DeleteMarker || object.Address.VersionID != "marker" {
		t.Fatal("native result accompanying error erased")
	}
	receipt, err = fixture.client.Remove(deadline(t), correlation("marker-remove"), []Address{{Key: "owned/a"}})
	removed := settle(t, receipt, err)
	if removed.Err() != nil || !removed.Outcome.Value.RemovalsCopy()[0].DeleteMarker || removed.Outcome.Value.RemovalsCopy()[0].DeleteMarkerVersionID != "new-marker" {
		t.Fatal("DELETE version evidence erased")
	}
}
func TestNativeCopyOptionsRequireExplicitEncoding(t *testing.T) {
	opts := native.CopySrcOptions{Bucket: "fixture", Object: "owned/key", VersionID: "a&b", MatchETag: "opaque"}
	headers := make(http.Header)
	opts.Marshal(headers)
	// This observation is NOT an acceptance test. The preceding integration test
	// rejects this native behavior and verifies the adapter's supported wire form.
	if !strings.HasSuffix(headers.Get("X-Amz-Copy-Source"), "?versionId=a&b") || headers.Get("X-Amz-Copy-Source-If-Match") != "opaque" {
		t.Fatal("native marshaling changed; re-evaluate adapter")
	}
}
