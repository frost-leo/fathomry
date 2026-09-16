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
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	native "github.com/minio/minio-go/v7"
)

func TestReviewListingRejectsInvalidObjects(t *testing.T) {
	for _, test := range []struct {
		name       string
		versions   bool
		startAfter string
		body       string
	}{
		{"before-start", false, "owned/b", "<Contents><Key>owned/a</Key><Size>1</Size></Contents>"},
		{"negative-size", false, "", "<Contents><Key>owned/a</Key><Size>-1</Size></Contents>"},
		{"missing-version", true, "", "<Version><Key>owned/a</Key><Size>1</Size></Version>"},
		{"missing-marker-version", true, "", "<DeleteMarker><Key>owned/a</Key></DeleteMarker>"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 1)
			server.mu.Lock()
			server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				root := "ListBucketResult"
				if test.versions {
					root = "ListVersionsResult"
				}
				_, _ = fmt.Fprintf(writer, "<%s><IsTruncated>false</IsTruncated>%s</%s>", root, test.body, root)
				return true
			}
			server.mu.Unlock()
			receipt, err := fixture.client.List(deadline(t), correlation(test.name), ListRequest{Prefix: "owned/", StartAfter: test.startAfter, Versions: test.versions})
			result := settle(t, receipt, err)
			if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() || len(result.Outcome.Value.ObjectsCopy()) != 0 {
				t.Fatal("invalid native listing became usable object evidence", result.Err())
			}
		})
	}
}

func TestReviewListingRejectsWrongXMLRoot(t *testing.T) {
	for _, body := range []string{"<Unexpected/>", "<ListBucketResult/><Unexpected/>", "<Error><Code>AccessDenied</Code><Message>private-native-canary</Message></Error>"} {
		server, options := newPeer(t)
		fixture := bindFixture(t, options, 1)
		server.mu.Lock()
		server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
			_, _ = io.WriteString(writer, body)
			return true
		}
		server.mu.Unlock()
		receipt, err := fixture.client.List(deadline(t), correlation("xml-root"), ListRequest{Prefix: "owned/"})
		result := settle(t, receipt, err)
		if result.Err() == nil || result.Outcome.Value.Complete() {
			t.Error("wrong XML root became successful empty listing")
		}
		if strings.HasPrefix(body, "<Error>") {
			var response native.ErrorResponse
			if !errors.Is(result.Err(), ErrDenied) || !errors.As(result.Err(), &response) || response.Code != "AccessDenied" {
				t.Error("native error embedded in an HTTP 200 response was lost")
			}
			if strings.Contains(fmt.Sprintf("%+v", result.Err()), "private-native-canary") {
				t.Error("native response text escaped restricted diagnostics")
			}
		}
	}
}

func TestReviewUploadCursorRemainsOpaque(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		_, _ = io.WriteString(writer, "<ListMultipartUploadsResult><Bucket>fixture</Bucket><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><NextKeyMarker>owned%2Fkey</NextKeyMarker><NextUploadIdMarker>opaque+id</NextUploadIdMarker><Upload><Key>owned%2Fkey</Key><UploadId>opaque+id</UploadId></Upload></ListMultipartUploadsResult>")
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.ListUploads(deadline(t), correlation("opaque-upload-cursor"), UploadQuery{Prefix: "owned/"})
	result := settle(t, receipt, err)
	_, next := result.Outcome.Value.UploadCursor()
	if result.Err() == nil && next != "opaque+id" {
		t.Fatal("URL decoding silently changed the opaque upload marker")
	}
	if result.Err() != nil && !errors.Is(result.Err(), ErrUnsupported) {
		t.Fatal("opaque cursor was neither retained nor explicitly refused", result.Err())
	}
	if len(result.Outcome.Value.UploadsCopy()) != 1 || result.Outcome.Value.Complete() || result.Err() != nil && next != "" {
		t.Fatal("unsupported cursor discarded validated uploads or returned a usable marker")
	}
}

func TestReviewInspectionLimitsRetainPartialEntries(t *testing.T) {
	for _, parts := range []bool{false, true} {
		server, options := newPeer(t)
		options.MaxEntries = 1
		fixture := bindFixture(t, options, 1)
		server.mu.Lock()
		server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
			if parts {
				_, _ = io.WriteString(writer, "<ListPartsResult><Bucket>fixture</Bucket><Key>owned/key</Key><UploadId>upload</UploadId><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>1</Size></Part><Part><PartNumber>2</PartNumber><Size>1</Size></Part></ListPartsResult>")
			} else {
				_, _ = io.WriteString(writer, "<ListMultipartUploadsResult><Bucket>fixture</Bucket><IsTruncated>false</IsTruncated><Upload><Key>owned/a</Key><UploadId>first</UploadId></Upload><Upload><Key>owned/b</Key><UploadId>second</UploadId></Upload></ListMultipartUploadsResult>")
			}
			return true
		}
		server.mu.Unlock()
		if parts {
			receipt, err := fixture.client.ListParts(deadline(t), correlation("partial-parts"), Upload{Key: "owned/key", ID: "upload"}, 0)
			result := settle(t, receipt, err)
			if !errors.Is(result.Err(), ErrLimit) || len(result.Outcome.Value.PartsCopy()) != 1 || result.Outcome.Value.Complete() {
				t.Error("bounded parts preceding overflow were discarded")
			}
		} else {
			receipt, err := fixture.client.ListUploads(deadline(t), correlation("partial-uploads"), UploadQuery{Prefix: "owned/"})
			result := settle(t, receipt, err)
			if !errors.Is(result.Err(), ErrLimit) || len(result.Outcome.Value.UploadsCopy()) != 1 || result.Outcome.Value.Complete() {
				t.Error("bounded uploads preceding overflow were discarded")
			}
		}
	}
}

func TestReviewPartsCursorMustMatchLastObservedPart(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		_, _ = io.WriteString(writer, "<ListPartsResult><Bucket>fixture</Bucket><Key>owned/key</Key><UploadId>upload</UploadId><IsTruncated>true</IsTruncated><NextPartNumberMarker>3</NextPartNumberMarker><Part><PartNumber>1</PartNumber><Size>1</Size></Part></ListPartsResult>")
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.ListParts(deadline(t), correlation("part-cursor"), Upload{Key: "owned/key", ID: "upload"}, 0)
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), ErrProtocol) || len(result.Outcome.Value.PartsCopy()) != 1 || result.Outcome.Value.NextPart() != 0 {
		t.Fatal("part cursor can skip unobserved parts or discards preceding evidence", result.Err())
	}
}

func TestReviewUploadChecksumsRetainObservations(t *testing.T) {
	info := native.UploadInfo{Key: "owned/key", ChecksumMode: "FULL_OBJECT", ChecksumSHA256: "observed-sha", ChecksumCRC32C: "observed-crc"}
	object := uploadInfo(info)
	if sums := object.ChecksumsCopy(); sums["SHA256"] != info.ChecksumSHA256 || sums["CRC32C"] != info.ChecksumCRC32C {
		t.Fatal("native upload checksum observations were dropped")
	}
	sums := object.ChecksumsCopy()
	sums["SHA256"] = "changed"
	if object.ChecksumsCopy()["SHA256"] != info.ChecksumSHA256 {
		t.Fatal("checksum inspection aliases evidence")
	}
}

func TestReviewNativePointerErrorRetainsIdentity(t *testing.T) {
	response := &native.ErrorResponse{Code: "AccessDenied", Message: "private-native-canary"}
	err := nativeFailure(ErrRead, "tags", context.Background(), errors.Join(response))
	var observed *native.ErrorResponse
	if !errors.Is(err, ErrDenied) || !errors.As(err, &observed) || observed != response {
		t.Fatal("native pointer error lost its technical classification or cause")
	}
}

func TestReviewTLSRejectsPartiallyValidTrustBundle(t *testing.T) {
	server, options := newPeer(t)
	secure := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(secure.Close)
	options.Endpoint = secure.URL
	options.Plaintext = false
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: secure.Certificate().Raw}))
	for _, content := range []string{
		certificate + "not-a-certificate",
		"not-a-certificate\n" + certificate,
		"-----BEGIN CERTIFICATE-----\nmalformed\n-----END CERTIFICATE-----\n" + certificate,
		certificate + string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("fixture-not-a-key")})),
	} {
		options.RootCAPEM = content
		if _, err := Select(options); !errors.Is(err, resource.ErrConfiguration) {
			t.Error("partially valid trust set silently ignored malformed or unsupported input")
		}
	}
	options.RootCAPEM = "\n" + certificate + "\n" + certificate
	if _, err := Select(options); err != nil {
		t.Fatal("explicit complete certificate bundle rejected", err)
	}
	if server.count() != 0 {
		t.Fatal("trust validation performed I/O")
	}
}

func TestReviewRemovalRetainsErrorResponseVersionEvidence(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if strings.HasSuffix(request.URL.Path, "/denied") {
			writer.Header().Set("X-Amz-Delete-Marker", "true")
			writer.Header().Set("X-Amz-Version-Id", "observed-marker")
			errorResponse(writer, 403, "AccessDenied")
			return true
		}
		return false
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Remove(deadline(t), correlation("remove-error-version"), []Address{{Key: "owned/denied"}, {Key: "owned/absent"}})
	result := settle(t, receipt, err)
	removals := result.Outcome.Value.RemovalsCopy()
	if len(removals) != 2 || !errors.Is(removals[0].Err, ErrDenied) || !removals[0].DeleteMarker || removals[0].DeleteMarkerVersionID != "observed-marker" {
		t.Fatal("version/delete-marker observations accompanying a DELETE error were dropped")
	}
	if removals[1].DeleteMarker || removals[1].DeleteMarkerVersionID != "" {
		t.Fatal("prior target headers leaked into later removal evidence")
	}
}

func TestReviewListingRetainsPriorPageWhenEmptyPageIsMalformed(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		switch request.URL.Query().Get("continuation-token") {
		case "":
			_, _ = io.WriteString(writer, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>second</NextContinuationToken><Contents><Key>owned/a</Key><Size>1</Size></Contents></ListBucketResult>")
		case "second":
			_, _ = io.WriteString(writer, "<Unexpected><IsTruncated>true</IsTruncated><NextContinuationToken>third</NextContinuationToken></Unexpected>")
		default:
			_, _ = io.WriteString(writer, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
		}
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.List(deadline(t), correlation("empty-page-validation"), ListRequest{Prefix: "owned/"})
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() || len(result.Outcome.Value.ObjectsCopy()) != 1 {
		t.Fatal("invalid empty page was lost across native iterator requests or prior valid evidence was discarded")
	}
}

func TestReviewInspectionCursorPositiveControls(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		query := request.URL.Query()
		if _, uploads := query["uploads"]; uploads {
			if query.Get("key-marker") == "" {
				_, _ = io.WriteString(writer, "<ListMultipartUploadsResult><Bucket>fixture</Bucket><IsTruncated>true</IsTruncated><NextKeyMarker>owned/key</NextKeyMarker><NextUploadIdMarker>opaque+%/id</NextUploadIdMarker><Upload><Key>owned/key</Key><UploadId>opaque+%/id</UploadId></Upload></ListMultipartUploadsResult>")
			} else {
				if query.Get("upload-id-marker") != "opaque+%/id" {
					t.Error("request changed an explicitly supplied opaque cursor")
				}
				_, _ = io.WriteString(writer, "<ListMultipartUploadsResult><Bucket>fixture</Bucket><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><NextKeyMarker>owned%2Fkey</NextKeyMarker><NextUploadIdMarker>unused+marker</NextUploadIdMarker></ListMultipartUploadsResult>")
			}
			return true
		}
		_, _ = io.WriteString(writer, "<ListPartsResult><Bucket>fixture</Bucket><Key>owned/key</Key><UploadId>upload</UploadId><IsTruncated>true</IsTruncated><NextPartNumberMarker>3</NextPartNumberMarker><Part><PartNumber>2</PartNumber><Size>1</Size></Part><Part><PartNumber>3</PartNumber><Size>1</Size></Part></ListPartsResult>")
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.ListUploads(deadline(t), correlation("cursor-first"), UploadQuery{Prefix: "owned/"})
	first := settle(t, receipt, err)
	key, id := first.Outcome.Value.UploadCursor()
	if first.Err() != nil || key != "owned/key" || id != "opaque+%/id" || first.Outcome.Value.Complete() {
		t.Fatal("nonencoded opaque cursor was not preserved", first.Err())
	}
	receipt, err = fixture.client.ListUploads(deadline(t), correlation("cursor-last"), UploadQuery{Prefix: "owned/", KeyMarker: key, UploadIDMarker: id})
	last := settle(t, receipt, err)
	key, id = last.Outcome.Value.UploadCursor()
	if last.Err() != nil || !last.Outcome.Value.Complete() || key != "" || id != "" {
		t.Fatal("terminal encoded page was not accepted or exposed an unused cursor", last.Err())
	}
	receipt, err = fixture.client.ListParts(deadline(t), correlation("parts-next"), Upload{Key: "owned/key", ID: "upload"}, 1)
	parts := settle(t, receipt, err)
	if parts.Err() != nil || parts.Outcome.Value.NextPart() != 3 || len(parts.Outcome.Value.PartsCopy()) != 2 || parts.Outcome.Value.Complete() {
		t.Fatal("valid part cursor rejected", parts.Err())
	}
	copyParts := parts.Outcome.Value.PartsCopy()
	copyParts[0].Size = 0
	if parts.Outcome.Value.PartsCopy()[0].Size != 1 {
		t.Fatal("native part DTO inspection mutated retained evidence")
	}
}

func TestReviewRemovalBudgetPreservesUnsubmittedTargets(t *testing.T) {
	server, options := newPeer(t)
	options.MaxRequests = 1
	fixture := bindFixture(t, options, 1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		writer.Header().Set("X-Amz-Delete-Marker", "true")
		writer.Header().Set("X-Amz-Version-Id", "observed-marker")
		writer.WriteHeader(http.StatusNoContent)
		return true
	}
	server.mu.Unlock()
	addresses := []Address{{Key: "owned/first", VersionID: "null"}, {Key: "owned/second"}, {Key: "owned/third"}}
	receipt, err := fixture.client.Remove(deadline(t), correlation("remove-bounded"), addresses)
	addresses[0].Key = "owned/mutated"
	result := settle(t, receipt, err)
	removals := result.Outcome.Value.RemovalsCopy()
	if !errors.Is(result.Err(), ErrLimit) || len(removals) != 3 || removals[0].Address.Key != "owned/first" || removals[0].Effect != Acknowledged {
		t.Fatal("submitted removal or frozen target identity was lost")
	}
	for _, removal := range removals[1:] {
		if removal.Effect != NotSubmitted || !errors.Is(removal.Err, ErrLimit) || removal.DeleteMarker || removal.DeleteMarkerVersionID != "" {
			t.Fatal("unsubmitted removal borrowed a prior target's effect or headers")
		}
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.requests) != 2 || server.requests[1].query.Get("versionId") != "null" {
		t.Fatal("literal null version was lost or bounded removals submitted excess requests")
	}
}

func TestReviewCapabilityRefusalsDoNotSubmit(t *testing.T) {
	server, options := newPeer(t)
	options.Writes, options.Versions, options.Tags = false, false, false
	fixture := bindFixture(t, options, 1)
	ctx := deadline(t)
	for _, test := range []struct {
		name string
		want error
		call func() (*invocation.Receipt[Result], error)
	}{
		{"write", ErrAuthority, func() (*invocation.Receipt[Result], error) {
			return fixture.client.Remove(ctx, correlation("refused-write"), []Address{{Key: "owned/key"}})
		}},
		{"abort", ErrAuthority, func() (*invocation.Receipt[Result], error) {
			return fixture.client.Abort(ctx, correlation("refused-abort"), Upload{Key: "owned/key", ID: "upload"})
		}},
		{"version", ErrUnsupported, func() (*invocation.Receipt[Result], error) {
			return fixture.client.Stat(ctx, correlation("refused-version"), Address{Key: "owned/key", VersionID: "null"})
		}},
		{"versions", ErrUnsupported, func() (*invocation.Receipt[Result], error) {
			return fixture.client.List(ctx, correlation("refused-versions"), ListRequest{Prefix: "owned/", Versions: true})
		}},
		{"tags", ErrUnsupported, func() (*invocation.Receipt[Result], error) {
			return fixture.client.GetTags(ctx, correlation("refused-tags"), Address{Key: "owned/key"})
		}},
		{"prefix", ErrAuthority, func() (*invocation.Receipt[Result], error) {
			return fixture.client.ListUploads(ctx, correlation("refused-prefix"), UploadQuery{Prefix: "other/"})
		}},
		{"ignored-marker", ErrInput, func() (*invocation.Receipt[Result], error) {
			return fixture.client.ListUploads(ctx, correlation("refused-marker"), UploadQuery{Prefix: "owned/", UploadIDMarker: "upload"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt, err := test.call()
			if receipt != nil || !errors.Is(err, test.want) {
				t.Fatal("source capability or requested selection was silently weakened", err)
			}
		})
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if receipt, err := fixture.client.List(canceled, correlation("canceled-list"), ListRequest{Prefix: "owned/"}); receipt != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled enumeration was submitted", err)
	}
	if server.count() != 1 {
		t.Fatal("refused operation performed native I/O")
	}
}

func TestReviewListingsRequireExplicitTerminalEvidence(t *testing.T) {
	for _, family := range []struct {
		name     string
		root     string
		metadata string
		call     func(*Client, context.Context) (*invocation.Receipt[Result], error)
	}{
		{"objects", "ListBucketResult", "", func(client *Client, ctx context.Context) (*invocation.Receipt[Result], error) {
			return client.List(ctx, correlation("terminal-objects"), ListRequest{Prefix: "owned/"})
		}},
		{"versions", "ListVersionsResult", "", func(client *Client, ctx context.Context) (*invocation.Receipt[Result], error) {
			return client.List(ctx, correlation("terminal-versions"), ListRequest{Prefix: "owned/", Versions: true})
		}},
		{"uploads", "ListMultipartUploadsResult", "<Bucket>fixture</Bucket>", func(client *Client, ctx context.Context) (*invocation.Receipt[Result], error) {
			return client.ListUploads(ctx, correlation("terminal-uploads"), UploadQuery{Prefix: "owned/"})
		}},
		{"parts", "ListPartsResult", "<Bucket>fixture</Bucket><Key>owned/key</Key><UploadId>upload</UploadId>", func(client *Client, ctx context.Context) (*invocation.Receipt[Result], error) {
			return client.ListParts(ctx, correlation("terminal-parts"), Upload{Key: "owned/key", ID: "upload"}, 0)
		}},
	} {
		for _, terminal := range []struct {
			name  string
			xml   string
			valid bool
		}{
			{"absent", "", false},
			{"empty", "<IsTruncated/>", false},
			{"duplicate", "<IsTruncated>false</IsTruncated><IsTruncated>false</IsTruncated>", false},
			{"invalid", "<IsTruncated>unknown</IsTruncated>", false},
			{"complete", "<IsTruncated>false</IsTruncated>", true},
		} {
			t.Run(family.name+"/"+terminal.name, func(t *testing.T) {
				server, options := newPeer(t)
				fixture := bindFixture(t, options, 1)
				server.mu.Lock()
				server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
					_, _ = fmt.Fprintf(writer, "<%s>%s%s</%s>", family.root, family.metadata, terminal.xml, family.root)
					return true
				}
				server.mu.Unlock()
				receipt, err := family.call(fixture.client, deadline(t))
				result := settle(t, receipt, err)
				if terminal.valid {
					if result.Err() != nil || !result.Outcome.Value.Complete() {
						t.Fatal("explicit complete-empty response was rejected", result.Err())
					}
				} else if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() {
					t.Fatal("terminal metadata was accepted or was not classified as a protocol failure", result.Err())
				}
			})
		}
	}
}

func TestReviewMissingTerminalEvidencePreservesPriorPage(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.URL.Query().Get("continuation-token") == "" {
			_, _ = io.WriteString(writer, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>owned/a</Key><Size>1</Size></Contents></ListBucketResult>")
		} else {
			_, _ = io.WriteString(writer, "<ListBucketResult><Contents><Key>owned/b</Key><Size>1</Size></Contents></ListBucketResult>")
		}
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.List(deadline(t), correlation("terminal-prior-page"), ListRequest{Prefix: "owned/"})
	result := settle(t, receipt, err)
	objects := result.Outcome.Value.ObjectsCopy()
	if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() || len(objects) != 1 || objects[0].Address.Key != "owned/a" {
		t.Fatal("missing terminal evidence was accepted or discarded the previously validated page", result.Err())
	}
}

func TestReviewUploadCursorDecodeFailureOmitsPartiallyDecodedEntries(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		_, _ = io.WriteString(writer, "<ListMultipartUploadsResult><Bucket>fixture</Bucket><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><NextKeyMarker>owned%2Fkey</NextKeyMarker><NextUploadIdMarker>opaque%</NextUploadIdMarker><Upload><Key>owned%2Fkey</Key><UploadId>opaque%</UploadId></Upload></ListMultipartUploadsResult>")
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.ListUploads(deadline(t), correlation("cursor-decode-failure"), UploadQuery{Prefix: "owned/"})
	result := settle(t, receipt, err)
	var nativeCause url.EscapeError
	key, id := result.Outcome.Value.UploadCursor()
	if !errors.Is(result.Err(), ErrUnsupported) || !errors.As(result.Err(), &nativeCause) || result.Outcome.Value.Complete() ||
		len(result.Outcome.Value.UploadsCopy()) != 0 || key != "" || id != "" {
		t.Fatal("native decode failure lost its cause or exposed partially transformed fields", result.Err())
	}
}
