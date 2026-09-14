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
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	native "github.com/minio/minio-go/v7"
)

func TestObjectStorageIntegration(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 32)
	observer, _ := invocation.NewObserver(1)
	client, err := Bind(fixture.assembly, fixture.selected, fixture.inbox, observer)
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]string{"owner": "original"}
	receipt, err := client.Put(deadline(t), deadline(t), correlation("create"), WriteRequest{Key: "owned/data", Size: 5, IfAbsent: true, ContentType: "text/plain", Metadata: metadata}, strings.NewReader("first"))
	created := settle(t, receipt, err)
	metadata["owner"] = "changed"
	if created.Err() != nil || created.Outcome.Value.Transfer().Effect != Acknowledged {
		t.Fatal(created.Err())
	}
	old, _ := created.Outcome.Value.Object()
	if string(server.content("owned/data")) != "first" {
		t.Fatal("independent stored bytes differ")
	}
	put(t, client, "owned/data", []byte("second"))
	receipt, err = client.Read(deadline(t), correlation("version-range"), ReadRequest{Address: old.Address, Offset: 1, Length: 3})
	read := settle(t, receipt, err)
	if read.Err() != nil || string(read.Outcome.Value.DataCopy()) != "irs" {
		t.Fatal("exact version/range weakened", read.Err())
	}
	observed, _ := read.Outcome.Value.Object()
	copyMeta := observed.MetadataCopy()
	copyMeta["Owner"] = "aliased"
	copyMeta["owner"] = "aliased"
	header := observed.HeadersCopy()
	header.Set("X-Amz-Meta-Owner", "aliased")
	receipt, err = client.Stat(deadline(t), correlation("stat"), old.Address)
	stat := settle(t, receipt, err)
	fresh, _ := stat.Outcome.Value.Object()
	if fresh.HeadersCopy().Get("X-Amz-Meta-Owner") != "original" || observed.HeadersCopy().Get("X-Amz-Meta-Owner") != "original" {
		t.Fatal("metadata aliases evidence or native cache")
	}
	receipt, err = client.Copy(deadline(t), correlation("copy"), CopyRequest{Source: old.Address, Key: "owned/copy"})
	copied := settle(t, receipt, err)
	if copied.Err() != nil || string(server.content("owned/copy")) != "first" {
		t.Fatal("source version not copied", copied.Err())
	}
	receipt, err = client.SetTags(deadline(t), correlation("tags-set"), Address{Key: "owned/copy"}, map[string]string{"purpose": "test"})
	if got := settle(t, receipt, err); got.Err() != nil {
		t.Fatal(got.Err())
	}
	receipt, err = client.GetTags(deadline(t), correlation("tags-get"), Address{Key: "owned/copy"})
	tagged := settle(t, receipt, err)
	if tagged.Err() != nil || tagged.Outcome.Value.TagsCopy()["purpose"] != "test" {
		t.Fatal("tags lost", tagged.Err())
	}
	put(t, client, "owned/empty", []byte{})
	receipt, err = client.Read(deadline(t), correlation("empty"), ReadRequest{Address: Address{Key: "owned/empty"}})
	empty := settle(t, receipt, err)
	if empty.Err() != nil || empty.Outcome.Value.DataCopy() == nil || len(empty.Outcome.Value.DataCopy()) != 0 || !empty.Outcome.Value.Complete() {
		t.Fatal("successful empty confused with absence")
	}
	receipt, err = client.List(deadline(t), correlation("list"), ListRequest{Prefix: "owned/"})
	listed := settle(t, receipt, err)
	if listed.Err() != nil || !listed.Outcome.Value.Complete() || len(listed.Outcome.Value.ObjectsCopy()) != 3 {
		t.Fatal("listing incomplete", listed.Err())
	}
	receipt, err = client.Read(deadline(t), correlation("missing"), ReadRequest{Address: Address{Key: "owned/missing"}})
	missing := settle(t, receipt, err)
	if !errors.Is(missing.Err(), ErrMissing) || missing.Outcome.Value.Complete() || missing.Outcome.Value.DataCopy() != nil {
		t.Fatal("missing accepted as empty")
	}
	conformance.Cause[native.ErrorResponse](t, missing.Err(), func(response native.ErrorResponse) bool { return response.Code == "NoSuchKey" })
	if native.ToErrorResponse(missing.Err()).Code != "" {
		t.Fatal("native wrapper extraction behavior changed")
	}

	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "DELETE" && strings.HasSuffix(request.URL.Path, "/denied") {
			errorResponse(writer, 403, "AccessDenied")
			return true
		}
		return false
	}
	server.mu.Unlock()
	receipt, err = client.Remove(deadline(t), correlation("mixed-remove"), []Address{{Key: "owned/copy"}, {Key: "owned/denied"}, {Key: "owned/absent"}})
	removed := settle(t, receipt, err)
	entries := removed.Outcome.Value.RemovalsCopy()
	if !errors.Is(removed.Err(), ErrDenied) || len(entries) != 3 || entries[0].Effect != Acknowledged || entries[1].Effect != Unknown || entries[2].Effect != Acknowledged {
		t.Fatal("per-target effects lost")
	}
	if removed.Observation != invocation.ObservationDropped {
		t.Fatal("diagnostic overload control missing")
	}
	if err := observer.ExportOne(deadline(t), func(context.Context, invocation.Event) error { return errors.New("offline") }); !errors.Is(err, invocation.ErrObservation) {
		t.Fatal("diagnostic failure not observed")
	}

	access, err := resource.AccessFor(fixture.assembly, fixture.selected)
	if err != nil {
		t.Fatal(err)
	}
	foundFailure := false
	for fixture.inbox.Usage().Outstanding > 0 {
		delivery, err := fixture.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		result, err := delivery.Receipt().WaitReleased(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if result.Context.Correlation.Call == "mixed-remove" {
			foundFailure = true
			conformance.Result(t, result, conformance.Expected[Result]{
				Context: fault.Context{Provider: ProviderID, Source: options.Name, Scope: "test", Operation: "remove", Correlation: correlation("mixed-remove")},
				Source:  access.Info(), Limits: access.Limits(), Shape: invocation.Async, Present: true, Final: true, Released: true, Primary: ErrDenied,
				Attempts: invocation.Attempts{Exact: true, Observed: 3},
				Value: func(t testing.TB, value Result) {
					if len(value.RemovalsCopy()) != 3 {
						t.Error("handled-error evidence lost")
					}
				},
			})
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if !foundFailure {
		t.Fatal("independent evidence missing")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/minio/minio-go/v7"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, access, client.Profile(), []compatibility.Requirement{{Guarantee: "conditional-publication", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("profile invented service qualification")
	}
	conformance.Facade(t, client, "Put", "Read", "Download", "Stat", "Copy", "List", "ListUploads", "ListParts", "Abort", "Remove", "GetTags", "SetTags", "Profile", "EvidenceBytes",
		"String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
}
func TestInboxSaturationRejectsBeforeNativeIO(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	put(t, fixture.client, "owned/first", []byte("first"))
	before := server.count()
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("rejected"), WriteRequest{Key: "owned/second", Size: 1}, bytes.NewReader([]byte("x")))
	if receipt != nil || !errors.Is(err, invocation.ErrEvidence) || server.count() != before {
		t.Fatal("evidence saturation submitted native work")
	}
}
