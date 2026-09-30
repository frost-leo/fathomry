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

package nacos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestRead(t *testing.T) {
	fixture := newService(t)
	selected := fixture.settings()
	selected.Keys = []Key{{DataID: "main"}, {DataID: "empty"}, {DataID: "absent"}}
	owner, _, _ := openService(t, fixture, selected)
	client := owner.Client()
	t.Run("metadata and isolation", func(t *testing.T) {
		document, err := client.Read(context.Background(), Key{DataID: "main"})
		if err != nil {
			t.Fatal(err)
		}
		if document.Key() != (Key{Group: "DEFAULT_GROUP", DataID: "main"}) || document.Namespace() != "" || document.MD5() != checksum("value: initial\n") || document.ContentType() != "yaml" || document.LastModifiedMillis() != 7 || document.Missing() {
			t.Fatal("metadata lost")
		}
		raw := document.RawCopy()
		raw[0] = 'x'
		if string(document.RawCopy()) != "value: initial\n" {
			t.Fatal("content alias")
		}
		if strings.Contains(fmt.Sprintf("%#v", document), "initial") {
			t.Fatal("content leaked")
		}
	})
	t.Run("required and raw default batch", func(t *testing.T) {
		if documents, err := client.ReadAll(context.Background()); len(documents) != 0 || !errors.Is(err, ErrEmpty) {
			t.Fatal("usable partial required batch", err)
		}
		documents, failed, err := client.ReadRawAll(context.Background())
		if err != nil || len(documents) != 3 || failed != -1 {
			t.Fatal(err)
		}
		if documents[0].Missing() || documents[1].Missing() || len(documents[1].RawCopy()) != 0 || !documents[2].Missing() {
			t.Fatal("presence semantics")
		}
		missing, err := client.ReadRaw(context.Background(), Key{DataID: "absent"})
		if err != nil || !missing.Missing() {
			t.Fatal(err)
		}
		if _, err := client.Read(context.Background(), Key{DataID: "absent"}); !errors.Is(err, ErrMissing) {
			t.Fatal(err)
		}
	})
	t.Run("malformed and native refusal", func(t *testing.T) {
		fixture.mu.Lock()
		fixture.malformed = true
		fixture.mu.Unlock()
		if values, failed, err := client.ReadRawAll(context.Background()); len(values) != 0 || failed != 0 || !errors.Is(err, ErrDecode) {
			t.Fatal("partial malformed batch", failed, err)
		}
		fixture.mu.Lock()
		fixture.malformed = false
		fixture.denied = true
		fixture.mu.Unlock()
		_, err := client.Read(context.Background(), Key{DataID: "main"})
		native, ok := InspectError(err)
		if !errors.Is(err, ErrDenied) || !ok || native.ErrorCode() != 403 || native.ResultCode() != 500 || native.Message() != "server-private-canary" {
			t.Fatal("native evidence missing", err)
		}
		if strings.Contains(fmt.Sprintf("%+v %#v", err, native), "private-canary") {
			t.Fatal("native message leaked")
		}
		fixture.mu.Lock()
		fixture.denied = false
		fixture.mu.Unlock()
	})
	t.Run("scope and runtime admission", func(t *testing.T) {
		bounded := fixture.settings()
		bounded.DynamicKeys = false
		bounded.Writable = false
		owner, _, inbox := openService(t, fixture, bounded)
		calls := fixture.queries.Load()
		if _, err := owner.Client().Read(context.Background(), Key{DataID: "unselected"}); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
		if fixture.queries.Load() != calls {
			t.Fatal("dispatched unselected key")
		}
		if err := inbox.Seal(); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.Client().Read(context.Background(), Key{DataID: "main"}); !errors.Is(err, adapters.ErrClosed) {
			t.Fatal(err)
		}
		if fixture.queries.Load() != calls {
			t.Fatal("dispatched without evidence")
		}
	})
}

func TestOwner(t *testing.T) {
	fixture := newService(t)
	owner, runtime, inbox := openService(t, fixture, fixture.settings())
	if fixture.queries.Load() != 0 || fixture.setups.Load() != 0 || owner.ShutdownComplete() {
		t.Fatal("Open performed readiness I/O")
	}
	info := owner.Info()
	if info.Name != "fixture" || info.Provider != ProviderID || len(info.Revision) != 32 || info.Format != 1 || len(info.Provenance) == 0 {
		t.Fatal("native metadata lost")
	}
	info.Provenance[0].Fields[0] = "changed"
	if owner.Info().Provenance[0].Fields[0] == "changed" {
		t.Fatal("metadata alias")
	}
	delivery, err := inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	if value, resolved := receipt.Snapshot(); resolved || value.Info().Released {
		t.Fatal("owner released at Open")
	}
	if err := owner.Close(nil); !errors.Is(err, ErrInput) || owner.ShutdownComplete() {
		t.Fatal("nil Close changed ownership", err)
	}
	if _, err := owner.Client().Read(context.Background(), Key{DataID: "main"}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(context.Background()); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	if value, err := receipt.WaitReleased(context.Background()); err != nil || value.Err() != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Client().Read(context.Background(), Key{DataID: "main"}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
