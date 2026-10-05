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
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

func TestSettingsValidationAndBudget(t *testing.T) {
	peer := newPeer(t)
	value := peer.options
	if err := Validate(value); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal("settings not loadable")
	}
	var copied Settings
	if err := json.Unmarshal(encoded, &copied); err != nil || copied != value {
		t.Fatal("settings mapping")
	}
	for _, change := range []func(*Settings){
		func(v *Settings) { v.PresignPUT = true; v.Writes = false },
		func(v *Settings) { v.MaxPresignExpiry = time.Second + 1 },
		func(v *Settings) { v.Endpoint += "/outside" },
		func(v *Settings) { v.SessionTimeout = -1 },
		func(v *Settings) { v.MaxParts = -1 },
		func(v *Settings) { v.Version = 2 },
	} {
		bad := value
		change(&bad)
		if Validate(bad) == nil {
			t.Fatal("invalid settings accepted")
		}
	}
	if peer.requests() != 0 {
		t.Fatal("pure validation used service")
	}
	policy, err := Recommend(value)
	if err != nil {
		t.Fatal(err)
	}
	budget := native.BudgetV1(options(value))
	if policy.Budget.WorkBytes < budget.WorkBytes+3*budget.EvidenceBytes || policy.Budget.EvidenceBytes < budget.EvidenceBytes || policy.Runtime.MaxActive != 1+budget.Active {
		t.Fatal("reservation undercharge")
	}
	overlap, err := policy.ForGenerations(2)
	if err != nil || overlap.Runtime.MaxWorkBytes != 2*policy.Runtime.MaxWorkBytes || overlap.Evidence.MaxBytes != 2*policy.Evidence.MaxBytes {
		t.Fatal("generation envelope")
	}
	if _, err := policy.ForGenerations(0); err == nil {
		t.Fatal("invalid generations")
	}
}

func FuzzSettingsPreparation(f *testing.F) {
	for _, seed := range []string{
		"{}", "{\"timeout_ns\":-1}", "{\"max_active\":17}", "{\"unknown\":true}", "{\"writes\":null}",
		"{\"max_entries\":1,\"max_entries\":2}", "{\"presign_put\":true}", "{\"max_presign_expiry_ns\":1000000001}",
		"{\"session_timeout_ns\":1000000}", "null", "[]", "{} {}",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		defaults := Settings{Name: "fuzz", Endpoint: "http://127.0.0.1:1", Plaintext: true, Region: "us-east-1", Bucket: "fixture", Prefix: "owned/", AccessKey: "fixture-access", SecretKey: "fixture-secret"}
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: defaults},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return
		}
		value, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal("accepted settings unavailable")
		}
		expected := defaults
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&expected) != nil || decoder.Decode(new(any)) != io.EOF || expected != value {
			t.Fatal("strict preparation changed data")
		}
		policy, err := Recommend(value)
		if (Validate(value) == nil) != (err == nil) {
			t.Fatal("recommendation drift")
		}
		if err != nil {
			return
		}
		runtime, err := adapters.New(context.Background(), policy.Runtime)
		if err != nil {
			t.Fatal("invalid recommendation")
		}
		defer runtime.Close(context.Background())
		inbox, err := adapters.NewInbox[Result](policy.Evidence)
		if err != nil {
			t.Fatal("invalid evidence recommendation")
		}
		endpoint, err := bind(Dependencies{Runtime: runtime, Evidence: inbox})
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := endpoint.Run(context.Background(), request("probe", "", policy.Budget.WorkBytes, policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) { _ = call.Resolve(adapters.Outcome[Result]{}) })
		if err != nil {
			t.Fatal("recommendation rejected its own root")
		}
		if snapshot, _ := receipt.Snapshot(); !snapshot.Info().Released {
			t.Fatal("finite reservation retained")
		}
		delivery, err := inbox.NextReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	})
}
