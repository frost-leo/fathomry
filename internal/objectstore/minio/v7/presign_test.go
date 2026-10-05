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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPresignGrantsConditionsAndPrivacy(t *testing.T) {
	server, options := newPeer(t)
	options.PresignGET, options.PresignHEAD, options.PresignPUT = true, true, true
	options.SessionToken = "static-token-canary"
	fixture := bindFixture(t, options, 8)
	before := server.count()
	for _, method := range []string{"GET", "HEAD", "PUT"} {
		request := SignRequest{Method: method, Address: Address{Key: "owned/capability-canary"}, Expiry: time.Second}
		if method == "PUT" {
			request.IfAbsent = true
		} else {
			request.MatchETag = "etag"
			request.Address.VersionID = "version+%/"
		}
		receipt, err := fixture.client.Presign(deadline(t), correlation("sign-"+method), request)
		result := settle(t, receipt, err)
		signed, present := result.Outcome.Value.Delegation()
		if result.Err() != nil || !present || !signed.StaticToken() || result.Outcome.Value.Transfer().Effect != NotSubmitted {
			t.Fatal("issuance evidence", result.Err())
		}
		parsed, err := url.Parse(signed.URL())
		if err != nil || parsed.Query().Get("X-Amz-Expires") != "1" || signed.Method() != method {
			t.Fatal("signed contract")
		}
		if method == "PUT" && (signed.HeadersCopy().Get("If-None-Match") != "*" || !strings.Contains(parsed.Query().Get("X-Amz-SignedHeaders"), "if-none-match")) {
			t.Fatal("unsigned condition")
		}
		if method != "PUT" && parsed.Query().Get("versionId") != request.Address.VersionID {
			t.Fatal("version changed")
		}
		headers := signed.HeadersCopy()
		headers.Set("If-Match", "changed")
		if signed.HeadersCopy().Get("If-Match") == "changed" {
			t.Fatal("aliased headers")
		}
		if strings.Contains(fmt.Sprintf("%+v %#v", signed, &signed), "canary") {
			t.Fatal("sensitive default format")
		}
		if _, err := json.Marshal(signed); err == nil {
			t.Fatal("capability serialized")
		}
	}
	for _, request := range []SignRequest{
		{Method: "DELETE", Address: Address{Key: "owned/key"}, Expiry: time.Second},
		{Method: "PUT", Address: Address{Key: "outside/key"}, Expiry: time.Second},
		{Method: "GET", Address: Address{Key: "owned/../key"}, Expiry: time.Second},
		{Method: "PUT", Address: Address{Key: "owned/key", VersionID: "v"}, Expiry: time.Second},
		{Method: "GET", Address: Address{Key: "owned/key"}, Expiry: time.Second + 1},
		{Method: "GET", Address: Address{Key: "owned/key"}, Expiry: 16 * time.Minute},
		{Method: "GET", Address: Address{Key: "owned/key"}, Expiry: 0},
	} {
		if receipt, err := fixture.client.Presign(deadline(t), correlation("reject"), request); err == nil || receipt != nil {
			t.Fatal("invalid signing request accepted")
		}
	}
	if server.count() != before {
		t.Fatal("signing performed network I/O")
	}
	denied := options
	denied.PresignGET = false
	other := bindFixture(t, denied, 1)
	if _, err := other.client.Presign(deadline(t), correlation("grant"), SignRequest{Method: "GET", Address: Address{Key: "owned/key"}, Expiry: time.Second}); !errors.Is(err, ErrAuthority) {
		t.Fatal("missing grant")
	}
}
