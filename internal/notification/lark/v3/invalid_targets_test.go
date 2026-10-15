/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestInvalidTargetEchoesRemainPartial(t *testing.T) {
	response := `{"code":0,"data":{"invalid_user_id_list":["bad/id"]}}`
	var native larkim.UrgentAppMessageResp
	if err := json.Unmarshal([]byte(response), &native); err != nil || !native.Success() || len(native.Data.InvalidUserIdList) != 1 || native.Data.InvalidUserIdList[0] != "bad/id" {
		t.Fatal("selected native response did not preserve invalid ID", err)
	}
	for _, operation := range []Operation{IMV1MessageUrgentApp, IMV1MessageUrgentPhone, IMV1MessageUrgentSms} {
		t.Run(string(operation), func(t *testing.T) {
			peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if string(body) != `{"user_id_list":["ou_good","bad/id"]}` {
					t.Error("invalid input echo fixture was not transmitted exactly")
				}
				_, _ = io.WriteString(w, response)
			})
			options := testOptions(peer)
			options.Profile, options.AppSecret = "tenant-token", ""
			options.TenantKey, options.TenantToken = "tenant_fixture", "synthetic"
			options.TokenExpiresAt = time.Now().Add(time.Hour)
			bound := bindTest(t, options, 1)
			request, err := NewRequest(operation, []string{"om_fixture"}, nil, jsonValue(t, `{"user_id_list":["ou_good","bad/id"]}`))
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := bound.client.Do(context.Background(), fault.Correlation{Call: "invalid-id"}, request, RequestOptions{})
			result := resolved(t, receipt, err)
			ids := result.Outcome.Value.InvalidUserIDs()
			if result.Err() != nil || result.Outcome.Value.Effect() != Partial || len(ids) != 1 || ids[0] != "bad/id" {
				t.Fatal("invalid-ID evidence was treated as a successful identity", result.Err(), result.Outcome.Value.Effect())
			}
		})
	}
}

func TestInvalidTargetNamespacesCheckTypesNotIdentitySyntax(t *testing.T) {
	for _, name := range []string{"invalid_department_ids", "invalid_open_ids", "invalid_user_ids", "invalid_union_ids"} {
		t.Run(name, func(t *testing.T) {
			value := Result{data: `{"` + name + `":["bad/id",""]}`}
			if err := value.readBatchPartial(); err != nil {
				t.Fatal("invalid native IDs cannot require valid-ID grammar", err)
			}
			ids := value.InvalidTargets()[name]
			if len(ids) != 2 || ids[0] != "bad/id" || ids[1] != "" {
				t.Fatal("invalid target evidence changed")
			}
			malformed := Result{data: `{"` + name + `":[17]}`}
			if err := malformed.readBatchPartial(); !errors.Is(err, ErrResponse) {
				t.Fatal("malformed member lost its response failure", err)
			}
		})
	}
	malformed := Result{data: `{"invalid_user_id_list":[false]}`}
	if err := malformed.readPartial("invalid_user_id_list"); !errors.Is(err, ErrResponse) {
		t.Fatal("malformed urgency member lost its response failure", err)
	}
}
