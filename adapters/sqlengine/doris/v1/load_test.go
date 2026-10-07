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

package doris

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const goodLoad = `{"Label":"gh111-run","TxnId":42,"Status":"Success","NumberTotalRows":1,"NumberLoadedRows":1,"NumberFilteredRows":0,"NumberUnselectedRows":0}`

func TestLoadAndLabelFacts(t *testing.T) {
	for _, test := range []struct {
		name, body                 string
		want                       error
		state                      LoadState
		known, complete, duplicate bool
	}{
		{"success", goodLoad, nil, LoadVisible, true, true, false},
		{"committed", strings.Replace(goodLoad, "Success", "Publish Timeout", 1), ErrUncertain, LoadCommitted, true, false, false},
		{"filtered", strings.Replace(strings.Replace(goodLoad, `"NumberLoadedRows":1`, `"NumberLoadedRows":0`, 1), `"NumberFilteredRows":0`, `"NumberFilteredRows":1`, 1), ErrRowQuality, LoadVisible, true, false, false},
		{"missing", strings.Replace(goodLoad, `"NumberFilteredRows":0,`, "", 1), ErrProtocol, LoadVisible, false, false, false},
		{"duplicate", `{"Label":"gh111-run","Status":"Label Already Exists","ExistingJobStatus":"FINISHED"}`, ErrDuplicate, LoadUnknown, false, false, true},
		{"reject", strings.Replace(goodLoad, "Success", "Fail", 1), ErrLoad, LoadRejected, true, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if string(body) != `[{"value":18446744073709551615}]` || r.Header.Get("strict_mode") != "true" || r.Header.Get("max_filter_ratio") != "0" || r.Header.Get("group_commit") != "off_mode" || r.Header.Get("two_phase_commit") != "false" {
					t.Error("strict public options lost")
				}
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			options := Settings{Name: "load", HTTPOrigins: []string{server.URL}, Database: "fixture", User: "user", Plaintext: true}
			owner, inbox, _ := testOwner(t, options, 0)
			options.HTTPOrigins[0] = "http://127.0.0.1:1"
			batch := Batch{Table: "rows", Label: "gh111-run", JSON: []byte(`[{"value":18446744073709551615}]`)}
			snapshot := (reader{t}).inspect(owner.Client().StreamLoad(testContext(t), batch))
			if test.want == nil && snapshot.Err() != nil || test.want != nil && !errors.Is(snapshot.Err(), test.want) {
				t.Fatal("classification", snapshot.Err())
			}
			value := valueOf(t, snapshot)
			load, ok := value.Load()
			if !ok || load.State != test.state || load.RowsKnown != test.known || value.Complete() != test.complete || load.Duplicate != test.duplicate || load.PayloadSHA256 != sha256.Sum256(batch.JSON) || calls.Load() != 1 {
				t.Fatal("effect facts flattened")
			}
			if test.duplicate && load.ExistingJobStatus != "FINISHED" {
				t.Fatal("duplicate status lost")
			}
			ack(t, inbox)
		})
	}
	for _, state := range []string{"UNKNOWN", "PREPARE", "PRECOMMITTED", "COMMITTED", "VISIBLE", "ABORTED"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprintf(w, `{"code":0,"msg":"success","data":%q}`, state)
		}))
		owner, inbox, _ := testOwner(t, Settings{Name: "label", HTTPOrigins: []string{server.URL}, Database: "fixture", User: "user", Plaintext: true}, 0)
		snapshot := (reader{t}).inspect(owner.Client().InspectLabel(testContext(t), "gh111-run"))
		value := valueOf(t, snapshot)
		load, ok := value.Load()
		if !ok || load.RowsKnown || load.TransactionKnown || load.Table != "" || load.PayloadSHA256 != [32]byte{} || value.Complete() != (state == "VISIBLE") {
			t.Fatal("label invented payload witness")
		}
		if state == "UNKNOWN" && !errors.Is(snapshot.Err(), ErrUncertain) {
			t.Fatal("expired/unknown label became absent")
		}
		ack(t, inbox)
		server.Close()
	}
}
func TestLostLoadReplyDoesNotRedispatch(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	owner, inbox, _ := testOwner(t, Settings{Name: "lost", HTTPOrigins: []string{server.URL}, Database: "fixture", User: "user", Plaintext: true}, 0)
	snapshot := (reader{t}).inspect(owner.Client().StreamLoad(context.Background(), Batch{Table: "rows", Label: "gh111-run", JSON: []byte("[{}]")}))
	load, _ := valueOf(t, snapshot).Load()
	if snapshot.Err() == nil || load.State != LoadUnknown || calls.Load() != 1 {
		t.Fatal("lost reply replayed")
	}
	ack(t, inbox)
}
