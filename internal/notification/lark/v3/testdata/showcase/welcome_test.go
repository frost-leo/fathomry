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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

func sampleRepositorySnapshot() (repositorySnapshot, time.Time) {
	now := time.Date(2026, 9, 17, 15, 0, 0, 0, time.UTC)
	since := time.Date(2025, 9, 18, 0, 0, 0, 0, time.UTC)
	created := "2026-09-07T18:07:21Z"
	snapshot := repositorySnapshot{Version: 1, Repository: "frost-leo/fathomry", RepositoryID: 1360531276,
		Branch: "develop", Head: strings.Repeat("a", 40), CapturedAt: now.Format(time.RFC3339), CompletedAt: now.Format(time.RFC3339),
		CreatedAt: created, Since: since.Format(time.RFC3339), Stars: 1, Commits: 2, Languages: map[string]int{"Go": 900, "Shell": 100},
		StarWeeks: []reportWeek{{Week: now.AddDate(0, 0, -7).Unix(), Total: 1, Days: []int{1, 0, 0, 0, 0, 0, 0}}}}
	for cursor := since; !cursor.After(now); cursor = cursor.AddDate(0, 0, 1) {
		day := cursor.Format("2006-01-02")
		count := 0
		if day == "2026-09-17" {
			count = 2
		}
		snapshot.Calendar = append(snapshot.Calendar, reportDay{Day: day, Count: count, Applicable: day >= created[:10]})
	}
	for offset := -11; offset <= 0; offset++ {
		month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, offset, 0).Format("2006-01")
		snapshot.Months = append(snapshot.Months, reportMonth{Month: month, Applicable: month >= created[:7]})
	}
	snapshot.Months[11].Issues, snapshot.Months[11].PRs = 2, 1
	return snapshot, now
}

func writeSnapshot(t testing.TB, snapshot repositorySnapshot) string {
	t.Helper()
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWelcomeUsesRealCopyWithoutInventedStatistics(t *testing.T) {
	payload, err := welcome("", false)
	if err != nil {
		t.Fatal(err)
	}
	data := payload.card.JSON().Bytes()
	var compact bytes.Buffer
	if err := json.Compact(&compact, data); err != nil {
		t.Fatal(err)
	}
	data = compact.Bytes()
	for _, text := range []string{"Welcome to Fathomry.", "Made for the work ahead.", "https://github.com/frost-leo/fathomry", `"tag":"collapsible_panel"`} {
		if !bytes.Contains(data, []byte(text)) {
			t.Fatalf("welcome is missing %s", text)
		}
	}
	for _, text := range []string{"Synthetic", "460", "chart_spec", "issue #64", "license_notice"} {
		if bytes.Contains(data, []byte(text)) {
			t.Fatalf("test data or metadata leaked into the Welcome message: %s", text)
		}
	}
	if payload.table != nil {
		t.Fatal("plain welcome invented a data attachment")
	}
}

func TestCallerFilesRejectNamedPipesWithoutAWriter(t *testing.T) {
	for _, read := range []func(string) error{
		func(path string) error { _, err := readSnapshot(path, false); return err },
		func(path string) error { _, err := readSettings(path); return err },
	} {
		path := filepath.Join(t.TempDir(), "not-a-regular-file")
		if err := syscall.Mkfifo(path, 0600); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- read(path) }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("named pipe accepted as a regular file")
			}
		case <-time.After(time.Second):
			writer, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
			if err != nil {
				t.Fatal(err)
			}
			_ = writer.Close()
			<-done
			t.Fatal("regular-file validation waited for a FIFO writer")
		}
	}
}

func TestWelcomeReportPreservesSnapshotDataAndNativeCharts(t *testing.T) {
	snapshot, _ := sampleRepositorySnapshot()
	path := writeSnapshot(t, snapshot)
	payload, err := welcome(path, false)
	if err != nil {
		t.Fatal(err)
	}
	data := payload.card.JSON().Bytes()
	if bytes.Count(data, []byte(`"tag":"chart"`)) != 4 || bytes.Count(data, []byte(`"tag":"table"`)) != 1 ||
		!bytes.Contains(data, []byte(snapshot.Head)) || !bytes.Contains(data, []byte(snapshot.CapturedAt)) {
		t.Fatal("native charts, source revision, timestamp or tables lost")
	}
	for _, row := range []string{"commits,day,2025-09-18,not_applicable", "commits,day,2026-09-17,2", "language_bytes,Go,2026-09-17T15:00:00Z,900", "opened,PRs,2026-09,1"} {
		if !bytes.Contains(payload.table, []byte(row)) {
			t.Fatalf("captured values changed: %s", row)
		}
	}
	if _, err := welcome(path, true); err == nil {
		t.Fatal("historical preview was accepted for fresh sending")
	}
	var card struct {
		Header json.RawMessage `json:"header"`
		Body   struct {
			Padding  string `json:"padding"`
			Spacing  string `json:"vertical_spacing"`
			Elements []struct {
				ID       string `json:"element_id"`
				Tag      string `json:"tag"`
				Expanded bool   `json:"expanded"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &card); err != nil {
		t.Fatal(err)
	}
	if len(card.Header) != 0 || card.Body.Padding != "24px 24px 20px 24px" || card.Body.Spacing != "0px" {
		t.Fatal("repository insertion lost the headerless Welcome layout")
	}
	charts, closing := 0, 0
	for _, element := range card.Body.Elements {
		if element.Tag == "chart" {
			if closing != 0 {
				t.Fatal("report charts escaped below the closing message")
			}
			charts++
		}
		if element.ID == "closing" {
			closing++
		}
		if element.Tag == "collapsible_panel" && element.Expanded {
			t.Fatal("secondary details displaced the Welcome hero")
		}
	}
	if charts != 4 || closing != 1 {
		t.Fatal("report insertion lost native charts or duplicated the closing")
	}
	var decoded struct {
		Body struct {
			Elements []map[string]any `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	var visit func(map[string]any, int)
	visit = func(element map[string]any, depth int) {
		if element["tag"] == "table" && depth != 0 {
			t.Fatal("Feishu does not support tables inside containers")
		}
		for _, key := range []string{"elements", "columns"} {
			children, _ := element[key].([]any)
			for _, child := range children {
				if child, ok := child.(map[string]any); ok {
					visit(child, depth+1)
				}
			}
		}
	}
	for _, element := range decoded.Body.Elements {
		visit(element, 0)
	}
}

func TestWelcomeRejectsUnknownAmbiguousAndIncompleteReportValues(t *testing.T) {
	snapshot, _ := sampleRepositorySnapshot()
	raw, _ := json.Marshal(snapshot)
	for name, change := range map[string]func(map[string]json.RawMessage){
		"missing-count":  func(fields map[string]json.RawMessage) { delete(fields, "stars") },
		"null-count":     func(fields map[string]json.RawMessage) { fields["stars"] = json.RawMessage("null") },
		"aliased-count":  func(fields map[string]json.RawMessage) { fields["Stars"] = json.RawMessage("100") },
		"wrong-identity": func(fields map[string]json.RawMessage) { fields["repository_id"] = json.RawMessage("1") },
		"partial-days":   func(fields map[string]json.RawMessage) { fields["calendar"] = json.RawMessage("[]") },
		"unknown-star-days": func(fields map[string]json.RawMessage) {
			fields["star_weeks"] = json.RawMessage(`[{"week":1789081200,"total":0,"days":[null,0,0,0,0,0,0]}]`)
		},
		"unknown-language": func(fields map[string]json.RawMessage) { fields["languages"] = json.RawMessage(`{"Go":null}`) },
		"wrong-total":      func(fields map[string]json.RawMessage) { fields["commits"] = json.RawMessage("3") },
		"markdown-branch": func(fields map[string]json.RawMessage) {
			fields["branch"] = json.RawMessage(`"[Injected link](https://example.test)"`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(raw, &fields)
			change(fields)
			data, _ := json.Marshal(fields)
			path := filepath.Join(t.TempDir(), "invalid.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := welcome(path, false); err == nil {
				t.Fatal("invalid snapshot was converted into a successful report")
			}
		})
	}
}

func TestIndependentCallerSendsOneWelcomeWithoutAssetUpload(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"fixture-token","expire":7200}`)
		case "/open-apis/contact/v3/users/batch_get_id":
			_, _ = io.WriteString(w, `{"code":0,"data":{"user_list":[{"email":"receiver@example.test","user_id":"ou_receiver"}]}}`)
		case "/open-apis/im/v1/messages":
			var message struct {
				ID      string `json:"receive_id"`
				Kind    string `json:"msg_type"`
				Content string `json:"content"`
			}
			if json.NewDecoder(r.Body).Decode(&message) != nil || message.ID != "ou_receiver" || message.Kind != "interactive" ||
				!strings.Contains(message.Content, "Welcome to Fathomry.") || strings.Contains(message.Content, "Synthetic") {
				t.Error("wrong Welcome payload or resolved recipient")
			}
			_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_welcome"}}`)
		default:
			t.Error("plain Welcome unexpectedly uploaded an asset or exercised lifecycle APIs")
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	options := lark.OptionsV1{Name: "welcome-test", Profile: "application", AppID: "fixture-app", AppSecret: "fixture-secret", BaseURL: server.URL,
		RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))}
	payload, err := welcome("", true)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := deliver(context.Background(), options, lark.Recipient{Type: "email", ID: "receiver@example.test"}, false, payload, &output); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || bytes.Count(output.Bytes(), []byte("\n")) != 2 {
		t.Fatal("unexpected Welcome request or independent evidence count")
	}
	if bytes.Contains(output.Bytes(), []byte("fixture-secret")) || bytes.Contains(output.Bytes(), []byte("receiver@example.test")) {
		t.Fatal("private inputs escaped Welcome diagnostics")
	}
}
