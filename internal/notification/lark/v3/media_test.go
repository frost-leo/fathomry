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

package lark

import (
	"bytes"
	"context"
	"errors"
	"github.com/frost-leo/fathomry/internal/fault"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
)

func TestUploadMultipartPreservesBytesAndSeparateEffects(t *testing.T) {
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	payload := []byte{0, 1, 2, 3, 255, 10}
	receipt, err := bound.client.UploadImage(context.Background(), fault.Correlation{Call: "image"}, "chart.png", payload)
	got := resolved(t, receipt, err)
	if got.Err() != nil || got.Outcome.Value.AssetKey() != "img_fixture" {
		t.Fatal("image not acknowledged")
	}
	captured := peer.snapshot()
	request := captured[len(captured)-1]
	_, parameters, err := mime.ParseMediaType(request.contentType)
	if err != nil {
		t.Fatal(err)
	}
	reader := multipart.NewReader(bytes.NewReader(request.body), parameters["boundary"])
	fields := map[string]string{}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(part)
		if part.FormName() == "image" {
			if part.FileName() != "chart.png" || !bytes.Equal(data, payload) {
				t.Fatal("uploaded media changed")
			}
		} else {
			fields[part.FormName()] = string(data)
		}
	}
	if fields["image_type"] != "message" {
		t.Fatal("avatar authority used")
	}
	drain(t, bound.inbox)
	file, err := bound.client.UploadFile(context.Background(), fault.Correlation{Call: "file"}, Upload{Name: "data.csv", Type: "stream", Data: []byte("label,value\na,1\n")})
	result := resolved(t, file, err)
	if result.Err() != nil || result.Outcome.Value.AssetKey() != "file_fixture" {
		t.Fatal("file upload failed")
	}
	if got.Outcome.Value.Effect() != Accepted {
		t.Fatal("later upload changed previous evidence")
	}
}
func TestDownloadBoundsPrivateCopiesAndAPIErrors(t *testing.T) {
	for _, mode := range []string{"ok", "oversized", "wrong-owner"} {
		t.Run(mode, func(t *testing.T) {
			peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/auth/") {
					defaultReply(w, r)
					return
				}
				switch mode {
				case "ok":
					w.Header().Set("Content-Type", "image/png")
					_, _ = io.WriteString(w, "private-image")
				case "oversized":
					w.Header().Set("Content-Type", "image/png")
					_, _ = io.WriteString(w, strings.Repeat("x", 300<<10))
				case "wrong-owner":
					_, _ = io.WriteString(w, `{"code":234007,"msg":"wrong owner"}`)
				}
			})
			bound := bindTest(t, testOptions(peer), 1)
			receipt, err := bound.client.DownloadResource(context.Background(), fault.Correlation{Call: "download"}, "om_fixture", "asset", "image")
			got := resolved(t, receipt, err)
			if mode == "ok" {
				if got.Err() != nil || string(got.Outcome.Value.Bytes()) != "private-image" {
					t.Fatal("download failed")
				}
				copy := got.Outcome.Value.Bytes()
				copy[0] = 'X'
				if string(got.Outcome.Value.Bytes()) != "private-image" {
					t.Fatal("download result aliased")
				}
			} else if got.Err() == nil || mode == "oversized" && !errors.Is(got.Err(), ErrLimit) || mode == "wrong-owner" && !errors.Is(got.Err(), ErrAPI) {
				t.Fatal("download refusal lost")
			}
		})
	}
}
