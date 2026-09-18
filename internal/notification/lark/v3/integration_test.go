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
	"context"
	"debug/buildinfo"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestActualConsumingExecutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Go, SDK, Mode string
		Closed        bool
	}
	if json.Unmarshal(output, &got) != nil || got.Go != runtime.Version() || got.SDK != "v3.12.0" || got.Mode != "lark-application" || !got.Closed {
		t.Fatal("consuming-binary evidence differs")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, dep := range info.Deps {
		if dep.Path == "github.com/larksuite/oapi-sdk-go/v3" {
			found = dep.Version == "v3.12.0" && dep.Replace == nil && dep.Sum == "h1:H8NP6YIgfEX0RBhKse25npdeZoiaDv8mrw9nCnCVFRc="
		}
	}
	if !found {
		t.Fatal("consumer lacks exact unmodified SDK")
	}
}
func TestFacadesContainNoNativeOrShutdownAuthority(t *testing.T) {
	peer := newPeer(t, nil)
	bound := bindTest(t, testOptions(peer), 1)
	source, _, err := resource.Bind(bound.assembly, bound.selected)
	if err != nil {
		t.Fatal(err)
	}
	conformance.Facade(t, &source, "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Facade(t, bound.client, "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON", "Profile",
		"Send", "Reply", "UpdateMessage", "PatchMessage", "Recall", "GetMessage", "ListMessages", "ReadUsers", "Forward", "MergeForward", "ResolveEmail",
		"UploadImage", "UploadFile", "DownloadImage", "DownloadFile", "DownloadResource",
		"CreateCard", "UpdateCard", "CardSettings", "BatchUpdateCard", "InsertElements", "UpdateElement", "PatchElement", "DeleteElement", "ElementContent",
		"SendWebhook", "Receive", "AddReaction", "DeleteReaction", "ListReactions")
}
