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

package redis

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

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestComposedProfileAndFacade(t *testing.T) {
	address := peer(t, func([]string) string { return "+PONG\r\n" })
	client, _, inbox, _ := bindTest(t, testOptions(address), 4)
	profile := client.Profile()
	profile.Options[0].Value = "modified"
	if client.Profile().Options[0].Value == "modified" {
		t.Fatal("profile aliases caller")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/redis/go-redis/v9"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, client.access, client.Profile(), []compatibility.Requirement{
		{Guarantee: "bounded-redis", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}},
	}, nil)
	if err != nil || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("declarations or missing evidence certified service support")
	}
	conformance.Facade(t, client, "Execute", "Pipeline", "Submit", "Automatic", "Dedicated", "Watch", "Transaction", "Subscribe", "Stats", "Profile",
		"String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, client, new(Client))
	got := executeTest(t, client, "ping", "PING")
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, inbox)
}
func TestActualConsumingExecutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build failed: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal("consumer execution failed")
	}
	var result struct {
		Go                   string
		ConstructedAndClosed bool
		QueryExecuted        bool
		SDK                  string
	}
	if json.Unmarshal(output, &result) != nil || result.Go != runtime.Version() || !result.ConstructedAndClosed || result.QueryExecuted || result.SDK != "v9.22.0" {
		t.Fatal("consumer execution evidence changed")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, module := range info.Deps {
		if module.Path == "github.com/redis/go-redis/v9" {
			found = module.Version == "v9.22.0" && module.Replace == nil && module.Sum != ""
		}
	}
	if !found {
		t.Fatal("consumer is not using the verified, unmodified official SDK")
	}
}
