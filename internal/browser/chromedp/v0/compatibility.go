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

package chromedp

import (
	"strconv"
	"strings"

	"github.com/frost-leo/fathomry/internal/compatibility"
)

// Build inspects the actual consuming executable, not the test runner. SDK and
// generated bindings remain independent module/replacement facts.
func Build() (compatibility.Build, error) {
	return compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/chromedp/chromedp", "github.com/chromedp/cdproto", "github.com/go-json-experiment/json"}})
}

// Profile reports non-secret settings and observed CDP browser facts. It does not
// certify browser HTTP protocols, launch flags of a borrowed browser, containment,
// fingerprint equivalence or tested support. Raw arguments/paths/proxies are absent.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	profile := compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "chromedp-v0"}
	version := client.owner.versionCopy()
	product, number, ok := strings.Cut(version.product, "/")
	if ok && (product == "Chrome" || product == "Chromium" || product == "HeadlessChrome") && numericVersion(number) {
		profile.ServiceVersion = compatibility.Fact{Kind: compatibility.Observed, Value: product + "-" + number}
	}
	if numericVersion(version.protocol) {
		profile.Protocol = compatibility.Fact{Kind: compatibility.Observed, Value: "cdp-" + version.protocol}
	}
	mode := "owned-process"
	if value.RemoteURL != "" {
		mode = "borrowed-browser"
	}
	profile.ServiceMode = compatibility.Fact{Kind: compatibility.Declared, Value: mode}
	if version.product != "" {
		profile.ServiceMode.Kind = compatibility.Observed
	}
	profile.Options = []compatibility.Option{
		{Name: "new-window", Value: strconv.FormatBool(value.NewWindow)},
		{Name: "assembly-readiness", Value: strconv.FormatBool(value.CheckReady)},
		{Name: "launch-configuration", Value: "private-unattested"},
		{Name: "http-attempts", Value: "unobserved"},
		{Name: "native-memory-limit", Value: "external"},
	}
	for _, entry := range []struct {
		name  string
		value int64
	}{
		{"max-sessions", int64(value.MaxSessions)}, {"queued-calls", int64(value.QueuedCalls)}, {"max-commands", int64(value.MaxCommands)},
		{"max-command-bytes", value.MaxCommandBytes}, {"max-result-bytes", value.MaxResultBytes}, {"max-event-bytes", value.MaxEventBytes},
		{"max-events", int64(value.MaxEvents)}, {"admission-timeout-ns", int64(value.AdmissionTimeout)},
		{"startup-timeout-ns", int64(value.StartupTimeout)}, {"session-timeout-ns", int64(value.SessionTimeout)}, {"cleanup-timeout-ns", int64(value.CleanupTimeout)},
	} {
		profile.Options = append(profile.Options, compatibility.Option{Name: entry.name, Value: strconv.FormatInt(entry.value, 10)})
	}
	return profile
}

func numericVersion(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if char != '.' && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

// LaunchArgumentsCopy deliberately exposes potentially sensitive arguments
// supplied to the owned executable, not attested effective Chrome settings.
// False means no owned launch was attempted (including borrowed-browser mode).
func (client *Client) LaunchArgumentsCopy() ([]string, bool) {
	if client == nil || client.owner == nil {
		return nil, false
	}
	client.owner.mu.Lock()
	defer client.owner.mu.Unlock()
	return append([]string(nil), client.owner.launchArguments...), client.owner.launchArguments != nil
}
