/*
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
	"crypto/tls"
	"encoding/json"
	"time"
	"unicode/utf8"

	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	source "github.com/frost-leo/fathomry/internal/resource"
)

const ProviderID = native.ProviderID

// Settings is loadable source data. Nil positive limits select Internal defaults.
// Explicit zero limits are validated, not silently defaulted. Durations are ns.
// No executable, endpoint, profile or headless selection is discovered.
// Headers configures the CDP handshake, not HTTP requests made by a page.
type Settings struct {
	Name               string              `json:"name"`
	Version            uint32              `json:"version"`
	ExecPath           string              `json:"exec_path"`
	RemoteURL          string              `json:"remote_url"`
	TempDir            string              `json:"temp_dir"`
	ProfileDir         string              `json:"profile_dir"`
	Environment        []string            `json:"environment"`
	InheritEnvironment bool                `json:"inherit_environment"`
	Flags              map[string]string   `json:"flags"`
	Headers            map[string][]string `json:"headers"`
	NewWindow          bool                `json:"new_window"`
	CheckReady         bool                `json:"check_ready"`
	MaxSessions        *int                `json:"max_sessions"`
	MaxPages           *int                `json:"max_pages"`
	MaxStreams         *int                `json:"max_streams"`
	MaxSubscriptions   *int                `json:"max_subscriptions"`
	MaxBindings        *int                `json:"max_bindings"`
	MaxBindingCalls    *int                `json:"max_binding_calls"`
	MaxObjectGroups    *int                `json:"max_object_groups"`
	MaxObjects         *int                `json:"max_objects"`
	MaxStreamBytes     *int64              `json:"max_stream_bytes"`
	QueuedCalls        int                 `json:"queued_calls"`
	MaxCommands        *int                `json:"max_commands"`
	MaxCommandBytes    *int64              `json:"max_command_bytes"`
	MaxResultBytes     *int64              `json:"max_result_bytes"`
	MaxEventBytes      *int64              `json:"max_event_bytes"`
	MaxEvents          *int                `json:"max_events"`
	AdmissionTimeout   *time.Duration      `json:"admission_timeout_ns"`
	StartupTimeout     *time.Duration      `json:"startup_timeout_ns"`
	SessionTimeout     *time.Duration      `json:"session_timeout_ns"`
	CleanupTimeout     *time.Duration      `json:"cleanup_timeout_ns"`
}

// NativeOptions borrows immutable TLS callbacks and private keys until source close.
// Prepare copies TLS containers; this affects CDP control, not browser HTTP traffic.
type NativeOptions struct {
	private
	TLS *tls.Config
}
type Dependencies struct {
	private
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}
type Prepared struct {
	private
	native       native.Prepared
	policy       browser.Policy
	ownedProcess bool
}

func Prepare(settings Settings, options NativeOptions) (Prepared, error) {
	texts := []string{settings.Name, settings.ExecPath, settings.RemoteURL, settings.TempDir, settings.ProfileDir}
	texts = append(texts, settings.Environment...)
	for key, value := range settings.Flags {
		texts = append(texts, key, value)
	}
	for key, values := range settings.Headers {
		texts = append(texts, key)
		texts = append(texts, values...)
	}
	for _, value := range texts {
		if !utf8.ValidString(value) {
			return Prepared{}, fail(ErrInput, "utf8")
		}
	}
	data, err := json.Marshal(settings)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	delete(fields, "name")
	delete(fields, "version")
	for name, value := range fields {
		if string(value) == "null" {
			delete(fields, name)
		}
	}
	data, err = json.Marshal(fields)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	prepared, err := native.PrepareV1(native.OptionsV1{Name: settings.Name, Version: settings.Version, Native: native.NativeOptionsV1{TLS: options.TLS}}, source.Layer{Kind: source.Local, Content: data})
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	policy := policyFor(prepared.Budget())
	if policy.Runtime.MaxWorkBytes > 1<<40 || policy.Runtime.MaxQueuedBytes > 1<<40 || policy.Evidence.MaxBytes > 1<<40 || policy.Evidence.Capacity > 65536 {
		return Prepared{}, fail(ErrLimit, "policy")
	}
	return Prepared{native: prepared, policy: policy, ownedProcess: settings.RemoteURL == ""}, nil
}
func Validate(settings Settings) error { _, err := Prepare(settings, NativeOptions{}); return err }
