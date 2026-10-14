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
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 configures one instance. Version zero selects format 1. Go zeros use
// the technical defaults below; explicit layer zeros are validated as supplied.
// No executable, remote browser, headless mode, proxy or business profile is chosen
// implicitly. Exactly one of ExecPath (absolute) and RemoteURL is required.
//
// Local mode creates a fresh private profile below TempDir (empty uses the OS
// temporary directory), unless ProfileDir explicitly selects caller property.
// Persistent directories use an exclusive lock file and are never deleted.
// Environment inheritance is opt-in. The debugging port is loopback and ephemeral. Flags are
// copied. Empty flag values mean bare switches; others mean --name=value.
// Lifecycle/debugging/profile switches are reserved. No sandbox-disabling switch
// is accepted. Other flags are trusted construction inputs, not a security policy.
// RemoteURL must be an exact ws(s) browser URL; the browser/process/profile and its
// launch flags remain borrowed. Existing context/page use needs an explicit
// SessionOptionsV1 borrow grant and a native membership check.
type OptionsV1 struct {
	private
	Native             NativeOptionsV1
	Name               string
	Version            uint32
	ExecPath           string
	RemoteURL          string
	TempDir            string
	ProfileDir         string
	Environment        []string
	InheritEnvironment bool
	Flags              map[string]string
	Headers            map[string][]string
	// NewWindow controls CDP target creation, independently of launch/headless flags.
	NewWindow bool
	// CheckReady enables an assembly readiness check; otherwise startup is lazy.
	CheckReady bool
	// MaxSessions defaults to 4 and includes contexts pending failed cleanup.
	// QueuedCalls defaults to zero. These are not HTTP request/connection limits.
	MaxSessions      int
	MaxPages         int
	MaxStreams       int
	MaxSubscriptions int
	MaxBindings      int
	MaxBindingCalls  int
	MaxObjectGroups  int
	MaxObjects       int
	MaxStreamBytes   int64
	QueuedCalls      int
	// MaxCommands defaults to 256 per session; it counts extension CDP commands,
	// not SDK initialization commands, navigation requests, or browser HTTP traffic.
	MaxCommands int
	// Byte limits bound encoded extension commands, cumulative decoded-command
	// inputs to callers, retained evidence and the event queue, not SDK heap/RSS.
	// Defaults: 1 MiB command, 2 MiB results, 2 MiB events, 256 queued events.
	MaxCommandBytes int64
	MaxResultBytes  int64
	MaxEventBytes   int64
	MaxEvents       int
	// Durations and YAML *_ns fields use nanoseconds. Defaults: admission 1s,
	// startup 15s, whole session 30s, cleanup 5s. Cancellation is cooperative.
	AdmissionTimeout time.Duration
	StartupTimeout   time.Duration
	SessionTimeout   time.Duration
	CleanupTimeout   time.Duration
}

type settings struct {
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
	MaxSessions        int                 `json:"max_sessions"`
	MaxPages           int                 `json:"max_pages"`
	MaxStreams         int                 `json:"max_streams"`
	MaxSubscriptions   int                 `json:"max_subscriptions"`
	MaxBindings        int                 `json:"max_bindings"`
	MaxBindingCalls    int                 `json:"max_binding_calls"`
	MaxObjectGroups    int                 `json:"max_object_groups"`
	MaxObjects         int                 `json:"max_objects"`
	MaxStreamBytes     int64               `json:"max_stream_bytes"`
	QueuedCalls        int                 `json:"queued_calls"`
	MaxCommands        int                 `json:"max_commands"`
	MaxCommandBytes    int64               `json:"max_command_bytes"`
	MaxResultBytes     int64               `json:"max_result_bytes"`
	MaxEventBytes      int64               `json:"max_event_bytes"`
	MaxEvents          int                 `json:"max_events"`
	AdmissionTimeout   time.Duration       `json:"admission_timeout_ns"`
	StartupTimeout     time.Duration       `json:"startup_timeout_ns"`
	SessionTimeout     time.Duration       `json:"session_timeout_ns"`
	CleanupTimeout     time.Duration       `json:"cleanup_timeout_ns"`
}

func defaults(options OptionsV1) settings {
	value := settings{ExecPath: options.ExecPath, RemoteURL: options.RemoteURL, TempDir: options.TempDir, Flags: options.Flags, NewWindow: options.NewWindow, CheckReady: options.CheckReady, MaxSessions: options.MaxSessions, QueuedCalls: options.QueuedCalls, MaxCommands: options.MaxCommands, MaxCommandBytes: options.MaxCommandBytes, MaxResultBytes: options.MaxResultBytes, MaxEventBytes: options.MaxEventBytes, MaxEvents: options.MaxEvents, AdmissionTimeout: options.AdmissionTimeout, StartupTimeout: options.StartupTimeout, SessionTimeout: options.SessionTimeout, CleanupTimeout: options.CleanupTimeout}
	if value.MaxSessions == 0 {
		value.MaxSessions = 4
	}
	value.ProfileDir, value.Environment, value.InheritEnvironment = options.ProfileDir, options.Environment, options.InheritEnvironment
	value.Headers = options.Headers
	value.MaxPages = options.MaxPages
	value.MaxSubscriptions = options.MaxSubscriptions
	value.MaxBindings, value.MaxBindingCalls = options.MaxBindings, options.MaxBindingCalls
	value.MaxObjectGroups, value.MaxObjects = options.MaxObjectGroups, options.MaxObjects
	if value.MaxObjectGroups == 0 {
		value.MaxObjectGroups = 8
	}
	if value.MaxObjects == 0 {
		value.MaxObjects = 128
	}
	if value.MaxBindings == 0 {
		value.MaxBindings = 8
	}
	if value.MaxBindingCalls == 0 {
		value.MaxBindingCalls = 8
	}
	if value.MaxSubscriptions == 0 {
		value.MaxSubscriptions = 8
	}
	value.MaxStreams, value.MaxStreamBytes = options.MaxStreams, options.MaxStreamBytes
	if value.MaxStreams == 0 {
		value.MaxStreams = 4
	}
	if value.MaxStreamBytes == 0 {
		value.MaxStreamBytes = 64 << 20
	}
	if value.MaxPages == 0 {
		value.MaxPages = 4
	}
	if value.MaxCommands == 0 {
		value.MaxCommands = 256
	}
	if value.MaxCommandBytes == 0 {
		value.MaxCommandBytes = 1 << 20
	}
	if value.MaxResultBytes == 0 {
		value.MaxResultBytes = 2 << 20
	}
	if value.MaxEventBytes == 0 {
		value.MaxEventBytes = 2 << 20
	}
	if value.MaxEvents == 0 {
		value.MaxEvents = 256
	}
	if value.AdmissionTimeout == 0 {
		value.AdmissionTimeout = time.Second
	}
	if value.StartupTimeout == 0 {
		value.StartupTimeout = 15 * time.Second
	}
	if value.SessionTimeout == 0 {
		value.SessionTimeout = 30 * time.Second
	}
	if value.CleanupTimeout == 0 {
		value.CleanupTimeout = 5 * time.Second
	}
	return value
}
func validate(value settings) error {
	if value.MaxObjectGroups < 1 || value.MaxObjectGroups > 128 || value.MaxObjects < 1 || value.MaxObjects > 4096 {
		return failure(ErrInput, "objects")
	}
	if value.MaxBindings < 1 || value.MaxBindings > 128 || value.MaxBindingCalls < 1 || value.MaxBindingCalls > 128 {
		return failure(ErrInput, "bindings")
	}
	if value.MaxSubscriptions < 1 || value.MaxSubscriptions > 128 {
		return failure(ErrInput, "subscriptions")
	}
	if value.MaxStreams < 1 || value.MaxStreams > 128 || value.MaxStreamBytes < 1 || value.MaxStreamBytes > 4<<30 {
		return failure(ErrInput, "streams")
	}
	if value.ProfileDir != "" && (!filepath.IsAbs(value.ProfileDir) || value.TempDir != "" || value.RemoteURL != "") {
		return failure(ErrInput, "profile-directory")
	}
	if len(value.Environment) > 256 || value.RemoteURL != "" && (len(value.Environment) != 0 || value.InheritEnvironment) {
		return failure(ErrInput, "environment")
	}
	for _, entry := range value.Environment {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || name == "" || len(entry) > 8192 || strings.ContainsRune(entry, '\x00') {
			return failure(ErrInput, "environment")
		}
	}
	if value.MaxPages < 1 || value.MaxPages > 128 {
		return failure(ErrInput, "pages")
	}
	if (value.ExecPath == "") == (value.RemoteURL == "") || value.ExecPath != "" && !filepath.IsAbs(value.ExecPath) || value.TempDir != "" && !filepath.IsAbs(value.TempDir) {
		return failure(ErrInput, "browser")
	}
	if value.RemoteURL != "" {
		address, err := url.Parse(value.RemoteURL)
		if err != nil || address.Host == "" || (address.Scheme != "ws" && address.Scheme != "wss") || !strings.HasPrefix(address.Path, "/devtools/browser/") || len(strings.TrimPrefix(address.Path, "/devtools/browser/")) == 0 || address.Fragment != "" || len(value.Flags) != 0 || value.TempDir != "" {
			return failure(ErrInput, "remote")
		}
	}
	if len(value.Flags) > 128 {
		return failure(ErrInput, "flags")
	}
	for name, val := range value.Flags {
		if name == "" || name[0] < 'a' || name[0] > 'z' || len(name) > 128 || len(val) > 8192 || strings.ContainsAny(val, "\x00\r\n") {
			return failure(ErrInput, "flag")
		}
		for _, letter := range name {
			if letter != '-' && !(letter >= 'a' && letter <= 'z') && !(letter >= '0' && letter <= '9') {
				return failure(ErrInput, "flag")
			}
		}
		switch name {
		case "user-data-dir", "remote-debugging-port", "remote-debugging-address", "remote-debugging-pipe", "remote-allow-origins", "no-sandbox", "disable-setuid-sandbox", "disable-seccomp-filter-sandbox", "disable-namespace-sandbox", "single-process", "no-zygote":
			return failure(ErrUnsupported, "reserved-flag")
		}
	}
	if value.MaxSessions < 1 || value.MaxSessions > 128 || value.QueuedCalls < 0 || value.QueuedCalls > 1024 || value.MaxCommands < 1 || value.MaxCommands > 65536 || value.MaxEvents < 1 || value.MaxEvents > 8192 {
		return failure(ErrInput, "counts")
	}
	for _, size := range []int64{value.MaxCommandBytes, value.MaxResultBytes, value.MaxEventBytes} {
		if size < 1 || size > 64<<20 {
			return failure(ErrInput, "bytes")
		}
	}
	for _, limit := range []time.Duration{value.AdmissionTimeout, value.StartupTimeout, value.SessionTimeout, value.CleanupTimeout} {
		if limit <= 0 || limit > time.Hour {
			return failure(ErrInput, "timeout")
		}
	}
	return nil
}
func (value settings) reservation() int64 {
	return 2*value.MaxCommandBytes + 2*value.MaxResultBytes + int64(value.MaxEvents)*512 + 65536 + int64(value.MaxPages)*(3*value.MaxEventBytes+2*value.MaxResultBytes+value.MaxCommandBytes+int64(value.MaxEvents)*384+65536) + int64(value.MaxStreams)*(128<<10) + int64(value.MaxSubscriptions)*(2*value.MaxEventBytes+int64(value.MaxEvents)*128+65536) + int64(value.MaxBindings)*(value.MaxEventBytes+int64(value.MaxEvents)*128+65536) + int64(value.MaxBindingCalls)*(2*value.MaxCommandBytes+8*value.MaxResultBytes+65536) + int64(value.MaxObjects)*1024 + int64(value.MaxObjectGroups)*(value.MaxResultBytes+4096)
}
func (value settings) evidenceBytes() int64 { return value.MaxResultBytes + 65536 }
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxSessions, Queued: value.QueuedCalls, Bytes: value.reservation() * int64(value.MaxSessions), QueuedBytes: value.reservation() * int64(value.QueuedCalls), MaxLeases: 1}
}

// LimitsV1 returns the policy for Go options. Layer overrides require matching
// limits supplied by composition; Bind checks the effective prepared settings.
func LimitsV1(options OptionsV1) (resource.Limits, error) {
	if options.Version != 0 && options.Version != 1 {
		return resource.Limits{}, failure(ErrInput, "version")
	}
	value := defaults(options)
	if err := validate(value); err != nil {
		return resource.Limits{}, err
	}
	return value.limits(), nil
}
