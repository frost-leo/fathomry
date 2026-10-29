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

package configuration

import (
	"time"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
)

// Document declares one original layer. Kind is base/environment/local; Encoding
// is yaml/json/toml. Optional=false requires positive presence, not nonempty data.
type Document struct {
	Kind     string                `json:"kind"`
	Encoding configsource.Encoding `json:"encoding"`
	Optional bool                  `json:"optional"`
}

// File selects an absolute local path and its layer. Path is sensitive input;
// neither filenames nor environment locations are discovered.
type File struct {
	Path     string
	Document Document
}

// BootstrapInput selects one environment and one to three unique local layers.
// Variables are already captured authorized values, never process lookups.
// Input storage is borrowed only for LoadBootstrap; do not mutate it concurrently.
type BootstrapInput struct {
	Environment string
	Files       []File
	Variables   []configsource.Variable
}

// Remote contains the selected read-only, fixed-key Nacos profile. Settings.Keys
// alone owns ordered targets; Observation's queue zero selects two.
type Remote struct {
	Settings    nacos.Settings       `json:"settings"`
	Observation nacos.ObserveOptions `json:"observation"`
}

// Bootstrap is sensitive loadable version-1 data. Mode/provider must be
// local/viper or remote/nacos, with exactly the matching non-nil branch.
// Documents match Local.Paths or Remote.Settings.Keys in order. Watch observes
// application documents, not this bootstrap. Explicit JSON may contain secrets.
type Bootstrap struct {
	Version     uint32               `json:"version"`
	Environment string               `json:"environment"`
	Mode        string               `json:"mode"`
	Provider    string               `json:"provider"`
	Watch       bool                 `json:"watch"`
	Documents   []Document           `json:"documents"`
	Local       *viper.WatchSettings `json:"local"`
	Remote      *Remote              `json:"remote"`
}

// Options supplies session budgets and captured application variables.
// Both durations must be positive; no implicit timeout or environment lookup.
// Start copies variables. Callbacks which ignore cancellation can delay cleanup.
type Options struct {
	InitialTimeout   time.Duration
	CandidateTimeout time.Duration
	Variables        []configsource.Variable
}

// Dependencies borrows only the selected application's public Runtime/Inbox.
// Bootstrap acquisition has separate Viper dependencies. Inboxes stay caller-owned;
// use NextReleased to receive finite evidence behind retained Nacos Open records.
// A non-nil selected Observer must be initialized. A sealed Observer remains valid
// optional diagnostics; unselected provider dependencies are not inspected.
type Dependencies struct {
	Local  viper.Dependencies
	Remote nacos.Dependencies
}
