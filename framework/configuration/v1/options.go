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
	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
)

// Encoding identifies a supported main-configuration document format.
type Encoding string

const (
	JSON Encoding = "json"
	YAML Encoding = "yaml"
	TOML Encoding = "toml"
)

// Document selects one main-configuration layer. Kind is base/environment/local;
// Encoding is yaml/json/toml. Optional=false requires positive source presence.
type Document struct {
	Kind     string   `json:"kind"`
	Encoding Encoding `json:"encoding"`
	Optional bool     `json:"optional"`
}

// Remote selects read-only, fixed-key Nacos configuration. Settings.Keys owns
// the ordered documents; an observation queue of zero selects the native default.
type Remote struct {
	Settings    nacos.Settings       `json:"settings"`
	Observation nacos.ObserveOptions `json:"observation"`
}

// SourcePlan describes where the application's main configuration comes from,
// not its Config value or running services. Version is the source-plan format,
// not an application version. Mode/provider is local/viper or remote/nacos, with
// exactly the corresponding non-nil branch. Watch applies to these documents,
// never to bootstrap selection. Explicit field/JSON access may reveal secrets.
type SourcePlan struct {
	Version     uint32               `json:"version"`
	Environment string               `json:"environment"`
	Mode        string               `json:"mode"`
	Provider    string               `json:"provider"`
	Watch       bool                 `json:"watch"`
	Documents   []Document           `json:"documents"`
	Local       *viper.WatchSettings `json:"local"`
	Remote      *Remote              `json:"remote"`
}

// Dependencies borrows only the selected provider's Runtime, evidence Inbox and
// optional observer. LoadConfiguration never closes or acknowledges these owners.
// Use NextReleased to receive finite evidence behind retained Nacos Open records,
// and close the Session before its borrowed dependencies. Unselected dependencies
// are not inspected. A non-nil observer must be initialized.
type Dependencies struct {
	Local  viper.Dependencies
	Remote nacos.Dependencies
}
