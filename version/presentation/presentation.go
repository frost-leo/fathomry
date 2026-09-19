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

package presentation

import (
	_ "embed"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
	"github.com/frost-leo/fathomry/version"
)

//go:embed locales/en.json
var english string

//go:embed locales/zh-Hans.json
var chinese string

// Resources returns fresh resource bytes for explicit preparation/composition.
// Message IDs belong to fathomry.version; collisions fail through i18n.Prepare.
// The English source and source-bound Simplified Chinese translations are embedded
// from external files. No resource parsing or catalog construction is implicit.
func Resources() []i18n.Resource {
	return []i18n.Resource{
		{Name: "fathomry/version/en.json", Data: []byte(english)},
		{Name: "fathomry/version/zh-Hans.json", Data: []byte(chinese)},
	}
}

// Summary renders a complete localized message in one catalog operation, keeping
// i18n's actual-language/fallback metadata intact. Values are exact scalar
// strings, never formatter objects. "?" marks an unavailable target/toolchain
// scalar, while development toolchains use the machine spelling "(devel)".
func Summary(catalog *i18n.Catalog, locale string, build version.Build) (i18n.Result, error) {
	snapshot := build.Snapshot()
	goos, goarch := "?", "?"
	for _, setting := range snapshot.Settings {
		switch setting.Name {
		case "GOOS":
			goos = setting.Value
		case "GOARCH":
			goarch = setting.Value
		}
	}
	toolchain := snapshot.Go.Value
	if toolchain == "" {
		toolchain = "?"
		if snapshot.Go.Evidence == version.DevelopmentEvidence {
			toolchain = "(devel)"
		}
	}
	arguments := i18n.Arguments{"Go": toolchain, "GOOS": goos, "GOARCH": goarch}
	id := "fathomry.version.summary.unstamped"
	claim := snapshot.Declaration
	if claim.Origin != version.NoDeclaration {
		id = "fathomry.version.summary.clean"
		if claim.Tree == version.Dirty {
			id = "fathomry.version.summary.dirty"
		}
		arguments["Release"] = claim.Release.String()
		arguments["Revision"] = claim.GitRevision
		arguments["BuildTime"] = claim.BuildTime.String()
	}
	return catalog.Render(locale, id, arguments)
}

// Error renders one of version's exact failure codes without inspecting an error
// graph or invoking arbitrary methods. Other codes return i18n.MessageNotFound.
// The original occurrence remains owned by the caller; rendering cannot rewrite
// its identity, cause, retry meaning or the successful machine record.
func Error(catalog *i18n.Catalog, locale string, code failure.Code) (i18n.Result, error) {
	switch code {
	case version.InvalidVersion, version.Unordered, version.InvalidTimestamp,
		version.InvalidRequest, version.InvalidMetadata, version.LimitExceeded,
		version.InvalidDeclaration, version.MissingDeclaration, version.Conflict,
		version.SerializationUnsupported:
		return catalog.Render(locale, string(code), nil)
	default:
		return i18n.Result{}, failure.New(i18n.MessageNotFound, nil)
	}
}
