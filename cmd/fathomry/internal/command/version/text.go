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

package version

import (
	"strings"

	"github.com/frost-leo/fathomry/i18n"
	buildversion "github.com/frost-leo/fathomry/version"
	"github.com/frost-leo/fathomry/version/presentation"
)

func marker(catalog *i18n.Catalog, locale, name string) (string, error) {
	result, err := catalog.Render(locale, "fathomry.cli.version.marker."+name, nil)
	return result.Text, err
}

func factText(catalog *i18n.Catalog, locale string, fact buildversion.Fact) (string, error) {
	if (fact.Evidence == buildversion.Reported || fact.Evidence == buildversion.Requested) && fact.Value != "" {
		return fact.Value, nil
	}
	switch fact.Evidence {
	case buildversion.Redacted:
		return marker(catalog, locale, "redacted")
	case buildversion.DevelopmentEvidence:
		return marker(catalog, locale, "development")
	default:
		return marker(catalog, locale, "unknown")
	}
}

func versionText(catalog *i18n.Catalog, locale string, build buildversion.Build) (string, error) {
	summary, err := presentation.Summary(catalog, locale, build)
	if err != nil {
		return "", err
	}
	snapshot := build.Snapshot()
	applicationPath, err := factText(catalog, locale, snapshot.Main.Path)
	if err != nil {
		return "", err
	}
	applicationVersion, err := factText(catalog, locale, snapshot.Main.Version)
	if err != nil {
		return "", err
	}
	frameworkPath, err := factText(catalog, locale, snapshot.Framework.Path)
	if err != nil {
		return "", err
	}
	frameworkVersion, err := factText(catalog, locale, snapshot.Framework.Version)
	if err != nil {
		return "", err
	}
	replacement := snapshot.Framework.Replacement
	replacementKind, err := marker(catalog, locale, "none")
	if err != nil {
		return "", err
	}
	replacementPath, replacementVersion := replacementKind, replacementKind
	if replacement.Kind != buildversion.NoReplacement {
		replacementKind, err = marker(catalog, locale, string(replacement.Kind))
		if err != nil {
			return "", err
		}
		replacementPath, err = factText(catalog, locale, replacement.Path)
		if err != nil {
			return "", err
		}
		replacementVersion, err = factText(catalog, locale, replacement.Version)
		if err != nil {
			return "", err
		}
	}
	modules, err := catalog.Render(locale, "fathomry.cli.version.modules", i18n.Arguments{
		"ApplicationPath": applicationPath, "ApplicationVersion": applicationVersion,
		"FrameworkPath": frameworkPath, "FrameworkVersion": frameworkVersion,
		"ReplacementKind": replacementKind, "ReplacementPath": replacementPath,
		"ReplacementVersion": replacementVersion,
	})
	if err != nil {
		return "", err
	}
	vcs, err := factText(catalog, locale, snapshot.Source.VCS)
	if err != nil {
		return "", err
	}
	revision, err := factText(catalog, locale, snapshot.Source.Revision)
	if err != nil {
		return "", err
	}
	tree := string(snapshot.Source.Tree)
	if tree == "" {
		tree = "unknown"
	}
	tree, err = marker(catalog, locale, tree)
	if err != nil {
		return "", err
	}
	commitTime := snapshot.Source.CommitTime.String()
	if commitTime == "" {
		commitTime, err = marker(catalog, locale, "unknown")
		if err != nil {
			return "", err
		}
	}
	source, err := catalog.Render(locale, "fathomry.cli.version.source", i18n.Arguments{
		"VCS": vcs, "Revision": revision, "Tree": tree, "CommitTime": commitTime,
	})
	if err != nil {
		return "", err
	}
	claim := snapshot.Declaration
	claimRelease, err := marker(catalog, locale, "none")
	if err != nil {
		return "", err
	}
	claimRevision, claimTree, claimTime := claimRelease, claimRelease, claimRelease
	if claim.Origin != buildversion.NoDeclaration {
		claimRelease, claimRevision, claimTime = claim.Release.String(), claim.GitRevision, claim.BuildTime.String()
		claimTree, err = marker(catalog, locale, string(claim.Tree))
		if err != nil {
			return "", err
		}
	}
	declaration, err := catalog.Render(locale, "fathomry.cli.version.declaration", i18n.Arguments{
		"Release": claimRelease, "Revision": claimRevision, "Tree": claimTree, "BuildTime": claimTime,
	})
	if err != nil {
		return "", err
	}
	return strings.Join([]string{summary.Text, modules.Text, source.Text, declaration.Text}, "\n") + "\n", nil
}
