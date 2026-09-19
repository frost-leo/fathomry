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
	"encoding/json"

	buildversion "github.com/frost-leo/fathomry/version"
)

type wireFact struct {
	State string  `json:"state"`
	Value *string `json:"value"`
}

type wireReplacement struct {
	Kind    string   `json:"kind"`
	Path    wireFact `json:"path"`
	Version wireFact `json:"version"`
}

type wireModule struct {
	Present     bool             `json:"present"`
	Path        wireFact         `json:"path"`
	Version     wireFact         `json:"version"`
	Replacement *wireReplacement `json:"replacement"`
}

type wireSource struct {
	VCS        wireFact `json:"vcs"`
	Revision   wireFact `json:"revision"`
	Tree       string   `json:"tree"`
	CommitTime *string  `json:"commit_time"`
}

type wireBuild struct {
	Go     wireFact `json:"go"`
	Target struct {
		OS   wireFact `json:"os"`
		Arch wireFact `json:"arch"`
	} `json:"target"`
}

type wireDeclaration struct {
	Origin    string `json:"origin"`
	Release   string `json:"release"`
	Revision  string `json:"revision"`
	Tree      string `json:"tree"`
	BuildTime string `json:"build_time"`
}

type wireVersion struct {
	Schema   string `json:"schema"`
	Program  string `json:"program"`
	Metadata struct {
		Available bool   `json:"available"`
		Origin    string `json:"origin"`
	} `json:"metadata"`
	Application wireModule       `json:"application"`
	Framework   wireModule       `json:"framework"`
	Source      wireSource       `json:"source"`
	Build       wireBuild        `json:"build"`
	Declaration *wireDeclaration `json:"declaration"`
}

func versionJSON(build buildversion.Build) (string, error) {
	snapshot := build.Snapshot()
	report := wireVersion{Schema: "fathomry.cli.version/v1", Program: "fathomry"}
	report.Metadata.Available = snapshot.Metadata
	report.Metadata.Origin = string(snapshot.Origin)
	if report.Metadata.Origin == "" {
		report.Metadata.Origin = "unknown"
	}
	report.Application = projectModule(snapshot.Main)
	report.Framework = projectModule(snapshot.Framework)
	report.Source = wireSource{
		VCS: projectFact(snapshot.Source.VCS), Revision: projectFact(snapshot.Source.Revision),
		Tree: treeState(snapshot.Source.Tree), CommitTime: optional(snapshot.Source.CommitTime.String()),
	}
	report.Build.Go = projectFact(snapshot.Go)
	report.Build.Target.OS = wireFact{State: "unknown"}
	report.Build.Target.Arch = wireFact{State: "unknown"}
	for _, setting := range snapshot.Settings {
		switch setting.Name {
		case "GOOS":
			report.Build.Target.OS = reportedFact(setting.Value)
		case "GOARCH":
			report.Build.Target.Arch = reportedFact(setting.Value)
		}
	}
	if claim := snapshot.Declaration; claim.Origin != buildversion.NoDeclaration {
		report.Declaration = &wireDeclaration{
			Origin: string(claim.Origin), Release: claim.Release.String(),
			Revision: claim.GitRevision, Tree: string(claim.Tree), BuildTime: claim.BuildTime.String(),
		}
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func treeState(state buildversion.TreeState) string {
	if state == buildversion.UnknownTree {
		return "unknown"
	}
	return string(state)
}

func reportedFact(value string) wireFact {
	if value == "" {
		return wireFact{State: "unknown"}
	}
	return wireFact{State: "reported", Value: optional(value)}
}

func projectFact(fact buildversion.Fact) wireFact {
	switch fact.Evidence {
	case buildversion.Reported:
		return reportedFact(fact.Value)
	case buildversion.Requested:
		if fact.Value != "" {
			return wireFact{State: "requested", Value: optional(fact.Value)}
		}
	case buildversion.Redacted:
		return wireFact{State: "redacted"}
	case buildversion.DevelopmentEvidence:
		return wireFact{State: "development"}
	}
	return wireFact{State: "unknown"}
}

func projectModule(module buildversion.Module) wireModule {
	result := wireModule{Present: module.Present, Path: projectFact(module.Path), Version: projectFact(module.Version)}
	if module.Replacement.Kind != buildversion.NoReplacement {
		result.Replacement = &wireReplacement{
			Kind: string(module.Replacement.Kind), Path: projectFact(module.Replacement.Path),
			Version: projectFact(module.Replacement.Version),
		}
	}
	return result
}
