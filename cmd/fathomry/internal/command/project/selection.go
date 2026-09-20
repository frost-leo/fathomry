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

package project

import "strings"

type environmentSource struct {
	Name     string
	Mode     string
	Provider string
}

type sourcePlan struct {
	Mode         string
	Provider     string
	Environments []environmentSource
	Local        bool
	Remote       bool
}

func selectSources(options Options) (sourcePlan, error) {
	mode, provider := options.Configuration, options.Provider
	if mode == "" {
		mode = "local"
	}
	if provider == "" {
		switch mode {
		case "local":
			provider = "viper"
		case "remote":
			provider = "nacos"
		}
	}
	valid := func(mode, provider string) bool {
		return mode == "local" && provider == "viper" || mode == "remote" && provider == "nacos"
	}
	if !valid(mode, provider) || len(options.EnvironmentSources) > 3 {
		return sourcePlan{}, problem(InvalidInput)
	}
	plan := sourcePlan{Mode: mode, Provider: provider}
	for _, name := range []string{"development", "test", "production"} {
		plan.Environments = append(plan.Environments, environmentSource{Name: name, Mode: mode, Provider: provider})
	}
	seen := make(map[string]bool)
	for _, declaration := range options.EnvironmentSources {
		if len(declaration) > 128 {
			return sourcePlan{}, problem(InvalidInput)
		}
		name, pair, ok := strings.Cut(declaration, "=")
		kind, implementation, split := strings.Cut(pair, "/")
		if !ok || !split || seen[name] || !valid(kind, implementation) {
			return sourcePlan{}, problem(InvalidInput)
		}
		found := false
		for index := range plan.Environments {
			if plan.Environments[index].Name == name {
				plan.Environments[index] = environmentSource{Name: name, Mode: kind, Provider: implementation}
				found = true
			}
		}
		if !found {
			return sourcePlan{}, problem(InvalidInput)
		}
		seen[name] = true
	}
	for _, environment := range plan.Environments {
		plan.Local = plan.Local || environment.Mode == "local"
		plan.Remote = plan.Remote || environment.Mode == "remote"
	}
	return plan, nil
}
