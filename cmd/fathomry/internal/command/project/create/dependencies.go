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

package create

import (
	_ "embed"
	"path"
	"slices"
	"strings"
)

var dependencyModules = []string{
	frameworkModule, "github.com/fsnotify/fsnotify", "github.com/go-viper/mapstructure/v2",
	"github.com/inconshreveable/mousetrap", "github.com/nacos-group/nacos-sdk-go/v2",
	"github.com/pelletier/go-toml/v2", "github.com/sagikazarmark/locafero", "github.com/sourcegraph/conc",
	"github.com/spf13/afero", "github.com/spf13/cast", "github.com/spf13/cobra", "github.com/spf13/pflag",
	"github.com/spf13/viper", "github.com/subosito/gotenv", "go.uber.org/multierr", "go.uber.org/zap",
	"go.yaml.in/yaml/v3", "golang.org/x/net", "golang.org/x/sys", "golang.org/x/text",
	"google.golang.org/genproto/googleapis/rpc", "google.golang.org/grpc", "google.golang.org/protobuf",
	"gopkg.in/natefinch/lumberjack.v2",
}

//go:embed dependency_packages.txt
var dependencyPackages string

func shadowsDependency(modulePath string) bool {
	if slices.Contains(dependencyModules, modulePath) {
		return true
	}
	// The root main package and generated subpackages must all remain unambiguous.
	// Exact directory identities preserve harmless shared dependency prefixes.
	for _, file := range templateFiles {
		if strings.HasSuffix(file, ".go") &&
			strings.Contains(dependencyPackages, "\n"+path.Join(modulePath, path.Dir(file))+"\n") {
			return true
		}
	}
	return false
}
