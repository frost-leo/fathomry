// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

module github.com/frost-leo/fathomry

go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/jackc/puddle/v2 v2.2.2
	// TODO(gh-21): Recheck this pin and lifecycle compatibility before Nacos upgrades
	// or framework releases. An official stable SDK must fix BOTH registry shutdown
	// races and saturated-listener shutdown. Retire only workarounds made unnecessary
	// by passing the native counterexamples, race/isolation/privacy and real-service
	// read/listen/reconnect/close gates; preserve Fathomry's contracts. Track evidence:
	// https://github.com/frost-leo/fathomry/issues/21
	github.com/nacos-group/nacos-sdk-go/v2 v2.3.5
	github.com/spf13/viper v1.21.0
	go.yaml.in/yaml/v3 v3.0.5
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/sagikazarmark/locafero v0.11.0 // indirect
	github.com/sourcegraph/conc v0.3.1-0.20240121214520-5f936abd7ae8 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
)
