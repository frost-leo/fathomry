<!--
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

# Generated application configuration

[Project creation](interface.md) / Application profile

**Audience:** authors of generated business projects.
**Status:** implemented template, validated against the exact published API
dependency. The structs belong to the generated project, not a new framework host.

## One aggregate, explicit sources

Config contains Application and will contain database/cache/provider sections
when used. Application stays in internal/setting/application.go and groups Build,
Deployment and Instance without splitting them into unrelated top-level settings.

| Field | Source and meaning |
| --- | --- |
| name | Stable generated project identity; must match ApplicationName |
| display_name | Editable human label; defaults to the project name |
| description | Optional human description; defaults to empty |
| namespace | Optional business grouping, not Kubernetes namespace or tenant authorization |
| build.version | Compiled application version; defaults to devel |
| build.revision | Compiled source revision; empty means unknown |
| build.built_at | Compiled UTC RFC3339 build time; empty means unknown |
| deployment.environment | Selected admitted bootstrap environment, never inferred |
| deployment.region / zone / cluster | Optional deployment location; empty means unspecified |
| instance.id | FATHOMRY_INSTANCE_ID when present, otherwise a fresh random startup identifier |
| timezone | UTC by default, or an IANA area/location identifier |
| i18n | Existing i18n.Preferences, default locale en |

A schema factory runs once after bootstrap admission, before application source
acquisition. It receives the selected environment and captures the declared
instance input. Help and invalid syntax do not call it. Pure schema validation
rejects attempts to override compiled Build, selected Environment or startup
Instance through application YAML or variable layers. These facts are not static
values copied into every replica's base.yaml.

The project returns configuration.Schema[Config] from the Framework package.
Its default template has no direct adapters/configsource import. Schema and
captured-variable translation into technical preparation are framework internals,
not boilerplate copied into the business application. Pure setting tests and
source-loading bootstrap tests retain separate responsibilities.

FATHOMRY_INSTANCE_ID is a separately declared process input consumed by the
project's bootstrap, not an additional dotenv bootstrap key. Explicit empty is
invalid; absence selects a random ID. The deployment owner must make supplied IDs
unique for the actual service-instance scope. Do not use this ID as a Workflow ID,
routing version, stable Worker capability or unbounded metric dimension.

## Validation and time semantics

Names and identifiers have bounded content; display names are at most 128 UTF-8
bytes, descriptions 1024, namespace/location/instance/build identifiers 128, and
environment 64 with the same identifier grammar as bootstrap. Human text rejects controls and surrounding whitespace.
A zone requires a region; location values do not infer network targets.
Build timestamps, when present, must parse as UTC RFC3339. No current time is
invented when a build timestamp is missing.

Timezone admission checks UTC or bounded slash-separated IANA-style identifier
syntax, rejecting Local, empty names and path traversal. It does not consult
host files or certify that a particular catalog contains the identifier. The
actual time-consuming capability must resolve the selected name against its
chosen IANA catalog before use and reject unknown names. The configuration-only
check is not that capability. Go's
[LoadLocation](https://pkg.go.dev/time#LoadLocation) may consult host files;
[time/tzdata](https://pkg.go.dev/time/tzdata) is an executable-owned fallback for
minimal images, not a framework-global setting.

Never change time.Local or reinterpret a frozen task's time zone because a new
configuration snapshot or display language was selected. Application defaults,
per-schedule time zones and serialized workflow inputs are separate contracts.
Configuration Watch publishes data; it does not rotate service identities,
clients, schedulers or topology automatically.

## Build metadata without editable provenance

From the generated project root, inject actual CI-owned values into the project
bootstrap package, replacing the example module and values:

```sh
GOWORK=off go build \
  -ldflags="-X example.com/acme/ordersync/internal/bootstrap.version=1.2.3 \
-X example.com/acme/ordersync/internal/bootstrap.revision=ACTUAL_SOURCE_REVISION \
-X example.com/acme/ordersync/internal/bootstrap.builtAt=ACTUAL_UTC_RFC3339_TIME" \
  -o "$BUILD_OUTPUT/fathomry" .
```

The placeholder build time is intentionally not valid input: supply the actual
timestamp, or omit the corresponding -X flag to retain unknown. Do not turn a
commit timestamp into a claimed build timestamp. Application artifact version,
Fathomry dependency version, configuration schema version and workflow source
revision remain independent.

The bounded config-check report exposes application name/locale and source
selection, not arbitrary descriptions, instance IDs, cluster inventories or full
Config/Bootstrap. Future telemetry composition should map selected fields
deliberately rather than dumping the aggregate into metric labels.

The vocabulary follows [OpenTelemetry service identity](https://opentelemetry.io/docs/specs/semconv/resource/service/)
and [deployment location](https://opentelemetry.io/docs/specs/semconv/resource/cloud/),
without importing a telemetry SDK or imposing cloud deployment on local projects.
[Build/release/run separation](https://12factor.net/build-release-run) informs the
read-only artifact facts. See [deployment examples](../../../../../../../development/project-deployment.md)
for runtime mounting and Secret boundaries.
