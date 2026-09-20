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

# Create an independent project

[Documentation](../README.md) / Development guide

**Audience:** developers creating a Fathomry business project.
**Status:** implemented configuration-only generation, not a Workflow/Worker runtime.
**Command:** `fathomry new`.

## Choose a version and configuration plan

Build the CLI from the checkout using [Build the CLI](build-cli.md). Local SDK
replacements still prevent treating that build as proof that arbitrary
`go install ...@version` works. Select an immutable obtainable framework module
version containing these public APIs; a development CLI cannot infer it from
`(devel)`. Creation does not download or qualify the selected version.

Run from the framework checkout with an existing writable destination parent:

~~~sh
go build -o /tmp/fathomry ./cmd/fathomry
/tmp/fathomry new example \
  --directory /path/to/projects \
  --module example.org/example \
  --framework-version "$FATHOMRY_VERSION" \
  --configuration local --provider viper \
  --environment-source production=remote/nacos \
  --default-locale en
~~~

This declares local/Viper for development and test, remote/Nacos for production.
Omit the override for a local-only project. Select `--configuration remote
--provider nacos` for all-remote environments. Provider defaults to the implemented
provider for the chosen kind; mismatched pairs fail before writing. Repeat
`--environment-source <name>=<kind>/<provider>` to override other declared
environments. Duplicate or unknown declarations fail.

`--lang` controls the creation tool's presentation, not the project's defaults.
`--default-locale en|zh-Hans` selects the generated project's initial language.

The result is a new `/path/to/projects/example`. Names are lowercase portable
labels, <=64 bytes and start with a letter. The module path is explicit and
<=256 bytes. Framework versions must be canonical v0/v1, not `latest`.

Existing targets, including empty directories and symlinks, are refused.
Generation performs no Git initialization, service access or dependency download.
Cancellation/write failure after creation leaves the partial directory owned by
the caller; no recursive rollback erases unrelated files. Output failure can
happen after successful creation, so inspect before retrying.

## Build and check

From the generated project root:

~~~sh
go mod tidy
go test -race ./...
go build -o bin/example ./cmd/example
./bin/example --environment development
~~~

Go resolution may use the network and generates `go.sum`; creation does not
fabricate it. A remote environment additionally needs its authorized service,
documents and bootstrap inputs. Successful output is diagnostic JSON containing
safe source metadata, environment, actual resource locale and a localized result.
It does not expose general configuration values or prove service readiness.

The single business module contains:

~~~text
cmd/example/main.go
internal/configuration/
  configuration.go
  bindings.go
  sources.go
  sources_local.go          # only when local/Viper is selected
  sources_nacos.go          # only when remote/Nacos is selected
  configuration_test.go
internal/resource/
  configuration.go          # visible data types, defaults and pure validation
  configuration_test.go
internal/localization/
  localization.go
  locales/{en,zh-Hans}.json
configs/
  base.yaml
  environments/{development,test,production}.yaml
  local/development.example.yaml
.env.example
.env.{development,test,production}.example
go.mod
README.md
.gitignore
LICENSE
~~~

`internal/resource` is the visible project configuration declaration module:
types, defaults and pure validation, initially composing public `i18n.Settings`.
It is not Fathomry's private runtime resource package and imports no loader or
adapter. `internal/configuration` selects sources and loads those declarations.
The generated README lists all current application fields and bootstrap bindings,
their types, defaults and input rules; developers need not infer them from loader
implementation. The framework
owns acquisition, preparation and cleanup. `internal/localization` owns messages.
The entry owns arguments, interruption and output. No Fathomry internals or
creation tooling are imported. No unimplemented service placeholders are emitted.

## Select explicit dotenv input and overrides

~~~sh
cp .env.development.example .env.development
chmod 600 .env.development
./bin/example --environment development --env-file .env.development
~~~

No dotenv file is automatically discovered, including `.env`.
`.env.example` is the development example under a conventional filename.
Private copies are ignored by Git; committed examples contain no credentials.
Production must explicitly select its own input, which can be an absolute
secret-mount path. Relative paths resolve from the selected project root.

A selected missing/invalid file fails. The
[literal dotenv contract](../reference/adapters/configuration/local/dotenv/interface.md)
has byte/entry bounds and no interpolation, shell evaluation, ambient expansion
or process mutation. Unused assignments must still be syntactically valid.

Precedence is:

~~~text
typed defaults < base < selected environment < authorized Local layer
               < declared variables: dotenv fallback < present process value
~~~

An empty process value wins, even when schema validation then rejects it.
Bindings are explicit and environment-prefixed, initially
`EXAMPLE_DEVELOPMENT_I18N_DEFAULT_LOCALE` for development. A production selection
does not consume development bindings. Unknown environment selection fails before
dotenv or remote acquisition. There is no global mutable settings object.

Local/Viper reads base and one required environment file. `--local` opts into
`configs/local/<environment>.yaml`; only absence is optional. The Local
precedence layer is not the same thing as the local acquisition category.

## Supply remote bootstrap before application configuration

Nacos environments use `sources_nacos.go` declarations and their dotenv examples.
HTTP/gRPC endpoints, namespace, credentials and trust are earlier inputs; they
cannot come from the remote configuration they locate. Environment names do not
infer Nacos namespaces. The default group is DEFAULT_GROUP; initial IDs are
`example.base.yaml`, `example.<environment>.yaml`, and the explicitly selected
optional `example.<environment>.local.yaml`.

The `configs/` files are remote content examples for these environments, never
a fallback. Publishing is a separate authorized deployment action, not generation.
Missing, denied, invalid or unavailable Nacos input never falls back to local or
stale data. HTTP TLS and gRPC security are separate; supply trust and keep
ALLOW_INSECURE=false except for deliberately authorized isolated testing.
Do not put credentials in CLI arguments or committed examples.

A single mixed-provider binary can select development without contacting Nacos,
and production without reading local YAML. Different source kinds do not change
the application's schema or merge rules.

## Extend actual public capabilities

The project's `resource` declaration composes public pure settings and validation;
it does not duplicate native options, connections or resource management.
Business fields remain project-owned. Add fields/defaults/pure rules in
`internal/resource/configuration.go`, then supply their environment values; the
loading flow does not change. Declare optional variable overrides explicitly in
`internal/configuration/bindings.go`. Add a future PostgreSQL or other capability
only after its public contract exists; an internal SDK integration is not that
contract. The [configuration architecture](../architecture/configuration.md)
keeps bootstrap, application settings and runtime resource ownership distinct.

After loading, effective `i18n.default_locale` selects output language.
`--lang` overrides one invocation without changing the prepared configuration.
Help and startup errors use the embedded default, so reporting a Nacos failure
does not require Nacos to work. English source/fallback remains mandatory; resource
composition is explicit code, not dynamic language-provider loading.

## Select patched dependencies deliberately

Configuration-only projects need none of the repository's 13 SDK replacements.
`--sdk-bundle /absolute/bundle` selects a version-bound local artifact, not implicit
redistribution of all forks. See the
[bundle contract](../reference/cmd/fathomry/internal/command/project/interface.md#sdk-bundle-contract).
Creation validates the inventory, copies under `third_party/fathomry`, writes
project-relative root replacements and verifies the installed result.

Preserve notices/provenance; content hashes are not producer authentication,
license clearance or compatibility certification. `go mod verify` does not
check local replacement contents. Subsequent upgrades and edits need separate
qualification. This is not a project regeneration or dependency-upgrade engine.

## Verification limits

Tests exercise exclusive creation, partial effects, same-binary environment
selection, public feature configuration, literal dotenv/empty-process/CLI
precedence, independent module artifacts, isolated Nacos TLS/authentication and a
real Temporal patch-sensitive consumer. Offline consumers use module-mode tests
and readonly builds, not complete resolution of all transitive dependency tests.

Windows/macOS runtime, external Nacos deployments, production-secret handling,
crash durability and arbitrary bundle compatibility are not established.
