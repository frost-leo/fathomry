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

# Project creation command interface

[Documentation](../../../../../../README.md) / Executable-private command

**Audience:** CLI maintainers and SDK-bundle producers.
**Status:** implemented new-project generation and explicit snapshot installation.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project`.

## Ownership and dependency direction

This package owns `fathomry new`, its templates, presentation and filesystem effects.
It is not a public framework package. The root registers `New`; the common CLI
mechanics do not import it. Generated programs consume public configuration/i18n
APIs, not the command package, its runtime declarations or a `framework/project` API.

`Create` is a private executable composition boundary. Its `Options` select an
absolute clean destination, portable name, independent module, explicit canonical
framework version, source-kind/Provider plan, initial locale and optional local SDK bundle. It reads no
ambient configuration and launches no shell, downloader, Git command or service.
The CLI resolves caller-supplied relative directories. An ordinary released main
module version may supply the default; development metadata cannot.

## Generation and partial effects

All options, templates and selected SDK content are validated before exclusive
target creation. An existing target is always refused. The destination parent
must already exist and may not be a directory inside the source bundle, including
an alias to one. Rooted file operations and exclusive file creation prevent
escaping child paths and overwriting existing files.

`Result.Created` transfers responsibility for the created directory, including
on later error. `Complete` means source generation and selected snapshot checks
finished, not dependency resolution, compilation or application readiness.
`FilesWritten` counts successfully written/closed files; a failed file may have
additional partial effects. A failure after acquisition retains the directory
and reports `Incomplete`, preserving a safe primary/cancellation cause. No
recursive rollback or portable atomic-publication promise is made.

Filesystem operations are synchronous and cooperative. Sources and selected
directory namespaces must remain stable; this is not a sandbox against another
actor controlling the same filesystem. Context cannot forcibly interrupt blocked
OS operations. Ordinary formatting/JSON reconstruction of runtime declarations
is restricted; deliberate result field access can expose the selected path.

## SDK bundle contract

The artifact schema is `fathomry.sdk-bundle/v1`. A bundle directory contains
`manifest.json` and one directory per corrected SDK:

~~~text
bundle/
  manifest.json
  temporal-sdk/
    go.mod
    UPSTREAM.json
    FATHOMRY.md
    LICENSE
    ...
~~~

The manifest has these fields:

| Field | Meaning |
| --- | --- |
| `format` | Exact artifact format identifier |
| `framework_version` | Exact match for the chosen framework dependency version |
| `modules` | Nonempty selected SDK inventory |
| `license` | Optional producer notice lines; not a new upstream grant |
| Module `path` / `version` | Original canonical Go module identity and upstream version |
| Module `revision` | Non-secret lowercase patch label, not module or project-schema version |
| Module `directory` | Unique portable lowercase directory label |
| Module `files` | Complete relative regular-file inventory, each mapped to `sha256:` plus lowercase digest |

`go.mod`, `UPSTREAM.json` and `FATHOMRY.md` must be inventoried for each module.
Preserve actual licenses and provenance. The parsed module path must match;
nested modules and nested `replace`/`exclude` directives are refused rather than
silently assumed to affect the consuming build. SDK namespaces may not overlap
the framework or consuming business module. Duplicate JSON members, unknown
fields, trailing values, path escape, symlinks, case-fold aliases, file/directory
conflicts and extra uninventoried files are refused.

The manifest is at most 1 MiB; at most 32 modules and 8,192 SDK files are accepted.
Individual SDK files are at most 16 MiB, aggregate SDK bytes at most 128 MiB.
File paths are bounded to 512 bytes and traversal work is separately bounded.
These are artifact/work bounds, not a process RSS or filesystem-time guarantee.

Installed files live under `third_party/fathomry/<directory>`. The root `go.mod`
receives exact requirements and unversioned-left relative replacements for the
selected snapshots. Creation verifies the installed content and declarations
against the preflight manifest. A producer declares provenance/compatibility;
hashes bind bytes but do not authenticate that producer, prove actual MVS/binary
selection, detect secrets or establish redistribution rights.

No bundle is implicitly included in the CLI. Normal Go module archives omit
nested SDK modules, and direct embedding cannot cross their module boundary.
This implementation consumes an explicit directory artifact instead. Ordinary
`go mod verify` checks downloaded module-cache material, not these local snapshots.
Subsequent upgrades, edits and retired patches require separate qualification;
this command does not manage already-existing projects.

## Generated configuration

See [Create a project](../../../../../../development/create-project.md) for layout,
environment/source selection and use. The generated schema and declarations are
business-owned source. `configuration.LoadVariables` prepares explicit bootstrap
defaults and variables through the existing preparation contract; it is not a
second configuration engine or automatic environment discovery.

Generation selects a default local/viper or remote/nacos pair with up to three
unique overrides of development/test/production. Mismatched pairs, duplicate or
unknown declarations fail before writing. Only selected concrete implementations
are imported. A single mixed-provider binary selects its declared
environment's Provider without recompilation. Unsupported environments refuse; local
overrides are opt-in and environment-scoped. Nacos bootstrap is earlier input,
not values obtained from its own remote document. Generation never publishes it.

The project's `internal/resource` owns visible data declarations, defaults and
pure validation without importing loaders/adapters. The separate configuration
module owns source selection and bindings, and loads `resource.Config`. Adding
business fields does not require changing acquisition code. The generated README
and examples expose the supported field/bootstrap contracts. Public `i18n.Settings`
is composed, not duplicated or replaced with demonstration-only fields.
Project-owned localization embeds resources; the thin entry
uses effective settings and a per-invocation locale override. Bootstrap help and
errors do not require application configuration. Generated default locale is en
or zh-Hans, separately selected from the creation command's presentation locale.

Dotenv examples contain no actual credentials and are never automatically loaded.
The generated `--env-file` selects one required literal file. It feeds only declared
environment-prefixed variables, with process values taking precedence even when
empty. No process mutation, interpolation, source discovery or shell execution
is introduced. Configurations retain safe variable origins, not names or values.

## Errors and presentation

Capability-owned codes distinguish invalid input, existing destination,
unavailable I/O, invalid bundle, incomplete generation and cancellation. Native
path/parser error text is withheld. Caller cancellation causes retain intentional
inspection. Human help/result/error resources are English-primary with Chinese
translations. Root help and creation help perform no filesystem acquisition.

After output failure or cancellation, an already-created project may remain.
Do not infer absence from a nonzero CLI status or automatically retry over it.

## Executable evidence

[Creation and boundary tests](../../../../../../../cmd/fathomry/internal/command/project/creation_test.go),
[bundle controls](../../../../../../../cmd/fathomry/internal/command/project/bundle_test.go),
[independent generated consumers](../../../../../../../cmd/fathomry/internal/command/project/consumer_test.go),
[actual CLI cases](../../../../../../../cmd/fathomry/internal/root/project_test.go) and
[Nacos generated-project TLS/authentication](../../../../../../../adapters/configuration/remote/nacos/generated_project_test.go)
exercise the supported paths. The Temporal consumer proves selection of a real
corrected SDK interface, with a rejecting patch-removal control; it is not fresh
Temporal service/replay qualification. No Workflow command or serialization changes
are introduced. Implemented for [Issue #83](https://github.com/frost-leo/fathomry/issues/83).
