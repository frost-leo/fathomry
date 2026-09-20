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

# Load configuration in an independent project

[Documentation](../README.md) / Development

**Audience:** developers using the current local or Nacos configuration capability.
**Status:** real public loading example; no generated project, Worker or service initialization.

The [project fixture](../../framework/configuration/testdata/project) is a small
business program using public declarations. Its schema owns business
fields/defaults and validation. Its source declaration selects a framework-provided
local adapter; the program does not import SDKs or private Fathomry mechanisms.

The program explicitly selects settings.yaml, optional environment.yaml and
optional settings.local.yaml beneath a supplied absolute, clean directory.
These are this example's choices, not framework-wide filename discovery.
It binds only FATHOMRY_EXAMPLE_LABEL. No configuration file chooses a new
Provider, expands access rights or starts business resources.

With the repository's Go 1.27 toolchain selected, this local example can be run
against a caller-owned directory containing a settings.yaml mapping:

```sh
go run ./framework/configuration/testdata/project /absolute/path/to/settings-directory
```

A minimal document is `{}`; the example then uses its declared defaults.
The output confirms the selected Provider and schema version without printing
configuration values. The only I/O performed by the loading path is its explicitly
declared local input access. No external service is initialized.

The [independent module acceptance test](../../framework/configuration/consumer_test.go)
also packages the framework into a temporary file-proxy artifact, builds this
project with an empty separate module cache and no source replacements, runs its
consumer compatibility tests and executes the resulting program. Cached dependency
archives are supplied through file-only proxies; registry network is disabled.
This establishes consumer feasibility, not public release installation.

## Select remote configuration instead

The [Nacos project fixture](../../adapters/configuration/remote/nacos/testdata/project)
uses the same framework loader and a different public Provider. It declares
one required base document; no native client, watch or Close method enters the
business program. The [adapter contract](../reference/adapters/configuration/remote/nacos/interface.md)
defines bootstrap bounds, optionality, TLS/authentication and cleanup semantics.

From the repository root with Go 1.27 selected, run only against explicitly
authorized endpoints and keys. This command performs remote reads and, when
selected, authentication; it does not publish or delete remote configuration:

```sh
go run ./adapters/configuration/remote/nacos/testdata/project \
  "$HTTP_URL" "$GRPC_ADDRESS" "$NACOS_NAMESPACE" "$NACOS_DATA_ID"
```

The example reads only the explicitly named bootstrap environment variables
FATHOMRY_EXAMPLE_NACOS_USERNAME, FATHOMRY_EXAMPLE_NACOS_PASSWORD and
FATHOMRY_EXAMPLE_NACOS_ROOT_CA_PEM. Empty credentials select no login; empty trust
uses system roots. TLS is required unless FATHOMRY_EXAMPLE_NACOS_INSECURE is exactly
true, an explicit plaintext test profile. Do not put credentials in command arguments
or committed example files. Bootstrap is available before the remote settings;
the loaded document cannot choose a new endpoint or expand its own access rights.

The independent module test also compiles and tests these public declarations
without a source replacement. A separate standalone executable test performs
successful authenticated/TLS reads against native loopback protocol servers.
This is not fresh external-Nacos deployment or production qualification.

## Changing structures without silently changing existing projects

The fixture keeps its consumer declarations and behavioral expectations.
A compatible framework update must preserve field/default/zero-value, overlay
and error semantics. Adding an optional field with a compatible default can
extend an options struct; adding a required field, changing a unit or changing
interpretation is not made compatible merely by retaining its name. Changes to
public interface method sets or function signatures also require compatibility
review. The framework module is the release/Go API version boundary, not each
runtime struct.

No unused second API family or migration registry is supplied. The project
separately owns business schema evolution: changing seconds to milliseconds is a data-schema
change even if both Go fields are int64. The
[schema evolution test](../../framework/configuration/configuration_test.go)
shows mismatch rejection, explicit business conversion and an unchanged old
prepared value. There is no generic automatic migration or code fingerprint.

## Framework and project boundaries

Future project generation may reproduce the project's thin entry, public
declarations, safe setting examples and checks. It must not copy private loading,
resource ownership or SDK setup into generated source. A settings declaration
is not an application initialization hook. Neither example implements a
management-command host, source registry or Workflow execution.

See the [public contract](../reference/framework/configuration/interface.md)
for explicit environment conversion, diagnostics, ownership and unsupported
cases, and the [local adapter](../reference/adapters/configuration/local/viper/interface.md)
for path, absence and filesystem limitations.
