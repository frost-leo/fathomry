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

# Bootstrap selection and configuration ownership

[Application configuration](interface.md) / Bootstrap profile

**Audience:** framework commands and application-entry authors.
**Status:** implemented source candidate; no formal application runtime.
**Package:** `github.com/frost-leo/fathomry/framework/configuration/v1`.

## Selection without application startup

LoadBootstrap reads one explicitly selected literal dotenv file using public
Viper acquisition. It returns Bootstrap data, not an application instance.
LoadConfiguration consumes that selection to obtain the first accepted main
configuration and, when selected, continue observing it.

The CLI resolves --bootstrap (default ./.env) once against caller cwd. Relative
configuration directories resolve against the selected dotenv parent. There is
no parent/HOME/executable/source-directory search. The selected dotenv file is
required, including when process overrides are supplied.

Precedence is explicit Environment override, captured Variables, dotenv, then
documented defaults. Every supplied layer undergoes syntax/type admission before
overlay, including shadowed values. Unknown keys reject. Empty/false are present,
not absence. Unrelated OS variables are ignored rather than reflected into Config.

The literal parser is the existing public ParseDotenv: at most 64 KiB and 64
assignments, duplicate-key rejection, literal single quotes and strict JSON double
quotes. No shell export, interpolation, global Setenv or Docker Compose equivalence.
Raw acquisition retains the provider's document bound before the tighter parser
bound; captured bootstrap names/values are separately bounded to 64 KiB.

## Keys and budgets

| Key | Meaning/default |
| --- | --- |
| FATHOMRY_BOOTSTRAP_VERSION | 1; other explicit versions reject |
| FATHOMRY_ENV | Required environment identifier; no development fallback |
| FATHOMRY_CONFIG_MODE | Required local or remote |
| FATHOMRY_CONFIG_PROVIDER | Required matching viper or nacos |
| FATHOMRY_CONFIG_WATCH | Strict true/false; default false |
| FATHOMRY_CONFIG_DIR | Local directory; default config relative to dotenv parent |
| FATHOMRY_CONFIG_DOCUMENTS_JSON | Remote ordered Document array |
| FATHOMRY_NACOS_SETTINGS_JSON | Remote strict public Nacos Settings |
| FATHOMRY_STARTUP_TIMEOUT | First configuration acceptance; default 30s |
| FATHOMRY_CONFIG_CANDIDATE_TIMEOUT | Candidate preparation/validation; default 5s |
| FATHOMRY_SHUTDOWN_TIMEOUT | Configuration cleanup upper bound; default 10s |

Duration strings belong only to this bootstrap profile. Initial/candidate bounds
are 1ms-5m; shutdown is 1ms-1m. Existing application/provider integer-nanosecond
contracts do not change.

LoadBootstrap uses its caller's acquisition context; a file cannot retroactively
set the budget for reading itself. The Resource composition's Policy provides
BootstrapTimeout and CleanupTimeout, each defaulting to 5s. Their bounds are
1ms-5m and 1ms-1m respectively. Accepted bootstrap replaces the inner shutdown
budget. The owning Scope and caller contexts may impose a smaller bound.

The main-configuration budget includes schema-factory and synchronous preparation
work. One original deadline reaches the candidate/publication fence; converting
elapsed phases into new full allowances is not permitted.

## Local and remote main configuration

Local mode selects two required YAML slots:
config/base.yaml and config/environment/<environment>.yaml under the resolved
directory. A valid empty mapping differs from an empty or missing document.
Only the selected environment is loaded. Explicit remote keys reject in local
mode, and an explicit local directory rejects in remote mode.

Remote mode requires 1-3 ordered Documents matching the fixed Nacos Settings.Keys
count/order. Native endpoints/authentication/TLS must be explicit and valid.
No local application-file mixing, dynamic keys, writes, inferred ports or fallback.
Observation queue zero retains the native default of 2. Sensitive Settings can
come from a declared process Secret value; they are not echoed in diagnostics.
Literal-dotenv JSON values use the actual parser quoting rules, for example
FATHOMRY_CONFIG_DOCUMENTS_JSON='[{"kind":"base","encoding":"yaml"}]'.

Application variable mappings are separately declared using Framework-owned
configuration.Variable values. They do not authorize arbitrary dotenv fields or
ambient environment reflection into Config.

## Resource composition and required evidence

PrepareInput performs only admission/copying. BindConfiguration declares a Fixed
binding; Apply acquires bootstrap and main configuration. The supplied project
SchemaFactory runs only after bootstrap admission, with the selected environment,
and cannot create business owners. Complete configuration validation happens after
source layers merge, not on incomplete defaults.

The binding creates the configuration-only Runtime/Inboxes. It returns a non-nil
Resource Instance even after failed construction so cleanup remains reachable.
Session closes before Runtime and remaining evidence are joined. A returned
Loaded value grants data access, not Close authority.

BindConfiguration returns the Resource Ref and a separate read-only settings
Reader whose snapshot root is Loaded. The Session publishes this private data
store alongside its first accepted main snapshot, before completing the initial
wait; custody does not depend on the constructor resuming to copy the result.
Capture returns settings.ErrUnconfigured before any acceptance; rejection does
not fabricate a result. Data custody therefore does not depend on a subsequent
cancelable lease acquisition. The Reader and captured Views own no resource and
remain usable through cleanup. Explicit Ref.Retry may replace the Reader's latest
accepted construction, without changing earlier captured Views.

Released provider records are handled with NextReleased, including records behind
retained Nacos Open work. Technical failures and unexpected mutation evidence are
not discarded. An optional Policy.Receive receives the released typed observation;
it is bounded, synchronous, non-panicking and context-cooperative. Failure requeues
the same observation and leaves cleanup incomplete. Continuation retries custody,
not the original source operation. Ack is handling, not durable journal storage.

The source lifetime is explicitly stopped in release. Resource generation
cancellation must not turn normal source closure into a fabricated failure.
Construction still follows the generation context and cannot be adopted after
the Scope stops. Failed or timed-out cleanup retains the same owner.

## Independent checks and later application startup

config check invokes this configuration composition, not an application runner.
It reports the first accepted snapshot only after required cleanup and output
succeed. Watch=true uses the actual observer's first batch, then closes; it is
not silently changed to false and does not keep a background watcher.

A future application-running command can reuse the configuration operations before
creating its required resources. Successful configuration is not their readiness.
Main-config Watch neither changes bootstrap nor automatically rotates clients.

See [loading](../../../../../framework/configuration/v1/load_configuration_test.go),
[selection](../../../../../framework/configuration/v1/load_bootstrap_test.go) and
[binding](../../../../../framework/configuration/v1/binding_test.go) tests.
Candidate-source, isolated protocol-peer and published-consumer evidence remain
separate; generated-project acceptance does not qualify production services.
