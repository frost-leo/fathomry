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

# Framework configuration scenarios

[Documentation](../../../../README.md) / Public package reference

**Audience:** project authors loading application settings or independent business data.
**Status:** implemented explicit inputs, finite Load, owned Watch, typed publication
and opt-in resource adoption. No application host or Worker runtime is implied.
**Package:** `github.com/frost-leo/fathomry/framework/configuration/v1`.

## Explicit declarations and dependencies

Projects import this package, not Adapter operation/client APIs. A Declaration
contains the project Schema and captured Variables. Dependencies.Provider is an
explicit, immutable source declaration. Zero Provider is invalid, never a default.
Constructing a Viper/Nacos descriptor validates and copies data without opening
files, connecting to services or creating a Watch. Each Load/Watch acquires its
own source and bounded operation/evidence owners.

```go
provider, err := configuration.Viper(configuration.ViperOptions{
    Documents: []configuration.File{
        {Path: absoluteBase, Kind: configuration.Base, Encoding: configuration.YAML},
        {Path: absoluteEnvironment, Kind: configuration.Environment, Encoding: configuration.YAML},
    },
})
if err != nil {
    return err
}
result, err := configuration.Load(ctx,
    configuration.Declaration[Settings]{
        Schema: configuration.Schema[Settings]{
            Version: 1, Defaults: defaults, Validate: validate,
        },
        Variables: capturedValues,
    },
    configuration.Dependencies{Provider: provider},
)
```

Each File or NacosDocument associates its location, layer kind, encoding and
optionality in one declaration. There are no parallel location/policy slices.
Viper requires distinct literal absolute paths, matching native Watch admission.
Descriptors do not access the filesystem to canonicalize aliases or symlinks.
NacosOptions contains NacosConnection, explicitly keyed Documents and an observation
capacity. Namespace, environment name and document key are different identities.
The scenario is read-only; the independent [Nacos Adapter](../../../adapters/configsource/nacos/v1/interface.md)
retains writes, dynamic keys, search and other supported public capabilities.

Viper and Nacos constructors share this package, so its build dependency closure
contains both public Adapters and their native dependencies. Only the explicitly
selected source is constructed. No service registry, name-based discovery, global
client or remote-to-local fallback exists. Framework uses public Adapters, not
direct Internal calls. `Values()` explicitly selects defaults/captured-values-only
Load; it cannot be watched.

These pre-release APIs intentionally replace Source/Runtime/Inbox dependencies
and Declaration.Layers. There are no compatibility aliases. Provider selection
does not require changing Load/Watch signatures or the project settings type.

## Bind startup inputs

ReadInputs receives InputOptions containing caller-supplied arguments, a lookup
function, named InputBindings and optional explicit dotenv selector names.
The project chooses authorized names/defaults; Framework implements the mechanism.

Precedence is arguments > supplied lookup > selected dotenv > declared defaults.
HasDefault and lookup presence preserve explicit empty values. Unknown/repeated
flags, positional arguments, duplicate bindings and invalid selected dotenv reject.
A dotenv assignment is allowed only when its binding permits it; application-value
overrides can deliberately be process-only. Shadowing cannot hide malformed or
unauthorized assignments in a selected file.

At most 32 bindings and argument tokens, 64 KiB per selected value and a 64 KiB
dotenv document are admitted. Raw argument bytes and selected text have separate
256 KiB aggregate limits. Flag names and punctuation count only toward the raw
argument budget, not the per-value limit; both flag/value spellings admit the same
value sizes. Four maximum-sized values can fit the selected-text budget but not
the raw argument budget once flag syntax is included. Dotenv uses the
[public literal profile](../../../adapters/configsource/v1/interface.md), not shell
execution/interpolation. Reads use the public Viper Adapter. Relative selected
dotenv paths resolve against the invocation directory; no ancestor scanning,
implicit example loading or process environment mutation occurs. Lookup is called
at most once per declared name and must be synchronous and bounded.

Inputs.Lookup deliberately exposes sensitive selected strings. Inputs.Records
returns detached released file-read facts, including after a later input failure.
No file selection means no source acquisition. Inputs does not install application
settings or choose a provider.

PrepareNacos accepts an explicit NacosBootstrapOptions file, selected environment,
application encoding and captured bootstrap Variables. The supported NacosBootstrap
schema owns connection, base key, up to 32 named environment keys and an optional
remote override. File extension selects YAML/YML, TOML or JSON; application encoding
is separate. Unknown environments, invalid declarations and unknown fields reject.
The helper uses ordinary Load over Viper, joins file ownership and returns an inert
Nacos Provider plus released records. It opens no remote client. Connection/document
choices freeze; application Watch does not watch or rotate the bootstrap connection.
There is no second parser or application host. Custom bootstrap schemas can still
use ordinary Load and explicit Nacos declarations. The generated [project Boot](../../../cmd/fathomry/interface.md#create-an-independent-project)
contains only these choices and calls.

## Strict data acceptance

At most three distinct Base, Environment and Override documents are selected.
Each is JSON, YAML or TOML. Optional permits positive absence only, not failed
reads, malformed content, unknown fields or incompatible values. Present-empty
TOML is an empty map; present-empty JSON/YAML rejects. Incomplete acquisition never
publishes a usable prefix. Multi-document reads are not a source-atomic transaction.

[Strict preparation](../../../adapters/configsource/v1/interface.md) remains the
single parser/overlay/type/null policy. Precedence is defaults < base < environment
< override < captured variables, regardless of the document slice order. Every
original layer is checked before overlay. The project validator receives a detached
candidate; mutating it cannot change the published data.

Up to 64 captured Variable values address declared struct-field paths. Present=false
contributes nothing; present empty overrides. JSON=false is literal string input;
JSON=true admits strict typed JSON values. Duplicate/overlapping paths reject.
Defaults and variables freeze at scenario admission. Validators must be bounded,
concurrency-safe, non-panicking and must not wait for their own scenario to close.
Load/Watch never reread process variables; change the declared values explicitly
when constructing another scenario.

## Finite results and watched replacement

Load returns Result[T], not a live client. State is present only after complete
acceptance/publication; released Records remain available even on initial source
failure. Accepted State is preserved when only cleanup fails. There are no live
scenario source handles after finite return.

Watch returns an asynchronous Watcher. Creation is not readiness: Reader.Capture
initially reports settings.ErrUnconfigured. Wait for an accepted Event or Capture
before using data. Watch does not update the Reader of a separate earlier Load.
A project that wants live data keeps the Watcher's reader from the start.

Reader exposes the original project root T. Capture returns an Accepted value
with a coherent ValueCopy, Description, View and Sequence. Separate Reader/Status
calls are not one transaction. Revisions are opaque identities, not remote cursors.
Equal accepted values may have different revisions.

One ingress coalesces pending source observations; one serial validator rejects
obsolete results before publication. There is no goroutine per update or second
native polling engine. Invalid/unavailable observations retain last-good data.
Valid later observations replace the entire accepted value, never individual fields.
Old captures remain readable and unchanged. Event capacity defaults to 16, accepts
1..64, and reports Gap when observations/notifications are coalesced. Events are not
a durable history.

## Ownership, adoption and evidence

Close fences publication before cancellation and joins observation, validation,
source cleanup and evidence release. A timeout retains the same owner; another
Close continues waiting. Parent cancellation also initiates owned cleanup. Last-good
data survives shutdown. Next cancellation ends only that wait. Records becomes
available only after final release and returns a detached slice.

Canceled startup admission preserves the original failure and caller cancellation
cause. It is not classified as cleanup failure unless cleanup itself fails; when
both fail, both causes remain inspectable.

Each scenario has an independent operation runtime: four active roots and 128 MiB
of declared work allowance, with bounded record custody. These limits are not RSS
measurements, a process-wide budget, or a remote quota. A separate framework.Runtime
does not automatically own a configuration Watch; sharing a parent context or a
borrowed resource scope is not ownership transfer.

Optional Dependencies.Resources explicitly borrows a resource Scope for accepted
view adoption. Configuration never closes that scope. Status.Adoption is the
asynchronous update receipt; AdoptionError/AdoptionFailed describe immediate Apply
refusal. Inspect/await resource adoption separately. Accepted data, resource
adoption and service readiness are different facts. Fixed/Follow policies, held
leases and failed-replacement retention remain owned by resource.

Released records are acknowledged into the returned in-process result. This is
custody transfer to the caller, not durable recording. Record.Info exposes technical
identifiers; Record.Source/Configuration expose bounded facts; Primary/Cleanup
retain original causes. Error handling or dropping notifications does not fabricate
successful delivery to an external sink. Components supplies the complete shared
and supported-provider definition/catalog set without constructing clients.

Handles guard ordinary formatting and refuse serialization. Explicit accepted
values, connection serialization, input lookup and original causes may be sensitive.
No process-local source or ownership handle belongs in Temporal Workflow history.

## Executable contracts

- [Input precedence, privacy and document bindings](../../../../../framework/configuration/v1/inputs_test.go).
- [Finite acceptance and malformed-input controls](../../../../../framework/configuration/v1/load_test.go).
- [Stale validation, last-good recovery and concurrent readers](../../../../../framework/configuration/v1/watch_test.go).
- [Partial acquisition, cleanup and parent cancellation](../../../../../framework/configuration/v1/lifecycle_test.go).
- [Nacos protocol fixtures, generated programs and explicit resource adoption](../../../../../framework/configuration/v1/integration_test.go).
- [Independent project data and localized errors](../../../../../framework/configuration/v1/testdata/consumer/consumer_test.go).
- [Opt-in live configuration/resource checks](../../../internal/conformance/fixtures.md#live-framework-configuration-path).

These contracts do not certify Nacos HA, authentication refresh, arbitrary blocked
filesystem calls, forced-process cleanup or a complete application runtime.
