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

# Typed application configuration

[Documentation](../../../../README.md) / Public package reference

**Audience:** project authors with explicit local or Nacos configuration sources.
**Status:** implemented; local-file and isolated plaintext Nacos protocol-peer
checks, not real-service TLS/auth/failover or published-module qualification.
**Package:** `github.com/frost-leo/fathomry/framework/configuration/v1`.

## Responsibilities

Projects own their `setting` types, defaults and semantic validator.
Configuration owns explicit bootstrap/source admission, one ordered acceptance
path, publication and source cleanup. Public Viper/Nacos Adapters own native
acquisition/observation/recovery. Existing
[configsource preparation](../../../adapters/configsource/v1/interface.md)
and [settings snapshots](../../../settings/v1/interface.md) remain independent.

The implementation imports both supported providers; selecting local at runtime
does not promise a local-only dependency closure. It adds no App/Boot container,
global provider registry, process default, source writes, manual refresh,
source retargeting, business resource adoption or Workflow logic.

## Bootstrap is separate from application data

`LoadBootstrap` synchronously reads one to three explicit absolute file paths
using borrowed Viper dependencies. This remains necessary even when the resulting
application plan is remote. Caller-selected environment names contain 1..64 ASCII
letters/digits/dot/underscore/hyphen. No filenames, cwd, HOME or environment
variables are discovered.

Each `File` supplies a `Document`: unique kind base/environment/local, encoding
yaml/json/toml, and Optional (false means required). File paths are at most 4096
bytes, valid UTF-8, absolute and free of NUL; duplicate paths reject.
Source existence/readability is checked during acquisition, not promised by
declarative preflight.

Bootstrap version must be 1. Its environment defaults only to the caller-selected
label, and an explicit mismatch rejects. Supported mode/provider pairs are
local/viper and remote/nacos. Exactly the selected branch must be non-nil.
The ordered Documents correspond exactly to Local.Paths or Remote.Settings.Keys.

Local interval zero means 1 second (nonzero range 10 ms..5 min), local queue zero
means 16 (nonzero range 1..64). Nacos observation queue zero means 2 (range 1..16).
Other Nacos settings retain their public defaults/validation and nanosecond units.
Remote Writable and DynamicKeys must both be false.

```json
{
  "version": 1,
  "mode": "local",
  "provider": "viper",
  "watch": false,
  "documents": [{"kind": "base", "encoding": "yaml"}],
  "local": {"paths": ["/explicit/application.yaml"]}
}
```

Input Variables are explicitly captured `configsource.Variable` values. Their
Path uses the existing slash syntax, for example `/application/workers`;
non-string values need JSON=true. Neither dotenv parsing nor variable binding
authorizes ambient scanning. Defaults/layers obey existing strict preparation,
including unknown-original-field rejection and absent/empty/null distinctions.

Bootstrap loading is synchronous and cooperatively cancellable. Cancellation
cannot forcibly interrupt a blocked filesystem syscall. Returned bootstrap is
detached data; explicit JSON/value access may contain credentials. Ordinary
formatting/logging is restricted.

## Start, wait and read

Selected non-nil diagnostic Observers must be initialized; invalid handles reject
synchronously as ErrDeclaration with the original cause. Nil remains optional,
and a valid sealed Observer remains admissible because diagnostics are lossy.
Unselected application provider dependencies are not inspected.

Call `Start[T]` with a lifetime context, Bootstrap, project `Schema[T]`,
Options and selected application Dependencies. InitialTimeout and CandidateTimeout
must both be positive. A non-nil project validator is mandatory; an explicit
successful callback represents no extra semantic constraints.

Start structurally freezes the supported defaults without calling the semantic
validator on incomplete data. Plan/provider data, variable containers and defaults
are detached during admission; callers must not mutate them during that call.
Afterward mutations of originals do not affect future candidates. The callback's
closure is borrowed behavior: pure, bounded, non-panicking and cooperative.

Admission failures return no session and no application-source effects.
Successful admission returns an owner before asynchronous setup/acquisition.
`WaitInitial` waits for one sticky first result; cancelling that wait does not
cancel the producer. Reader reports settings.ErrUnconfigured until acceptance.

The initialization deadline includes queued setup and first validation. Its
failure decision and first publication share a fence; an expired old initialization
timer cannot cancel a successfully initialized watcher. CandidateTimeout limits
preparation/validation cooperatively. Late results cannot publish after the
session's close fence.

One-shot sessions capture once and retain owned source cleanup until Close or
parent cancellation. Watch sessions consume the provider's initial complete batch,
then subsequent complete observations, without redundant Capture or another
reconnection/polling engine. Another finite load requires a new explicit session.

## Publication and reporting

The Store is session-private. Only successful complete preparation and project
validation publish; rejected updates retain the previous accepted value.
No `settings.Configure` call is implicit. Old Views and Reader data remain valid
after later updates and source cleanup.

`Status` separates initializing/ready/closing/closed, accepted data and confirmed
completion. `Next` admits one waiter; overlap rejects with ErrLimit.
A successful publication report carries its exact View and publication sequence.
Other reports carry no new View. Update.Err describes a candidate/source/cleanup
failure; the outer Next error describes the wait. Cancellation does not consume
the pending report or cancel the producer.

There is one pending latest report. Replacements count Skipped and preserve source
Gap until consumption; this is not a journal of all changes. Sequences are local
uint64 counts, not source timestamps, hashes or versions. Exhaustion seals instead
of wrapping. After final reporting, Next preserves io.EOF.

Publication is not a same-time multi-document transaction, backend readiness,
credential validity or atomic resource adoption. Consumers explicitly coordinate
any resource bindings with accepted Views.

## Ownership and cleanup

Each session has one source/acceptance worker and one bounded lifetime/deadline
guard, plus the selected provider's existing owned work. There is no worker per
update. Close seals admission/publication, requests stop, joins owned work and
continues incomplete source cleanup. Observer cleanup precedes explicit owned-source
cleanup. Parent/provider autonomous cancellation retains existing contracts.

Close timeout does not abandon the session. Keep it and call Close again.
ShutdownComplete is independent of retained producer/cleanup errors: completion
may coexist with a non-nil error. A non-cooperative callback can delay completion;
it cannot authorize a late publication. The first and latest cleanup errors remain
inspectable, not an unbounded journal of retries.

Borrowed Runtime and Inboxes are never closed or silently drained. Required native
facts remain receivable until the caller acknowledges them. Finite bootstrap
evidence also remains charged until Ack. Use NextReleased to receive later finite
records behind a still-live Nacos Open; admission-order delivery can wait behind it.

Minimum configured envelopes, excluding other callers' existing reservations:

| Profile | Active roots | Work | Evidence |
| --- | --- | --- | --- |
| Local bootstrap/finite | 1 | 8 MiB | 1 x 64 KiB |
| Local Watch | 1 | 24 MiB | 1 x 64 KiB |
| Nacos Open + finite capture | 2 | 17 MiB | 2 x 64 KiB |
| Nacos Open + Watch | 2 | (17 + 4q) MiB | 2 x 64 KiB |

q is normalized Nacos observation capacity. q>=12 does not fit the default
64 MiB Runtime alongside Open. Preflight validates ceilings, not currently free
capacity; busy or subsequently closed borrowed resources can still reject work.
Declared budgets do not certify process RSS or arbitrary callback/cause-graph size.

## Errors, privacy and compatibility

The existing configuration owner/facility 0x045, local codes 1..11, identifiers,
Revision 1 and `fathomry.configuration.details/v1` are preserved.
SourceIndex is a selected slot or -1; operation names distinguish bootstrap and
application acquisition. The retired assembly codes are not reused.

Definitions, Resources and Component support explicit offline metadata composition.
English and zh-CN messages describe safe component facts. Original project/native
causes remain available through errors.Is/As. Opaque session/report/status/options
values refuse JSON reconstruction and safe diagnostics do not format those causes.
Deliberate raw-cause or accepted-value inspection is sensitive.

The restored package path does not restore the former application Boot API.
Schema, bootstrap, package, module and publication versions are different axes.

## Executable evidence

[Configuration loading tests](../../../../../framework/configuration/v1/load_test.go),
[session tests](../../../../../framework/configuration/v1/session_test.go),
[protocol peers](../../../../../framework/configuration/v1/provider_test.go),
[identity/privacy/fuzz tests](../../../../../framework/configuration/v1/error_test.go)
and [independent consumer](../../../../../framework/configuration/v1/integration_test.go)
exercise the public path and failure controls. The consumer defines its own setting
package, uses a separate module with local replacement, and tests both supported
pairs without importing Internal or changing generated projects.

These checks do not certify live Nacos deployments or published-module distribution.
See [#165](https://github.com/frost-leo/fathomry/issues/165) for the selected contract.
