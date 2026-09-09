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

# Named source preparation and resource ownership

Status: implemented foundation for [Issue #4](https://github.com/frost-leo/fathomry/issues/4),
not a concrete SDK integration. The implementation follows S01, S03–S06, S08–S09
of the [accepted standard](../architecture/internal-sdk-integration.md) at
`0c3f9d82947229a861a30fbf9d1730fa02ca44ea`. No Temporal, database, broker, transport,
object-store, or service compatibility is claimed.

## Package and dependency boundaries

`source` owns explicit configuration preparation and process-local named resource
assembly. `failure` is the public error foundation, shared by composition,
internal implementations, adapters, and independently compiled business projects.
Neither imports a concrete Provider, business workflow, framework orchestrator,
or exporter. Concrete integrations remain independently selected by Go imports.

An independent project can use these packages without importing Fathomry's
private implementation packages. See the executable
[composition example](../../source/example_test.go) and
[external-module test](../../source/boundary_test.go). The example is a local
function fixture, not a service client.

## Prepare before constructing

1. Define a Provider-owned `Schema[T]`: configuration format, typed defaults, and
   pure semantic validation. Resolve and authorize secrets in outer composition.
2. Call `Prepare(schema, input)` for the explicitly selected source. Handle its
   error. This operation acquires no resource and reads no files or environment.
3. Call `Select(prepared, factory)` to obtain a typed selection token.
4. Call `Assemble(initCtx, cleanupCtx, scope, selections...)`. Give initialization
   and cleanup independently appropriate caller-owned budgets.
5. Use `Bind(assembly, selection)` only at the trusted outer composition boundary.
   Pass the resulting non-owning facade, not the assembly or resource, to consumers.

The assembly validates every selection and duplicate local name before any factory
runs. A zero/unprepared selection, missing constructor, unavailable source, or
known exclusive-transfer conflict is rejected at preflight. A sharing scope can
become unavailable after preflight; acquisition then fails through the normal
partial-initialization cleanup path.

Provider identity identifies an implementation, not its SDK version. Source names
are unique across all Providers within one assembly. Provider/name/scope labels
use 1–64 lowercase ASCII letters, digits, dots, underscores, or hyphens. Composition
must exclude secrets from these labels; syntax cannot authenticate or redact them.
They are not automatically appropriate metric labels.

Composition owns scope-label uniqueness wherever evidence from independent
assemblies is combined. Scope labels are not globally registered or automatically
qualified with a process/worker identity. Distinct isolated applications can reuse
labels; combining identically labeled assemblies would make their diagnostic
attribution ambiguous even though their resource records remain independent.

### Explicit configuration format

`Input.Format` must equal the nonzero `Schema.Format`; there is no fallback to a
different version's defaults. This is the selected Provider's configuration schema
version, not the YAML specification version, module version, or source revision.

The supported DTO is a nonrecursive Go struct with explicit `json:"field"` tags.
Fields may contain strings, booleans, finite numeric values, pointers, nested plain
structs, string-keyed maps, and slices. Tag options, anonymous/private fields,
arrays, interfaces, byte slices, custom serialization, and runtime handles are
rejected. Default strings and map keys must be valid UTF-8; invalid bytes are
rejected instead of being silently normalized by JSON encoding. Named scalar types such as `time.Duration` can use their numeric
representation, with units defined by the Provider. SDK options, callbacks, dynamic
credential-refresh handles, and `time.Time` are not generic copyable settings:
derive or inject those at their actual ownership boundary.

Each explicit layer supplies one YAML mapping; JSON syntax is accepted as YAML.
This is a restricted YAML input contract, not a promise to accept all YAML features:

| Aspect | Contract |
| --- | --- |
| Precedence | Typed defaults < base < environment-specific < local < variables; independent of slice order |
| Layer identity | At most one document per non-default layer kind; composition combines multiple inputs of the same kind before this boundary |
| Structural validation | Every layer must have exact, known field names and compatible value types, even if a higher layer would override it |
| Semantic validation | Provider validation runs on the final effective settings, on a separate copy whose mutations are discarded |
| Objects | Recursive merge retains siblings; an empty object preserves inherited children |
| Maps and lists | String-keyed maps merge recursively without case folding; slices replace as a whole, never concatenate |
| Absence | Inherits existing settings; newly introduced objects use Go zero values for otherwise absent fields |
| Empty values | Empty string, false, numeric zero, and empty slice explicitly override |
| Null | Clears pointers, maps, and slices; rejected for non-nullable fields; not a deletion operator |
| Numbers | JSON numeric syntax, with target-type range checks; integer fields reject fractions and exponent notation without a float64 intermediate |
| Rejected syntax | Duplicate keys, non-string keys, anchors, aliases, merge keys, explicit/custom tags, implicit timestamps, multiple documents |
| Bounds | Each input document and resolved JSON at most 1 MiB; normalized input nesting at most 64 levels |
| Reload | No implicit reload, environment reread, file watcher, credential rotation, or live settings mutation |

The YAML parser is `go.yaml.in/yaml/v3 v3.0.5`, chosen as a stable release rather
than selecting a reference snapshot automatically. V3 is security-maintained;
upstream recommends its evolving V4 line for new work. The restricted contract and
tests, not the parser's defaults, determine Fathomry behavior.
[Upstream version policy](https://github.com/yaml/go-yaml#version-intentions),
[pinned release](https://github.com/yaml/go-yaml/releases/tag/v3.0.5).

Viper is not a managed resource or forbidden dependency. A future application
loader may use an isolated Viper instance where its semantics fit. The resource
foundation does not pass Viper objects to Providers. A Viper v1.21.0 comparison
confirmed that configuration lookup can fall back from explicit null to a default,
case-fold mapping keys, and modify a map supplied to `MergeConfigMap`; its
`MergeConfig` does preserve nested siblings in the tested case. These behaviors
are not interchangeable with the contract above. Taking `AllSettings()` cannot
recover input distinctions already lost.
[Viper documentation](https://github.com/spf13/viper/tree/v1.21.0),
[merge/lookup implementation](https://github.com/spf13/viper/blob/v1.21.0/viper.go).

### Isolation, provenance, and revision

Preparation freezes resolved bytes, not an alias to the caller's maps or slices.
Every factory receives a freshly decoded settings copy, including when the same
selection is assembled concurrently in different scopes. Mutations by one factory
cannot change another instance's settings. The generic type boundary cannot be
reinterpreted as another configuration or capability type through an ordinary Go
conversion. Unsafe code is outside this boundary.

Callers must not mutate input bytes/defaults concurrently with preparation.
Validation/factory closures and injected dependencies have their own explicit
concurrency and ownership contracts; freezing settings does not clone closures or
stop an SDK from secretly sharing resources. Those remain integration obligations.

`Description.Revision` is an opaque, random 128-bit preparation identity. Every
successful preparation gets a new revision, even for equivalent settings; reusing
that prepared value preserves it. It identifies the frozen effective settings,
including defaults, without hashing low-entropy secrets. It does not establish
content equality, authenticity, deployment correspondence, or compatibility.
Token refresh is not implemented, and mutable tokens never belong in frozen Run
input.

`Description.Provenance` records the declared layers and schema fields they
supplied, in precedence order. It is input provenance, not a reconstructed winning
origin for every dynamic map entry. Map/list contents, arbitrary map keys, raw
documents, file paths, and environment variable names are excluded. Returned
descriptions are independent copies. `Prepared` formatting is redacted and its
JSON serialization is refused.

## Binding and ownership

Factories return a `Resource[C]` to composition. After all factories and optional
readiness checks succeed, `Bind` returns its capability `C` plus actual source
identity. Bind accepts only the exact selection token; a new token with the same
name does not match, and a missing source never falls back to another instance.

The Provider must supply an actual non-owning facade, including its dynamic type,
callbacks, and returned values. Assigning a raw client to a narrow interface does
not hide its other methods. Nil capabilities are rejected during initialization.
The generic manager cannot audit arbitrary compiled Provider code or provide a Go
security sandbox. Its tests verify the supplied facade and callback contracts,
not future SDK compliance.

Mode, account/session, and capability-specific constraints belong to the typed
Provider/capability contract and outer authorized selection. This manager does not
infer them from an endpoint or offer unrestricted business-time lookup. It does
not place mutable per-call attribution on a shared client. The
[controlled-call implementation](controlled-calls.md) adds scoped admission through
`WithLimits`/`AccessFor`, borrowing-tree completion and typed execution/evidence
handoff. Limits are shared across aliases, not reset by another binding. `Bind`
alone does not automatically wrap capability methods or enforce their constraints.
Retry policy and distributed quotas remain separately scoped.

### One authoritative resource record

| Relationship | Construction/acceptance | Scope shutdown |
| --- | --- | --- |
| Owned | A factory transfers its acquired cleanup responsibility to its assembly | Assembly invokes the record's cleanup action |
| Borrowed | `Borrow` acquires a lease on an already-ready record under a local alias | Returns only that lease; never invokes the owner's release/stop callback |
| Delegated | `Delegate` transfers exclusive ownership to the receiving assembly at acquisition | Only the receiver may release it; donor binding is refused |

Actual `Info.Scope`, Provider, name, configuration format, and revision remain
those of the originally constructed resource, even across aliases or transfer.
Current `OwnerScope` is a distinct lifecycle fact. Borrowed views never become
independent physical resource owners.

Borrowing pins the record. Owner shutdown refuses new shares and owner bindings
but cannot release a record with outstanding borrowers. Existing borrowing scopes
remain responsible for their use and may finish independently. Returning one
borrowing scope does not stop another. The owner retries shutdown explicitly after
leases end; there is no hidden background reaper.

Controlled root calls borrow this same record. Their active use prevents returning
their scope's borrowing lease and protects earlier ordered dependencies. Nested
borrowing nodes share one root admission and byte reservation; they are not another
physical resource owner. `Status.Borrowers` counts borrowing assembly scopes;
`Status.Usage` reports controlled root calls and queues separately. Existing borrowed
scopes retain their admission after owner shutdown, until their own scope closes.
Applied call limits are separately inspectable and are not part of the prepared
Provider configuration revision.

The [version-accountability API](version-accountability.md) associates original
source identity, format, preparation revision and applied limits with an explicit
effective Provider profile and consuming-binary facts. It does not equate random
preparation revisions with content equality or claim to inspect native settings.

Delegation is deliberately conservative: the donor must have exactly one entry,
with its entire owned dependency set contained within that record, and no borrowers.
Moving one record out of an ordered dependency set would lose lifetime protection,
so that mode is rejected before construction. A preflight rejection leaves
ownership with the donor. After acquisition, later recipient failure does not
return ownership implicitly: the failed recipient retains cleanup responsibility.

Composition must quiesce previously handed-out capabilities before ownership
transfer and before ending their owning/borrowing scope. Arbitrary Go values cannot
be revoked. Assembly-to-assembly delegation is implemented; a concrete SDK accepting
ownership of an injected component still needs its own explicit acceptance/failure
contract and tests. This foundation does not claim that SDK support.

## Initialization failure and truthful shutdown

Factories run in declaration order; readiness checks run only after construction
succeeds. Parsing, constructing, checking readiness, and creating remote resources
are different permissions. A `Check` callback authorizes only its documented work.

A factory must return `Acquired: true` whenever it acquires any cleanup responsibility,
including on error, and preserve every required handle through `Release`. A
non-nil release callback is conservatively treated as acquired responsibility.
Reporting no acquisition is a Provider statement about local ownership, not proof
that no remote effect happened. A factory missing its required cleanup callback
leaves an explicitly pending record; the manager cannot invent a cleanup action.

On construction or readiness failure:
- no usable assembly is exposed;
- `Report.Primary` retains the original attributed failure;
- cleanup runs with the separately supplied cleanup context;
- the returned assembly retains unresolved cleanup/return responsibility;
- the returned error preserves primary and cleanup causes through `errors.Is/As`.

Keep that returned assembly until responsibility is discharged or explicitly
handed off to an accountable owner. Do not discard it merely because construction
returned an error.

Framework-owned cancellation checks before construction, before readiness checks,
and after readiness preserve both `ctx.Err()` (`context.Canceled` or
`context.DeadlineExceeded`) and `context.Cause(ctx)`. Both the returned error and
`Report.Primary` retain deliberate `errors.Is/As` inspection, including causes
propagated from a parent context; ordinary diagnostics do not print cause text.
If a constructor or readiness callback returns its own error, that observed error
remains primary: coincident context cancellation neither replaces it nor adds an
inferred cause. Cleanup still uses the caller's separate budget, retains its own
errors and leaves unconfirmed resource/dependency responsibility pending.
Cancellation is not release, rollback or permission to retry cleanup. See the
[cancellation regressions](../../source/resources_cancellation_test.go) for
[Issue #11](https://github.com/frost-leo/fathomry/issues/11).

Dependencies, including callback captures, must precede consumers. Shutdown is
reverse-order and conservative: an incomplete resource retains all earlier
entries, including borrowing leases. This intentionally does not promise precise
cleanup of independent branches or a general dependency graph.

`ReleaseResult` reports three independent kinds of information:
- `Quiescent`: positive evidence that users/background work no longer need dependencies;
- `Released`: positive evidence that all resources owned by this record are released;
- `Err`: cleanup failure, which may coexist with either positive fact.

Both positive facts are required for completion; false means unconfirmed.
Confirmed evidence is monotonic for this non-reloadable lifecycle. These fields do
not represent business data durability or reversal of external effects.

An incomplete result can explicitly return a `Continue` action to resume cleanup
or reconcile completion. The original callback is never blindly retried. Without
a continuation, responsibility remains visible but cannot be completed by this
manager alone. A later nil/no-op result cannot supply missing positive evidence.
Historical cleanup errors remain inspectable even after confirmed completion.

`Close` is synchronous and serialized. Waiting for another Close observes the
caller's context; invoking an arbitrary callback cannot provide a hard deadline
or forcible cancellation. No goroutine is spawned to hide unfinished work.
Callbacks must cooperate with their declared budgets and must not reenter Close.
They must return errors rather than panic after acquiring resources; arbitrary
Provider panics and process termination are not recoverable ownership guarantees.

Snapshots copy metadata and error slices. They observe each record consistently,
but do not promise a single atomic instant across different owners/resources.
The lifetime of native errors themselves remains their authors' responsibility.

## Error and data-version boundaries

The shared `failure` kernel defines immutable process-local identities and
occurrences. Component/capability packages own their declarations; the kernel
does not aggregate every SDK's error catalog. It retains original Go cause
inspection while formatting only safe identity text. Structured logging of error
pointers emits fixed identity fields; native error presentation is not invoked.
Deliberately unwrapping or independently printing native causes is not sanitized.

`failure.Definition.Version` is semantic identity revision, not a wire schema.
`Diagnostic`, `Info`, and `Report` are process-local API projections, not approved
database/Temporal/message DTOs. Runtime error JSON encoding and decoding are refused.
Future persisted or cross-process contracts require their own schema identity,
bounds, absent/unknown meanings, historical-reader behavior, and compatibility tests.
Go API compatibility and configuration format evolution are separate obligations;
private runtime structs do not each get a ceremonial wire version.

The default Temporal converter does not preserve an arbitrary multi-cause Go error
tree. No Temporal failure bridge, durable evidence ledger, retry policy,
localization renderer, or Item/Run terminal decision is implemented here.
[Reference converter source, not a dependency](https://github.com/temporalio/sdk-go/blob/v1.46.0/internal/failure_converter.go#L65-L209).

## Verification and remaining obligations

The adjacent tests cover malformed configuration, precedence/null/empty semantics,
large integer precision, provenance privacy, aliasing, generic type isolation,
multi-Provider binding, per-call fixture state, partial initialization, readiness
failure, explicit continuation, shutdown uncertainty, borrowed dependencies,
delegation rejection/acceptance, concurrent sharing/shutdown, and independent public
module consumption. Race tests and a bounded parser fuzz run complement these
tests; results are recorded on the implementation PR.

Run the repository checks and the existing Go CI commands. A focused fault/race
repeat is `go test -race -count=20 -timeout=2m ./...`; a bounded parser experiment is
`go test ./source -run '^$' -fuzz '^FuzzPrepare$' -fuzztime=10s -parallel=2`.

The module minimum is Go 1.26.0. Local verification uses Go 1.26.4 on linux/amd64;
this is evidence about that toolchain, not a claim that it is the latest patch.
No service was contacted or mutated, no SDK support was verified, and no Temporal
command/serialization integration was added. Production cancellation, SDK-native
sharing, session/account isolation, background/native cleanup, service effects,
and workload/SLO limits still require concrete integration evidence.
