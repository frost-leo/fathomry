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

# Error-code layout and allocation

[Documentation](../../../README.md) / [Failure interface](interface.md)

**Audience:** framework, Adapter and extension authors allocating public errors.
**Status:** implemented 32-bit protocol, capability-domain allocation and catalog ownership
checks. This is not Windows ABI integration or a global plugin registry.
**Package:** `github.com/frost-leo/fathomry/failure/v1`.

## Decode the number

A Code is `uint32`. It adopts the field positions of Microsoft's
[customer-defined HRESULT layout](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-erref/0642cb2f-2075-4469-918c-4441e69c548a),
with only the error-result form admitted:

| Bits | Field | Fathomry rule |
| --- | --- | --- |
| 31 | S: failure | Always 1; successful Go calls return nil, not an error occurrence |
| 30 | R: reserved | Always 0 |
| 29 | C: customer-defined | Always 1, including first-party Fathomry; these are not Microsoft assignments |
| 28 | N: embedded NTSTATUS | Always 0; native errors stay in causes |
| 27 | X: reserved | Always 0 |
| 26..16 | Facility | Stable subsystem in a capability domain, 0x001..0x7FF |
| 15..0 | Number | Owner-local error definition, 0x0001..0xFFFF |

`Code = 0xA0000000 | (Facility << 16) | Number`.

For example, `0xA0010001` identifies failure's invalid-code definition:
customer failure, facility 0x001, local number 0x0001.
`0xA0410001` identifies settings' invalid-snapshot definition in configuration.
Zero, zero-valued fields, foreign flags and reserved bits are invalid.
`MakeCode` checks inputs before composing; `Facility` and `Number` decode a
valid Code and return zero on invalid codes. `Code.Valid` checks representation,
not whether the selected catalog contains a definition.

S is not a logging level, fatality, retry permission or proof of external effects.
Language, location/instance, semantic revision and SDK version are not code bits.
NTSTATUS has a different severity/facility layout and is not interchangeable.
Never convert a native status by casting, truncating or masking. Preserve its
original error object as a cause, even if its own number exceeds 32 bits.

Codes use eight uppercase hexadecimal digits with a `0x` prefix in logs and
text/JSON encoding. Query parsing also accepts unsigned decimal and `0x`/`0X`
hexadecimal. Signed HRESULT decimal spellings are not accepted. JSON numbers/null
are refused to keep one explicit representation, not because uint32 would lose
precision in common JSON consumers. Invalid decoding leaves the receiver unchanged.
The unreleased uint64 and layer-based allocation drafts have no compatibility aliases.

## Allocate by capability, not code layer

The hierarchy is **capability domain -> logical subsystem -> error definition**.
Facility numbers do not classify Adapter/Framework/CLI layers, filenames, instances,
SDK major versions, import hashes or registration order. A database capability
does not become a different domain when a Framework scenario calls it.

The authoritative capability manifest is
[`Domains`](../../../../failure/v1/domains.go). Each concrete subsystem obtains
its own facility within its domain, not a share of one domain-wide reason counter.
Extension facilities mirror the same capability bands at offset 0x400:

| Capability domain | First-party facilities | Extension facilities | Scope/examples, not public availability |
| --- | --- | --- | --- |
| Core contracts | 0x001..0x03F | 0x400..0x43F | Error, locale and cross-cutting resource contracts |
| Configuration | 0x040..0x07F | 0x440..0x47F | Settings, source loading/Watch, Viper/Nacos |
| Database | 0x080..0x0FF | 0x480..0x4FF | PostgreSQL/MySQL and DuckDB/Doris/Trino SQL capabilities |
| Cache | 0x100..0x13F | 0x500..0x53F | Cache/key-value capabilities such as Redis |
| Object storage | 0x140..0x17F | 0x540..0x57F | Object operations such as S3/MinIO |
| Messaging | 0x180..0x1BF | 0x580..0x5BF | Kafka and other messaging capabilities |
| Orchestration | 0x1C0..0x1FF | 0x5C0..0x5FF | Workflow/task execution such as Temporal |
| Network | 0x200..0x27F | 0x600..0x67F | HTTP and other network capability contracts |
| Observability | 0x280..0x2BF | 0x680..0x6BF | Logging, tracing and metrics; Zap/zerolog/OTel |
| Notification | 0x2C0..0x2FF | 0x6C0..0x6FF | SMTP, Lark and other delivery channels |
| Browser | 0x300..0x31F | 0x700..0x71F | Browser automation such as chromedp |
| Table format | 0x320..0x33F | 0x720..0x73F | Iceberg-style table/schema/snapshot semantics |
| Application | 0x340..0x37F | 0x740..0x77F | Project/business-specific semantics |
| Reserved growth | 0x380..0x3FF | 0x780..0x7FF | No current domain or declarable definitions |

Facility 0 remains invalid. `DomainRange.First/Last` describe base bands; the core
base starts at zero so extension 0x400 can be classified, not to admit facility 0.
Mirrored slots share a capability domain, not the identity of a first-party owner.

Each facility has 65,535 nonzero local error numbers. These are namespace capacities,
not permission to exceed the [catalog admission bounds](interface.md#admission-and-bounds).
Bands are capacity policy, not new fixed-width domain/component fields inside Code.
Reserved growth can supply additional non-overlapping bands without renumbering
existing facilities. A range reserves a classification; it does not allocate a
specific SDK's error definitions or prove full Adapter coverage.

`Code.Domain` and `Facility.Domain` return a known capability label, or empty for
an invalid number/unassigned domain. `Code.Valid` distinguishes malformed numbers
from well-formed future/unknown codes. Unknown codes remain queryable as absent,
but definitions cannot claim reserved domain bands. `Catalog.Components` includes
the domain alongside its exact subsystem owner, facility and codes.

Classify the public semantic capability, not the backend brand. A Redis-backed
configuration source belongs to configuration; a Redis Streams messaging capability
belongs to messaging. These require different logical subsystem owners even if
implemented using the same SDK. Iceberg table operations do not become object-storage
errors merely because their data files use S3. Likewise, a new configuration-load
failure may retain a database cause without taking the cause's domain as its own.
Forwarding an existing error across layers does not by itself justify a new code.
Application is not a catch-all category for everything implemented in Framework.

## Assign the subsystem owner

The authoritative first-party manifest is
[`Allocations`](../../../../failure/v1/allocations.go):

| Facility | Module / component | Availability |
| --- | --- | --- |
| 0x001 | fathomry / failure | Implemented |
| 0x003 | fathomry / i18n | [Resource atlas and error presentation](../../i18n/v1/interface.md) |
| 0x004 | fathomry / resource | [Runtime instance ownership and configuration adoption](../../resource/v1/interface.md) |
| 0x005 | fathomry / operation | [Public operation ownership and evidence custody](../../adapters/v1/interface.md) |
| 0x006 | fathomry / assembly | [Public runtime composition and evidence reception](../../framework/v1/interface.md) |
| 0x007 | fathomry / command_line | [Command invocation and terminal I/O](../../cmd/fathomry/interface.md), not a CLI-layer allocation band |
| 0x00A | fathomry / project_creation | [Dependency admission and exclusive project creation](../../cmd/fathomry/interface.md#create-an-independent-project) |
| 0x041 | fathomry / settings | [Typed configuration data storage/access](../../settings/v1/interface.md) |
| 0x042 | fathomry / configuration_data | [Independent strict preparation and raw batch contract](../../adapters/configsource/v1/interface.md) |
| 0x043 | fathomry / configsource_viper | [Public Viper configuration capability](../../adapters/configsource/viper/v1/interface.md) |
| 0x044 | fathomry / configsource_nacos | [Public Nacos configuration capability](../../adapters/configsource/nacos/v1/interface.md) |
| 0x045 | fathomry / configuration | [Typed configuration acceptance and publication](../../framework/configuration/v1/interface.md) |
| 0x080 | fathomry / database_postgres | [Public PostgreSQL SQL and ownership](../../adapters/database/postgres/v1/interface.md) |
| 0x081 | fathomry / database_mysql | [Public MySQL SQL and ownership](../../adapters/database/mysql/v1/interface.md) |
| 0x082 | fathomry / database_duckdb | [Public embedded DuckDB SQL and ownership](../../adapters/sqlengine/duckdb/v1/interface.md) |
| 0x083 | fathomry / database_trino | [Public Trino SQL, bounded reading and ownership](../../adapters/sqlengine/trino/v1/interface.md) |
| 0x084 | fathomry / database_doris | [Public Doris SQL, Stream Load and retained result ownership](../../adapters/sqlengine/doris/v1/interface.md) |
| 0x140 | fathomry / objectstore_minio | [Public MinIO object-storage operations and ownership](../../adapters/objectstore/minio/v1/interface.md) |
| 0x180 | fathomry / broker_kafka | [Public Kafka broker and ownership](../../adapters/broker/kafka/v1/interface.md) |
| 0x100 | fathomry / cache_redis | [Redis key-value/cache and source ownership](../../adapters/cache/redis/v1/interface.md) |
| 0x181 | fathomry / messaging_redis | [Redis messaging and mixed-command attribution](../../adapters/cache/redis/v1/interface.md) |
| 0x200 | fathomry / http_nethttp | [Standard HTTP requests, streams and direct-connection ownership](../../adapters/httpclient/nethttp/v1/interface.md) |
| 0x201 | fathomry / http_tlsclient | [Native-profile HTTP, racing, streams and measured bandwidth](../../adapters/httpclient/tlsclient/v1/interface.md) |

Unlisted first-party facilities cannot be used by definitions. Future modules add
reviewed entries in their capability range. The previous unreleased settings slot
0x002 is no longer allocated and does not alias 0x041. Declaration admission checks the exact
first-party owner; the `fathomry` module namespace and its dotted subnamespaces
cannot use the extension range. This enforces consistency, not author authentication.

Declare explicit local-number constants, never `iota`:

```go
const facility failure.Facility = 0x442 // A coordinated configuration subsystem.
const denied failure.Code = failure.ErrorPrefix | failure.Code(facility)<<16 | 0x0001
```

Publish the corresponding Definition, Identifier, owner, explanation and any
component-owned detail contract together. A number does not define its own meaning.

## Compose extensions without ambiguous lookup

Windows permits some
[interface-relative HRESULT meanings](https://learn.microsoft.com/en-us/windows/win32/com/codes-in-facility-itf).
Fathomry requires more for its numeric atlas: within one prepared catalog each
facility identifies exactly one module/component, and each owner uses exactly
one facility. Disjoint local numbers do not excuse conflicting ownership.
Duplicate codes or Identifiers also reject. There is no load-order override,
automatic remapping or silent collision resolution.

Extension authors coordinate stable facilities with the target project's catalog
owner before publishing constants. Reusable components publish their allocations;
consumers compose and check the complete selected catalog before use. A standalone
constructor has no global registry and cannot detect an unassembled extension.

The extension range is not globally collision-free across unrelated distributions.
CLI lookup must use the intended distribution's catalog. A 32-bit integer cannot
uniquely identify independently self-assigned errors without an allocation authority
or additional context. No central online registration service is implied.

Allocations returns detached first-party metadata. Catalog.Components reports
the selected definitions' capability domains, owners, facilities and codes, not loaded SDKs or instances.
Native Windows message resources do not contain Fathomry explanations merely because
the bit layout is familiar.

## Preserve identity over time

- Assign numbers explicitly; review facility and local-number uniqueness.
- Assign new meanings new numbers. Revision, language or declaration order must
  never change an existing number's meaning.
- Keep retired definitions and allocations as reserved tombstones; never recycle
  their constants. No retirements exist yet in this new manifest.
- Preserve logical owner/number identity across package moves and SDK upgrades.
- Do not derive codes from native values or allocate them at startup.
  Native evidence and public semantics coexist.

[Layout/allocation tests](../../../../failure/v1/allocations_test.go),
[exhaustive domain tests](../../../../failure/v1/domains_test.go),
[parsing/fuzz tests](../../../../failure/v1/code_test.go) and the
[independent consumer](../../../../failure/v1/integration_test.go) exercise field
boundaries, conflicts, exact width, invalid legacy spellings and constant overflow.
The [native fixture](../../../../internal/conformance/failure_v1_test.go) preserves
actual SDK causes independently of the public code's width.
