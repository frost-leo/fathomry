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

# Feishu Welcome and lifecycle caller example

This independent Linux in-module caller sends a real Fathomry Welcome card and owns
presentation and explicit private-file loading. The Provider owns neither business
metrics nor file discovery. Default sending never invents report statistics.

- `welcome.go` / `resources/welcome.json`: headerless, email-inspired Welcome copy,
  native typography/colored decorations, spacing, columns, navigation and a collapsed
  getting-started panel. No image upload, HTML screenshot or hosted page is needed.
- `repository.go`: optional captured public repository snapshot, native charts,
  source/revision/timestamp captions, numeric table and complete CSV.
- `resources/catalog.json`: fixed lifecycle/test fixture with line/bar/pie charts, numeric
  table, two-column summary, collapsible panel, link button, localized rich post
  and an **unsent** form/input/select card for callback-enabled applications.
  License metadata is outside the transmitted card object.
- `catalog.go`: test-fixture asset references, deterministic 640×300 PNG alternative and CSV.
  PNG x-axis numbers 1–4 mean Jan–Apr; bar labels are 80/120/100/160 items.
- `main.go`: explicit named assembly, separate required evidence reception,
  bounded diagnostics and cleanup; no hidden recipient discovery.
- `*_integration_test.go`: independent TLS caller test and opt-in application/
  CardKit service tests. Skips are not service qualification.

From the product root, preview without credentials or network:

```sh
go test -race ./internal/notification/lark/v3/testdata/showcase
go run ./internal/notification/lark/v3/testdata/showcase -preview /path/to/new-directory
```

Preview refuses an existing directory and writes `card.json`. With `-report`, it
also writes `repository-snapshot.csv`. This is a **payload preview**, not a
Feishu-client rendering certificate; it reads no credentials and sends nothing.

## Explicit live application use

An owner-only regular YAML file (mode 0600 or stricter, at most 1 MiB) supplies
`feishuid` and `feishusecret`. Other deployment entries are not transmitted.
Never put secrets in command arguments, fixtures, output or source control.

With separate authorization for the application and target account:

```sh
go run ./internal/notification/lark/v3/testdata/showcase \
  -send -config /path/to/private.yaml -to recipient@example.test
# Or an explicitly supplied app-scoped identity / authorized chat:
go run ./internal/notification/lark/v3/testdata/showcase \
  -send -config /path/to/private.yaml -to-type open_id -to ou_authorized
```

`-send` sends and retains one **Welcome to Fathomry** native card. With `-report`,
the same card includes repository charts and a second message supplies the CSV.
Email mode first performs a separately evidenced exact-email lookup, requiring
the corresponding contact permission. It never substitutes an unrelated user.

`-exercise` explicitly uses the fixed synthetic catalog to exercise upload,
message/read/reply/edit/CardKit/resource
APIs and attempts recall of every acknowledged created message with a separate
cleanup context. Unknown/no-ID writes cannot be reconciled automatically. IM
assets and CardKit entities have no delete endpoint here; no physical purge is
claimed. The command never retries a failed/unknown operation automatically.

App-bot availability, recipient tenant, media/message read permissions and
`cardkit:card:write` must be granted by the application owner. The command does
not change these permissions or deploy a callback/WebSocket service. Input files
must be regular files; nonblocking opens reject FIFOs without waiting for a writer.

## Optional real-data repository report

Use the `snapshot.json` produced by the mail Welcome example's
[read-only repository collector](../../../../mail/v0/testdata/welcome/README.md#optional-repository-report).
The Feishu command does not collect GitHub data during sending or render images:
it uses the captured values directly in native VChart components. No ECharts or
browser library is needed by this Go caller. The collector's optional tooling stays
outside the Go runtime and is not reimplemented here.

```sh
go run ./internal/notification/lark/v3/testdata/showcase \
  -preview /path/to/new-preview -report /path/to/captured-report/snapshot.json
go run ./internal/notification/lark/v3/testdata/showcase \
  -send -config /path/to/private.yaml -to recipient@example.test \
  -report /path/to/fresh-report/snapshot.json
```

The bounded version-1 snapshot must identify `frost-leo/fathomry`, repository ID
1360531276, branch, revision and capture timestamps. Sending rejects snapshots older
than ten minutes; previews retain the historical timestamp. Missing/null counts,
inconsistent totals, partial date ranges and ambiguous field names are rejected,
not converted to zeroes. An empty observed series is described rather than fabricated.
Validation does not authenticate a caller-supplied file: obtain it with the documented
collector and preserve its provenance.

Charts show captured weekly commits (UTC Mondays), service-defined weekly new stars,
language-byte share and monthly Issue/PR creation. Dates before repository creation
remain unavailable; first/last weeks can be partial. Stars are not historical net
totals, language bytes are not lines of code, and current counts are not an ongoing
dashboard. The complete CSV retains the original daily and monthly values. Oversized
card envelopes fail before networking instead of silently dropping data.
Tables stay at the body root, as required by Feishu; only supplementary text is
collapsed. Report insertion preserves the Welcome body's styles and closing order.
The native font sizes and theme-aware colors follow
[Card JSON 2.0](https://open.feishu.cn/document/feishu-cards/card-json-v2-structure)
and [rich-text styling](https://open.feishu.cn/document/feishu-cards/card-json-v2-components/content-components/rich-text),
not arbitrary CSS. API acceptance and local layout checks do not certify the exact
desktop/mobile appearance; inspect the received card in the Feishu client.

## Bounded WebSocket reception

`-listen` is an explicit receiver lifetime, not a deployed background service.
Construction alone opens no socket. It uses the same source/invocation ownership
as outbound messages and stops on its duration limit or process cancellation.

```sh
# Handshake/heartbeat probe; unexpected events are left unacknowledged:
go run ./internal/notification/lark/v3/testdata/showcase \
  -listen -duration=30s -config /path/to/private.yaml

# Accept only this synthetic text from the exact app-visible email account:
go run ./internal/notification/lark/v3/testdata/showcase \
  -listen -duration=4m -config /path/to/private.yaml \
  -to recipient@example.test -accept-text fathomry-gh64-ws-probe \
  -event-file /path/to/new-owner-only-event.json
```

The second mode writes an example-owned JSON event record, syncs the file and
its parent directory, and only then requests code 200. Existing files are never
overwritten. Up to eight different-text messages from the authorized sender receive
code 500 while listening continues; other senders/types stop the probe without a
success ACK.
Logs contain numeric/status projections, not event bodies, IDs or gateway URLs.
Final status includes terminal and cleanup errors after the listener and assembly
have stopped. Sender/type/text matching rejects ambiguous JSON field aliases.
This is not a durable Fathomry history format or exactly-once processing.

The application owner must enable long connection event subscriptions, including
`im.message.receive_v1` and the corresponding message permission for that probe.
A working heartbeat does not establish that subscriptions are active.
`receiver_test.go` independently verifies the file-before-ACK ordering over TLS.
See the [WebSocket contract](../../../../../../docs/reference/internal/notification/lark/v3/websocket.md).

`TestLiveNativeWebSocket` is a separate opt-in diagnostic comparison, never run
alongside the Provider receiver. It observes only whether the official SDK sees
data messages, installs no event handler and emits no success ACK. Its known
native cache worker is contained by the test process; it is not the Provider's
production runtime.

Opt-in tests:

```sh
FATHOMRY_FEISHU_CONFIG=/path/to/private.yaml \
FATHOMRY_FEISHU_RECIPIENT=recipient@example.test \
go test -race -count=1 -run '^TestLiveApplication$' -v \
  ./internal/notification/lark/v3/testdata/showcase

# Creates one synthetic unsent card; no recipient is required:
FATHOMRY_FEISHU_CONFIG=/path/to/private.yaml FATHOMRY_FEISHU_CARDKIT=1 \
go test -race -count=1 -run '^TestLiveCardKit$' -v \
  ./internal/notification/lark/v3/testdata/showcase
```

For non-email test targets set `FATHOMRY_FEISHU_RECIPIENT_TYPE` explicitly.
Current live evidence does not establish rendering on desktop/mobile, signed
custom-bot service delivery or production callback ingress. Those require their
own authorized profiles and client inspection. Core tests cover their protocol
contracts with independent local TLS/signature fixtures.
