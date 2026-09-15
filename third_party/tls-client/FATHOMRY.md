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

# Local tls-client compatibility corrections

**Status:** SDK-only local compatibility checkpoint for #49. This is not a
Fathomry Provider implementation or a publication-ready dependency decision.
The subsequent Provider integration belongs in a separate commit.

## Publication and license gate

The original [LICENSE](LICENSE) is retained byte-for-byte. Its advertising clause
matches BSD-4-Clause, not BSD-3-Clause or MIT. The
[GNU GPL FAQ](https://www.gnu.org/licenses/gpl-faq.html.en#OrigBSD) identifies the
original BSD advertising clause as GPL-incompatible. This project is GPL-3.0-or-later.
Do not publish or distribute the combined integration until compatible permission
or another reviewed licensing resolution is established. Keeping LICENSE, using
replace, or placing sources in this directory does not resolve that gate.
Project-authored additions keep their project notices; they do not relicense the
upstream material. The embedded upstream notice in connect.go is also preserved.

## Exact source and copying boundary

- Module: github.com/bogdanfinn/tls-client v1.16.0.
- Immutable module-proxy origin: 291b8f9e1b86cc35f210bdb6bf44770bf2660ab5.
- Original module sum: h1:km3YLI6CMRLZnfrC+hGStePPryIicd3hoxFQvHB1itY=.
- Compatibility revision: v1, exported as FathomryCompatibilityRevision.
- [UPSTREAM.json](UPSTREAM.json) records original runtime-file hashes and provenance.
- Runtime root files, profiles, bandwidth, module metadata and license are retained.
  Upstream examples, CFFI applications and external-site tests are not copied.
- The moved upstream v1.16.0 Git tag is not interchangeable with the acquired
  module bytes. This is a local replacement, not a claim that upstream released
  these corrections. No Go module-cache mutation or private reference path is needed.

## Scoped corrections

| Files | Correction |
| --- | --- |
| racer.go | Independent request/header/trailer containers and replay readers for racing legs; cooperative caller cancellation including the delayed TCP leg; winner context retained through response Close; owned draining and cleanup of late/error/losing responses; reuse of the actual H3 transport. |
| fathomry_compat.go | Explicit native Close for a configuration-frozen client; owned TCP method/dependency tracking, H3 transport registry and joined racing/dial/proxy work. Original cleanup errors remain inspectable. |
| roundtripper.go | Native request and body lifetimes, tracked connections/H3 creation, and the upstream #270 setup-versus-reconnect correction including orphaned-kind handling. |
| client.go | Pre-canceled/closed admission and native hook lifetime accounting. Existing hooks and native dial options are not removed. |
| connect.go | Tracked physical proxy connections and context-driven proxy TLS setup, so a retained H2 CONNECT session does not escape cleanup. |
| socks5_udp.go | Tracked SOCKS control connection and joined cleanup of connection-owned QUIC transport/UDP resources. |
| profiles/contributed_browser_profiles.go | Go formatting only, required by repository-wide formatting checks. No profile selection or contents are changed. |

There is no new business profile, profile allowlist, Cookie/token policy, business
retry or Provider rotation logic. Native retry/racing may still produce multiple
external attempts; cancellation does not establish non-effect.

## Native Close obligations and limits

Callers close every response body, including after EOF. Close seals new managed
HTTP admission, cancels managed work and closes its transports; a caller retaining
a response body or blocking inside an opaque callback can keep Close waiting.
Do not call Close reentrantly from hooks/body callbacks or mutate native client
configuration concurrently. Raw handles obtained through native dial/WebSocket
helpers remain caller-owned; this checkpoint does not make them safe Provider APIs.

Protocol racing requires an independent GetBody reader for a nonempty upload.
Missing or nil replay readers are refused instead of sharing a consuming reader.
The patch does not invent replayability, business idempotence, exact physical
attempt counts, distributed quotas or complete response-integrity policy.

These changes are not a general certification of all upstream behavior. SDK cache
bounds, configuration provenance, independent invocation evidence, controlled
public extension exposure and complete Provider-level integrity validation remain
work for the separate Fathomry integration. Real proxy-service/SOCKS deployment
qualification and production-site behavior are not established by these tests.

## Verification entry points

From the repository root, with external-service opt-ins and proxies disabled:

- go test -race -count=1 ./internal/httpclient/tlsclient/v1
- go -C third_party/tls-client test -race -count=1 -run=^TestFathomry ./...
- go vet ./...; go test -race -count=1 -timeout=10m ./...; go build ./...
- go mod tidy -diff; go mod verify; gofmt -l .

The root compatibility test uses an independent standard-library TCP/TLS server
and official quic-go HTTP/3 peer, including both racing legs, replay bytes, winner
streaming, cancellation, connection reuse and observed connection/socket closure.
It also invokes the SDK-local private-boundary controls with the consuming Go
executable. The Linux socket check tracks socket identities, not reusable FD numbers.
Other platforms still run the peer-connection checks without claiming that OS oracle.

The standalone upstream SDK's additional full vet check reports pre-existing
unkeyed profile literals. The same diagnostics were reproduced in the unchanged
upstream source; they are not silently reformatted into unrelated source changes
or described as a clean standalone vet pass. Repository vet remains a separate gate.
Private failure/ablation logs and review material are intentionally not shipped.

## TODO(gh-49): retire only proven-obsolete corrections

Before a tls-client upgrade or framework release, inspect actual official source
for request isolation, winner/loser/error cleanup, cancellation and reconnect fixes.
Re-run original rejecting controls and the integration's integrity, proxy-isolation,
native-extension, privacy and build gates. Remove only corrections shown obsolete,
then repeat those checks on the resulting code. A version tag, release note or PR
merge alone is not a retirement criterion. Preserve Fathomry's contracts and resolve
the separate license gate before publication. Track the existing
[issue #49](https://github.com/frost-leo/fathomry/issues/49), not a new backlog item.
