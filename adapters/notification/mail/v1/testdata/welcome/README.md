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

# Welcome email public consumer

**Audience:** independent consumers of the public Mail Adapter.

**Status:** public-only, outside-module build/test coverage with isolated SMTP/TLS
and fake local delivery. Rendering in a real recipient client requires its own
observation and authorization.

This caller owns welcome copy, the optional repository presentation and explicit
preview/send actions. It imports public Mail/adapters APIs, not Internal or the
native SDK. The original white welcome design and optional four-CID report remain;
the attached welcome pack expands its technical coverage without adding a report
engine or presentation policy to the Provider.

- `main.go`: explicit modes, independent source/finite evidence and attempt guard.
- `session.go`: caller-owned Runtime/Inbox/Owner; independent bounded observation
  and cleanup waits, including cancellation and source-first release order.
- `welcome.go` / `welcome.html`: welcome text/layout; no script, remote font or tracking.
- `preparation.go` / `pack/`: admitted parts, named templates, transforms, FS,
  borrowed Reader/scoped Writer inputs and UTF-8 attachment metadata.
- `repository.go`: bounded snapshot validation, four caller-supplied CID PNGs and CSV.
- `*_test.go`: independent MIME parsing, real loopback TLS/SMTP, signatures and
  fake-process evidence. No test reads private configuration or sends external mail.

## Start offline

From the product root:

```sh
go test -race -count=1 ./adapters/notification/mail/v1/testdata/welcome
go test -race -count=1 -run TestIndependentConsumer ./adapters/notification/mail/v1

job=$(mktemp -d /home/frost/tmp/codex-build/jobs/build.XXXXXX)
go build -o "$job/welcome" ./adapters/notification/mail/v1/testdata/welcome
"$job/welcome" -preview /path/to/new-preview.html
"$job/welcome" -preview /path/to/new-report-preview.html -report /path/to/captured-report
```

Preview opens only local owners and runs the same public PrepareMessage/Export
path as sending. It does not dial or read SMTP configuration, refuses an existing
output file and substitutes copied CID bytes with data URIs only in the browser
preview. Historical report previews retain the original capture timestamp.
Ordinary welcome tests need OpenSSL for independent S/MIME verification and a POSIX
shell for the fake sendmail peer; neither signs with real sender credentials.

## One explicitly authorized send

Sending requires a separately authorized target, an owner-only YAML file with the
existing `fathomry_mail_gh63` section, and a new attempt path:

```sh
"$job/welcome" -send -config /path/to/private.yaml \
  -attempt /path/to/new-attempt.jsonl -report /path/to/fresh-report
```

Omit `-report` for the ordinary welcome pack. The private section must have
`enabled: true` and explicit `smtp_host`, `smtp_port`, `tls_mode`, `auth`,
`hello`, `smtp_username`, `smtp_password`, `from` and `to`. Optional
`root_ca_pem` provides explicit trust for an isolated TLS peer. There is no
environment/file discovery or configuration mutation. Never put credentials in
arguments or publish the file. This example does not authorize enabling a real
deployment merely to run a test.

The command exclusively creates and syncs the attempt file before Send, appends
the sanitized final outcome after cleanup, and refuses a reused path. This is a
one-shot test guard, not a persistent outbox, delivery deduplicator or retry policy.
Do not delete it to retry an unknown effect. Creating another path is another
explicit send, not automatic reconciliation.

The summary distinguishes relay effect/stage/code, primary/cleanup failure, attempts,
source/Runtime release and evidence custody. It contains no credentials, recipient
addresses, payload or raw server response. A source record can release before a
finite record during cancellation; the consumer observes both independently of the
canceled operation context. SMTP 250 is not inbox placement or human reading.

## What the welcome payload actually tests

| Scenario | Qualification |
| --- | --- |
| Original plain/HTML welcome, optional four-CID report and CSV | Independent MIME parsing and captured SMTP bytes; authored report labels cannot become template source |
| Welcome pack | UTF-8 guide filename, FS guide, Reader CSV, scoped Writer JSON, named text/HTML template files, explicit quoted-printable/base64 and detached transform |
| Explicit file path, import and export | Absolute-path acquisition, relative-path refusal, partial writer facts, decoded-content equality and explicit native header-loss reporting |
| TLS/LOGIN and command guard | Isolated real protocol peer; duplicate attempt refuses before MAIL; configuration unchanged |
| Acceptance, lost ACK, RSET/QUIT failure | Effect and cleanup remain separate; no automatic resend |
| BCC, DSN/MDN and batches | Wire envelope/header inspection; fail-fast and explicit continuation retain positions without resending |
| DKIM plus S/MIME over the report welcome | Standard-library RSA/body-hash and OpenSSL CMS verification, exact signed entity and corruption controls; no certificate/DNS trust claim |
| Local delivery | Fake executable captures exact envelope, BCC and MIME; joined process success is not SMTP acceptance |

All these checks use the welcome theme; incompatible modes are separate cases, not
a claim that one message can exercise every option. Other authentication families,
SCRAM limits/PLUS binding, PGP structure, native faults and retained Fixed/Follow
generations remain in the [Internal controls](../../../../../../internal/notification/mail/v0/)
and [public contract consumer](../consumer/). They are not removed because the
welcome example passes. Historical research and live probes remain separate evidence.

## Optional repository report

The owner-approved report example captures public repository data before sending.
It is entirely test/maintainer tooling, not a Provider or application API.

- `repository-report.mjs` performs bounded, read-only GitHub requests, pins the
  commit-history branch SHA and writes an immutable `snapshot.json`. It does not
  send mail. It fails on incomplete pagination or failed reads rather than
  fabricating zeroes. Responses are capped at the reader/output boundary; this
  is not a whole-process/browser heap guarantee.
- `calendar-renderer.jsx` adapts react-activity-calendar 3.2.0 to repository-only
  daily commit data. Before-creation dates are explicitly unavailable, not zero
  contributions. The renderer has no access to the SMTP account.
- ECharts 6.1.0 renders weekly new stars, language-byte shares and paired monthly
  Issue/PR creation bars. Public GitHub issue listings also contain PRs; each
  record is classified once. Star buckets are service-defined, not guaranteed
  UTC-aligned, and do not establish historical net star totals.
- The captured PNGs are ordinary CID resources. The email executes no renderer
  or script and fetches no image host. Captions, text alternatives and a complete
  CSV preserve interpretation when images are unavailable.

Optional prerequisites are Node.js, curl, Playwright CLI/Chrome, and an isolated
renderer dependency directory. Keep downloaded libraries, their licenses, lockfile
and generated reports outside the product tree. No Node dependency is added to
the Go runtime or required by the ordinary Go tests.

One reproducible renderer setup, from the product root:

```sh
# Choose an existing local evidence directory, not a production path.
RENDERER=/path/to/isolated-renderer
npm install --prefix "$RENDERER" --ignore-scripts --no-audit --no-fund --save-exact \
  react@19.3.0 react-dom@19.3.0 react-activity-calendar@3.2.0 esbuild@0.28.2
NODE_PATH="$RENDERER/node_modules" "$RENDERER/node_modules/.bin/esbuild" \
  adapters/notification/mail/v1/testdata/welcome/calendar-renderer.jsx \
  --bundle --format=iife --platform=browser \
  --define:process.env.NODE_ENV='"production"' --legal-comments=linked \
  --outfile="$RENDERER/calendar-bundle.js"
```

Supply the official ECharts 6.1.0 `dist/echarts.min.js` as `ECHARTS_JS`, retaining
its Apache-2.0 notices. The script verifies Git blob SHA
`3b8ed4bcd17f7c838d86d4920af588f1a0aeb389` rather than trusting a filename.
Use a new directory for each capture; do not overwrite historical evidence:

```sh
REPORT=/path/to/new-report
node adapters/notification/mail/v1/testdata/welcome/repository-report.mjs \
  "$REPORT" "$ECHARTS_JS" "$RENDERER/calendar-bundle.js"
# PWCLI is the explicitly selected Playwright CLI executable/wrapper.
"$PWCLI" -s=mail-report open about:blank --browser=chrome
"$PWCLI" -s=mail-report run-code --filename "$REPORT/render-report.js"
```

If the command-line HTTPS path is unavailable, an explicitly running Playwright
session can be selected for the same public, non-redirecting reads with
`FATHOMRY_REPORT_BROWSER_SESSION` and `FATHOMRY_REPORT_BROWSER_CLI` (a Bash CLI
wrapper path). The reader is not silently switched. Cookies/account authority
are not needed for the public endpoints. No SMTP credentials enter the browser.
`--render` before the directory argument re-renders an existing snapshot without
network access or changing its capture timestamp.

Use the explicit preview/send modes above after rendering. Sending rejects
snapshots older than ten minutes; ordinary example tests neither read private
configuration nor contact external services. Close the owned browser session when
finished. The collector and chart tools remain optional maintainer dependencies.

Rejected decorative-image experiments are not product assets. All current chart
images are deterministically rendered from captured data; no generated logo or
AI artwork is used.
