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

# Welcome email caller example

This standalone command is an ordinary in-module consumer of the mail Provider.
It owns welcome copy, private deployment-file reading, report presentation and
explicit preview/send actions. None of these responsibilities are compiled into
the Provider or its core tests. The Provider implementation is not modified to
expose private helpers for this example.

- `main.go`: explicit CLI modes, named resource assembly, independent evidence
  reception and separately bounded cleanup.
- `welcome.go` / `welcome.html`: embedded English HTML/text with a white
  background and sparse static confetti; no logo, script, remote font or tracking.
- `repository.go`: this example's snapshot validation, four CID inputs and CSV.
- `*_test.go`: example-specific validation and an independent loopback SMTP
  receiver, using only the Provider's exported API.

From the product root:

```sh
go test -race ./internal/notification/mail/v0/testdata/welcome
go run ./internal/notification/mail/v0/testdata/welcome -preview /path/to/new-preview.html
go run ./internal/notification/mail/v0/testdata/welcome \
  -preview /path/to/new-report-preview.html -report /path/to/captured-report
```

Previewing sends nothing, refuses existing output files and never reads SMTP
configuration. Historical report previews retain the original capture timestamp.

To send **one** separately authorized message, use `-send` and an explicit
owner-only YAML file containing the `fathomry_mail_gh63` section:

```sh
go run ./internal/notification/mail/v0/testdata/welcome \
  -send -config /path/to/private.yaml -report /path/to/fresh-report
```

Omit `-report` for the plain welcome example. The private section must have
`enabled: true` and explicit `smtp_host`, `smtp_port`, `tls_mode`, `auth`,
`hello`, `smtp_username`, `smtp_password`, `from` and `to`. Do not publish
that file or put passwords in command arguments. No environment/file discovery
occurs. The command prints only bounded technical submission evidence, never
credentials or message bodies. It does not mutate the configuration, retry an
unknown effect, read a mailbox or claim inbox placement. The owner must disable
live sending when finished.

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
  internal/notification/mail/v0/testdata/welcome/calendar-renderer.jsx \
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
node internal/notification/mail/v0/testdata/welcome/repository-report.mjs \
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
