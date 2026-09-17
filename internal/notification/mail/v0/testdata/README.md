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

# Mail test data

The Provider's package directory contains only its implementation and core tests.

| Test category | Location | Scope |
| --- | --- | --- |
| Unit tests | `options_test.go`, `errors_test.go`, `message*_test.go` | Configuration, errors, immutable input and in-memory MIME; fuzzing and benchmarks |
| Protocol integration | `*_integration_test.go` | Owned loopback SMTP/TLS, effects, cancellation and cleanup |
| Foundation integration | `integration_test.go`, `integration_fixture_test.go` | Resource/invocation/conformance and the actual consuming executable |
| Go documentation examples | `example_test.go` (`package mail_test`) | Exported calling contracts; deterministic output; no external sending |
| Application example | [`welcome/`](welcome/README.md) | Independent caller for welcome copy, repository data, charts, previews and explicitly authorized live sending |

`consumer/main.go` is a minimal consuming-executable fixture. It constructs,
binds and closes a source without dialing.

`fuzz/FuzzMessageHeaders/` retains the machine-generated whitespace-subject
counterexample. This metadata and its full license notice apply to those
non-commentable corpus files.

The welcome example has its own unit and caller-integration tests. It imports
the Provider and existing foundation packages; it cannot access private functions
such as `compose` or `defaults`. Go's `./...` pattern skips testdata, so these
tests are explicitly run separately in CI:

```sh
go test -race ./internal/notification/mail/v0
go test -run '^Example' ./internal/notification/mail/v0
go test -race ./internal/notification/mail/v0/testdata/welcome
```

Neither command sends real email or fetches GitHub data. See the example's README
for deliberate preview/render/send commands and their authorization requirements.
