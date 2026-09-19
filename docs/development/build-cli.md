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

# Build and verify the CLI

[Documentation](../README.md) / Development

**Audience:** contributors building the current executable from a checkout.
**Status:** implemented offline help/version entry; no project generator, query/control command, runtime or release pipeline.

From the repository root, use Go 1.27.0 or later and an absolute output path
outside any source fixture:

```sh
go build -trimpath -o "$PWD/../fathomry-local" ./cmd/fathomry
"$PWD/../fathomry-local" --help
"$PWD/../fathomry-local" version --output=json
```

Help and version do not require a project directory, business configuration or
reachable service. `version` and root `--version` report the consuming binary.
Human text is localized; JSON uses the [version command's CLI-owned profile](../reference/cmd/fathomry/internal/command/version/version-output.md).
Use `--lang=zh-CN` for Simplified Chinese. A valid unsupported tag falls back
to English. The default is English regardless of the host locale; current
commands do not read stdin.

Exit codes are 0 for successful help/report, 2 for usage, 1 for inspection/
render/output failure, 130 for cancellation/SIGINT and 143 for Unix SIGTERM.
Unix SIGPIPE is handled as an output failure when a write returns EPIPE.
After the first SIGINT/SIGTERM cancels work, a later signal uses native process
termination if a command or Writer remains blocked; that forceful path does
not guarantee ordinary cleanup.
Capture stdout and stderr separately: diagnostics are never a successful JSON
record. A failing pipe may already contain part of a report.

The module's local `replace` directives mean a checkout build does **not**
establish that `go install github.com/frost-leo/fathomry/cmd/fathomry@latest`
works. No tagged release or standalone distribution is claimed. The four
existing linker symbols and expected-artifact checks are documented in the
public [injection contract](../reference/version/injection.md). A producer
must compare all four expected values independently against the running built
binary; no Git, CI environment or clock fallback fills missing values.

Focused verification uses `go test -race ./cmd/fathomry/...`; the tests build
real CLI variants and distinguish local fake command tests from shipped
behavior. Before delivery, run current repository formatting, module-tidiness,
vet, full race-test and build gates. A Windows cross-build is compile evidence,
not Windows runtime acceptance. The CLI's [package boundary](../reference/cmd/fathomry/internal/root/interface.md)
does not promise a real query implementation merely because another command
family can be registered.
