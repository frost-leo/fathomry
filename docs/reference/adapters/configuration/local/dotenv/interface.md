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

# Explicit dotenv acquisition

[Documentation](../../../../../README.md) / Public adapter reference

**Audience:** projects selecting dotenv bootstrap or configuration variables.
**Status:** implemented literal file acquisition; no shell compatibility, discovery,
watching or secret-manager integration.
**Package:** `github.com/frost-leo/fathomry/adapters/configuration/local/dotenv`.

## Call sequence and ownership

Call `ReadFile(ctx, absolutePath)` for one explicitly selected required regular
file. The path must already be clean and is limited to 4096 UTF-8 bytes. Symlinks
follow normal OS semantics, including explicitly selected secret mounts. The
file must remain stable during reading. It is closed before return.

The returned immutable `Values` supports concurrent reads; copies share private
immutable strings. `Lookup` reads the file only. `LookupWithProcess` first reads
the exact process name and uses the file only when the process name is absent.
An explicitly empty process value wins. Neither method modifies or enumerates
the process environment. Its zero value is a valid empty file.

Pass either method as
[configuration.Request.LookupVariable](../../../../framework/configuration/interface.md).
Only declared names are looked up and each name is captured once per load.
Safe origins are `dotenv` or `process`; paths, names and values are not included
in source descriptions. Keep the process environment stable when a coherent
multi-name view is required. No atomic file/process snapshot is promised.

## Literal format and limits

Input must be UTF-8 without NUL, at most 64 KiB and 128 assignments. Names use
ASCII letters/digits/underscore, cannot start with a digit, and are <=256 bytes.
Supported syntax:

- `KEY=value`, empty values, surrounding whitespace and optional `export `.
- Blank lines and full-line comments; unquoted inline comments start at a `#`
  preceded by whitespace (or the start of the value).
- Single-quoted literal values, including dollar signs and hash characters.
- Double-quoted values using Go string escapes, such as `\n`; trailing comments.
- CRLF or LF line endings.

Dollar expressions are always literal, never interpolated from the environment.
No sourcing, command execution, YAML syntax, multiline source strings or recursive
file includes are supported. Duplicate names and malformed unused assignments
reject the whole file. This deliberate subset is not a promise of compatibility
with every dotenv library or shell.

## Failure and privacy

A selected missing file fails; optionality/discovery is not hidden in this API.
Invalid input, unavailable acquisition, invalid content and capacity failures use
the public configuration identities. Safe absence/permission categories and
caller cancellation causes remain inspectable. Native filesystem/parser text,
paths and contents are withheld. Failure returns no partially usable values.

Read/Stat/Open/Close are synchronous and cancellation is cooperative, not a hard
filesystem deadline. No background work or persistent resource is retained.
Formatting is restricted and JSON persistence is refused; intentional lookup
exposes sensitive data and must not be logged.

Keep actual dotenv files out of version control and restrict their permissions.
Commit examples without real credentials. A selected dotenv file is input, not
a deployment-secret storage service.

## Evidence

[Tests](../../../../../../adapters/configuration/local/dotenv/dotenv_test.go)
exercise literal non-expansion, no process mutation, empty process precedence,
concurrent snapshots, malformed/duplicate/oversized input, privacy and cancellation.
These are local checks, not hostile-filesystem or production-secret qualification.
