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

# Selected native CMS signing primitive

[Documentation](../../../../../../../README.md) / Internal package reference

**Audience:** Mail integration maintainers.

**Status:** unchanged private upstream helper; not a public API or verifier.

**Package:** `github.com/frost-leo/fathomry/internal/notification/mail/v0/internal/pkcs7`.

The sole consumer is the managed Mail S/MIME path. It supplies already-bounded,
corrected MIME bytes, parsed immutable RSA/ECDSA keys and at most one intermediate.
NewSignedData/AddSigner/Detach/Finish construct detached CMS using the exact
selected go-mail algorithm. The parent retains actual crypto work and accounts
for ASN.1 materialization before release; this helper has no goroutines, runtime,
source registry, key loader or network access.

`pkcs7.go` is byte-identical to
[go-mail v0.8.1 at 2ac36f0](https://github.com/wneessen/go-mail/blob/2ac36f08567b2a215ebaffdfebcd8f82f165f256/internal/pkcs7/pkcs7.go):
SHA-256 `377076385e4f038cc3360ab83b6c985516c8f616c316c0d4d54b2804e1b020d6`.
Its upstream notices and MIT permission text are retained, rather than relicensed.
No Go module version or replacement changes.

This narrow in-module copy is needed because the selected SDK's private S/MIME
path signs through its defective 7bit serializer. Rewriting the completed signed
message would invalidate the signature; a whole-SDK fork would introduce a
distribution dependency. Reusing the selected CMS primitive permits signing and
emitting the identical corrected MIME entity without inventing CMS cryptography.
Requalify this source and the parent's independent OpenSSL/corruption controls
when updating go-mail. Historical native counterexamples remain research evidence.

[Parent contract](../../interface.md) and
[exact-entity verification](../../../../../../../../internal/notification/mail/v0/review_regression_test.go)
describe the executable boundary.
