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

# Cache adapter shared contracts

[Documentation](../../../../README.md) / Public package reference

**Audience:** composition and cache adapter authors.
**Status:** implemented, unreleased public vocabulary.
**Package:** `github.com/frost-leo/fathomry/adapters/cache/v1`.

`Info` identifies frozen source configuration; `Attribution` records public
operation identity and the generation actually borrowed. `Attempts` separates
observed dispatch count from exactness. None asserts application success.

`Budget` declares a root-family work envelope and independent per-result evidence.
`Policy` explicitly includes concurrently owned native sources, resident work,
operation capacity and evidence custody. Source reservation lasts until actual
shutdown, including at idle. It is neither measured RSS nor a distributed quota.

This package imports no Internal/native code and owns no runtime, client, cache
policy, lock, or universal SDK interface. The [Redis adapter](../redis/v1/interface.md)
supplies concrete operations. A Redis messaging operation retains its messaging
error owner even when it shares a native source and these technical metadata types.

Runtime metadata is redacted during ordinary formatting/logging and refuses JSON
serialization. It remains deliberately inspectable; identity fields are not metric
labels or a recommendation to publish deployment information.

## Package organization

The [Adapter tree map](../../../../../adapters/README.md) defines this package's role
and file responsibilities; shared mechanisms, capability vocabulary, preparation
and concrete providers do not acquire identical APIs by convention.
