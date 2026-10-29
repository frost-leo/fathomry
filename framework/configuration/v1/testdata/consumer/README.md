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

# Independent configuration consumer

This is a hand-authored acceptance fixture, not generated project output.
It demonstrates the public `framework/v1` and `framework/configuration/v1` APIs
with a project-owned setting package split by application, database and cache.

Run it through the parent harness from the repository root:

```sh
go test -race -count=1 ./framework/configuration/v1 -run '^TestIndependentConsumer$'
```

The harness creates an isolated loopback Nacos protocol peer, copies this fixture
to a temporary independent Go module, and supplies only that peer's explicit test
endpoints. The consumer defines two explicit environment/input plans, loads local
bootstrap data, starts local or remote application configuration, validates and
reads typed snapshots, then closes owned work and acknowledges provider evidence.

The harness checks module tidiness, vet/race behavior and dependency boundaries.
It uses a local replacement of the candidate source; this proves public API
isolation, not published-module distribution or deployed Nacos qualification.
Do not run the fixture directly inside the product module or point its test
variables at production services.

No generated templates, startup commands or application Boot container are added.
