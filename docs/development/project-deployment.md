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

# Package and check a project deployment

[Documentation](../README.md) / Development

**Audience:** project and delivery-pipeline maintainers.
**Status:** configuration-only examples validated with an actual generated
published-module consumer and a non-root, read-only local container. No Worker
host, real-cluster, TLS or production Nacos qualification is implied.

## Build once, supply configuration at runtime

Run Go commands from the generated project root, after explicit dependency
resolution. Supply an existing BUILD_OUTPUT directory outside the source tree;
use a disposable build area for experiments. FATHOMRY_SOURCE is the framework
checkout containing the example files.

```sh
set -eu
test -n "$BUILD_OUTPUT" && test -d "$BUILD_OUTPUT"
GOWORK=off go mod tidy
GOWORK=off go test -race ./...
GOWORK=off CGO_ENABLED=0 go build -trimpath -o "$BUILD_OUTPUT/fathomry" .
cp "$FATHOMRY_SOURCE/docs/development/project-deployment/Dockerfile" "$BUILD_OUTPUT/"
cp "$FATHOMRY_SOURCE/docs/development/project-deployment/.dockerignore" "$BUILD_OUTPUT/"
docker build --tag ordersync-check:local "$BUILD_OUTPUT"
```

The packaging [Dockerfile](project-deployment/Dockerfile) copies only the built
binary. Its [.dockerignore](project-deployment/.dockerignore) excludes all other
inputs, including dotenv, Go sources and credentials. The local tag is for the
example; a delivery pipeline publishes once and promotes the same image digest
instead of rebuilding for each environment. Supply build version/revision/time
as described in the [application profile](../reference/cmd/fathomry/internal/command/project/create/application.md).
These are artifact facts, not YAML overrides.

From the generated project root, with an unprivileged host user:

```sh
docker run --rm --network=none --read-only \
  --user "$(id -u):$(id -g)" --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m,mode=1777 \
  --mount "type=bind,src=$PWD/.env,dst=/run/fathomry/.env,readonly" \
  --mount "type=bind,src=$PWD/config,dst=/run/fathomry/config,readonly" \
  ordersync-check:local
```

Generation requests mode 0600 for .env. The host-user override lets that same user
read the mounted file without chmodding it. In another runtime, arrange the file's
UID/GID and permissions explicitly; never make a credential file world-readable
to work around a mount error. The image defaults to UID/GID 65532. Writable /tmp
is explicit; adding future SDKs may require other owned writable locations.

The scratch example is deliberately qualified only for local configuration. It
contains no CA trust store or timezone database. For remote TLS or actual
timezone-dependent work, supply trusted roots and a timezone catalog explicitly;
Go programs can choose time/tzdata in their main package. Merely accepting a
timezone identifier does not qualify catalog resolution or scheduling.

## Kubernetes configuration-check Job

[config-check-job.yaml](project-deployment/config-check-job.yaml) contains one
immutable non-secret ConfigMap and one Job. Replace the image placeholder with
your published immutable image reference, and adapt the application name and
resource budgets. The example has one process per Pod, no restarts, and injects
Pod UID as its startup instance ID. A multi-process application needs a finer
instance identity.

The ConfigMap projects .env, config/base.yaml and
config/environment/production.yaml under one mount. Application bootstrap is
required even when environment overrides are supplied. The Job mounts the whole
directory, not individual subPath entries, and keeps the root filesystem read-only.
Kubernetes documents the update behavior and immutable option in
[ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/).
A new configuration revision needs a new immutable object and a new Job name.
This is not a transaction guarantee across independently acquired files.

Validate against the chosen cluster's API only with deployment authorization.
The repository test checks structure and runs the projected input bytes against
the actual generated executable; it does not contact a cluster or certify its
admission policies, runtime, storage permissions or scheduling.

For remote mode, explicitly override mode/provider/document descriptors and
supply the complete Nacos Settings JSON through a Secret reference, for example:

```yaml
env:
  - name: FATHOMRY_CONFIG_MODE
    value: remote
  - name: FATHOMRY_CONFIG_PROVIDER
    value: nacos
  - name: FATHOMRY_CONFIG_DOCUMENTS_JSON
    value: '[{"kind":"base","encoding":"yaml"}]'
  - name: FATHOMRY_NACOS_SETTINGS_JSON
    valueFrom:
      secretKeyRef:
        name: application-nacos-settings
        key: settings.json
```

This fragment is not a Secret creation command or a qualified remote deployment.
The secret must match the existing [Nacos settings contract](../reference/adapters/configsource/nacos/v1/interface.md),
including explicit HTTP/gRPC endpoints, fixed keys and verified TLS. Do not put
credentials in the ConfigMap, image, command line or diagnostic output. Remote
mode never falls back to the mounted local application files.

## Completion, cancellation and later workloads

The check validates one application snapshot and closes its sources. Watch=true
still uses the real observer and then exits; neither profile is a liveness or
readiness server. No database migration, catalog registration or Worker starts
implicitly. [Jobs can start a program more than once](https://kubernetes.io/docs/concepts/workloads/controllers/job/);
they are not an exactly-once mechanism for later mutating commands.

Exec-form entrypoint makes the application the signal recipient. Allow a grace
period longer than configured cleanup ceilings; the example uses 75s for the
entry's maximum 60s outer cleanup ceiling. Kubernetes sends a termination signal
before eventual forced termination, as described in
[Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination).
A forced kill cannot certify cleanup.

The existing CLI exit policy applies: cancellation is 130 unless retained
execution/output/cleanup failures have higher precedence (1). In particular,
canceling remote acquisition can retain a source failure reported by cleanup;
do not replace that fact with a fabricated success or discard its causes.
Programmatic callers retain ExecutionError for actual completion inspection and
cleanup continuation. The process boundary has no durable cleanup journal.

Future rolling deployment must separately preserve Workers required by frozen
workflow source revisions. This finite check does not implement that rollout
policy or validate those Workers.
