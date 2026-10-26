/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package health

import (
	"context"
	"testing"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

func TestPublicBinding(t *testing.T) {
	value := Registration()
	bundle, err := declaration.Compile(context.Background(), value.Declaration)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := implementation.Prepare(context.Background(), bundle, declaration.Identity{Namespace: "example", Code: "server-health", Version: "1"}, value.Implementations[0].Bindings)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := bound.ManifestCopy()
	if err != nil || len(manifest.Bindings) != 3 || len(manifest.Calls) != 3 {
		t.Fatal("incomplete coverage", err)
	}
	if err := implementation.ValidateManifest(context.Background(), bundle, manifest); err != nil {
		t.Fatal(err)
	}
	result, err := bound.Invoke(context.Background(), "read_cpu", []byte(`{"target":"local"}`))
	if err != nil || !result.Invoked || string(result.Output) != `{"value":42}` {
		t.Fatal("checked invocation failed", err)
	}
	result, err = bound.Invoke(context.Background(), "read_cpu", []byte(`{}`))
	if err == nil || result.Invoked || result.Output != nil {
		t.Fatal("missing input was dispatched")
	}
}
