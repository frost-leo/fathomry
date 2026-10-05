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

package minio

import (
	"net/http"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

func TestMetadataCopiesAndAbsentFacts(t *testing.T) {
	object := Object{headers: http.Header{"X-Amz-Meta-Test": {"original"}}, metadata: map[string]string{"test": "original"}, checksums: map[string]string{"SHA256": "observed"}}
	object.HeadersCopy().Set("X-Amz-Meta-Test", "changed")
	object.MetadataCopy()["test"] = "changed"
	object.ChecksumsCopy()["SHA256"] = "changed"
	if object.HeadersCopy().Get("X-Amz-Meta-Test") != "original" || object.MetadataCopy()["test"] != "original" || object.ChecksumsCopy()["SHA256"] != "observed" {
		t.Fatal("metadata alias")
	}
	empty := project(invocation.Result[native.Result]{}, adapters.Info{})
	if empty.HasData() || empty.Complete() || empty.DataCopy() != nil {
		t.Fatal("missing facts manufactured")
	}
	if _, ok := empty.Object(); ok {
		t.Fatal("missing object manufactured")
	}
}
