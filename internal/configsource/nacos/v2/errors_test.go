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

package nacos

import (
	"errors"
	"github.com/frost-leo/fathomry/internal/conformance"
	"testing"
)

func TestErrorAndRuntimePrivacy(t *testing.T) {
	native := &RemoteError{resultCode: 500, errorCode: 403, message: "native-message-canary"}
	err := fail(ErrDenied, "query", native)
	if !errors.Is(err, ErrDenied) {
		t.Fatal("qualified identity lost")
	}
	conformance.Cause(t, err, func(value *RemoteError) bool { return value == native })
	conformance.Private(t, err, "native-message-canary")
	for _, entry := range []struct{ value, target any }{
		{&Document{content: "document-canary", md5: "metadata-canary"}, new(Document)},
		{&Change{selected: key{"group-canary", "id-canary"}, err: err}, new(Change)},
		{&Subscription{}, new(Subscription)},
		{&Client{}, new(Client)},
		{native, new(RemoteError)},
	} {
		conformance.Runtime(t, entry.value, entry.target, "document-canary", "metadata-canary", "group-canary", "id-canary", "native-message-canary")
	}
	conformance.Private(t, (*Document)(nil), "document-canary", "metadata-canary")
	if native.Message() != "native-message-canary" || native.ErrorCode() != 403 || native.ResultCode() != 500 {
		t.Fatal("intentional native inspection changed")
	}
}
