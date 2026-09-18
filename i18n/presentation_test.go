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

package i18n_test

import (
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
)

type occurrence = failure.Error
type retryHint struct {
	occurrence
	seconds int64
}

func (hint retryHint) Seconds() int64 { return hint.seconds }

func TestTypedFailureSurvivesPresentationAndMissingDiagnostics(t *testing.T) {
	const code failure.Code = "business.request.deferred"
	const messageID = "demo.retry"
	cause := errors.New("test.cause")
	original := retryHint{occurrence: failure.New(code, cause, failure.Attribute{}), seconds: 0}
	described, ok := failure.Inspect(original)
	if !ok || !described.Diagnostic().Omitted {
		t.Fatal("rejecting optional-diagnostic control not exercised")
	}
	catalog := prepared(t, resources(t))
	expected := fixture[adversarialCases](t, "adversarial.json").Retry
	for _, locale := range []string{"en", "zh-Hans"} {
		result, err := catalog.Render(locale, messageID, i18n.Arguments{"Delay": original.Seconds()})
		if err != nil || result.Text != expected[locale] {
			t.Fatal("typed presentation failed", err)
		}
		current, ok := failure.Inspect(original)
		var typed retryHint
		var common failure.Error
		if !ok || current != described || !errors.Is(original, code) || !errors.Is(original, cause) ||
			!errors.As(original, &typed) || typed.Seconds() != 0 || !errors.As(original, &common) || common != described {
			t.Fatal("presentation changed occurrence, facts or matching")
		}
	}
	for _, id := range []string{"unavailable.message", messageID} {
		result, renderErr := catalog.Render("zh-Hans", id, nil)
		if renderErr == nil || result != (i18n.Result{}) {
			t.Fatal("render failure control did not fail")
		}
		fallback := original.Error()
		if fallback != string(code) || !errors.Is(original, code) || !errors.Is(original, cause) {
			t.Fatal("safe code fallback changed original")
		}
	}
}
