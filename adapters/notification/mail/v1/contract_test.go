/*
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
*/

package mail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	native "github.com/frost-leo/fathomry/internal/notification/mail/v0"
)

func TestOfflineCatalogErrorsAndPrivacy(t *testing.T) {
	catalog, err := failure.Prepare(Definitions()...)
	if err != nil {
		t.Fatal(err)
	}
	_ = catalog
	_, err = i18n.Prepare(i18n.Component{Module: "fathomry", Name: "notification_mail", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	secret := errors.New("private-mail-canary")
	original := fail(ErrInput, "fixture", secret)
	for _, value := range []error{original, fmt.Errorf("private wrapper: %w", original), errors.Join(original, fail(ErrState, "state"))} {
		mapped := translate(value, "outer")
		if !errors.Is(mapped, secret) || !errors.Is(mapped, ErrInput) {
			t.Fatal("public cause lost")
		}
		if strings.Contains(fmt.Sprintf("%+v", mapped), "private") {
			t.Fatal("public error leak")
		}
	}
	mapped := translate(native.ErrSend.New(fault.Context{}, secret), "send")
	if !errors.Is(mapped, ErrSend) || !errors.Is(mapped, secret) {
		t.Fatal("native mapping")
	}
	var output strings.Builder
	slog.New(slog.NewTextHandler(&output, nil)).Error("fixture", "error", mapped, "settings", Settings{Password: "private-mail-canary"})
	if strings.Contains(output.String(), "private-mail-canary") {
		t.Fatal("slog leak")
	}
	for _, value := range []any{Result{}, Message{}, Content{Subject: "private"}, NativeOptions{}, DKIM{}, SMIME{}, new(Owner), new(Client), new(ResponseReader)} {
		if data, err := json.Marshal(value); err == nil {
			t.Fatalf("runtime serialized %T: %s", value, data)
		}
	}
}
func TestExactPreparationAndRejectedConstruction(t *testing.T) {
	value := Settings{Name: "test", Host: "localhost", Port: 465, Hello: "test.invalid", TLSMode: "implicit", Auth: "none", MaxActive: 1}
	prepared, err := Prepare(value, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	nativePrepared, err := native.PrepareV1(options(value, NativeOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if prepared.metadata != nativePrepared.Metadata() {
		t.Fatal("effective native preparation diverged")
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[Result](adapters.EvidenceOptions{Capacity: 1, MaxBytes: policy.Evidence.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Open(context.Background(), Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("undersized owner retained authority", err)
	}
	status, _ := inbox.Inspect()
	if status.Outstanding != 0 {
		t.Fatal("preconstruction rejection retained evidence")
	}
}
