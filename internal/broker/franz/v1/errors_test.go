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

package franz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestErrorsRetainIdentityCausesAndSafePresentation(t *testing.T) {
	secret := errors.New("private-message-and-credential-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(secret)
	err := nativeFailure(ErrProduce, "delivery", ctx, kerr.NotEnoughReplicasAfterAppend)
	for _, cause := range []error{ErrProduce, kerr.NotEnoughReplicasAfterAppend, context.Canceled, secret} {
		if !errors.Is(err, cause) {
			t.Fatal("cause lost")
		}
	}
	var broker *kerr.Error
	if !errors.As(err, &broker) || broker != kerr.NotEnoughReplicasAfterAppend {
		t.Fatal("native broker type lost")
	}
	var occurrence *fault.Error
	if !errors.As(err, &occurrence) || occurrence.Diagnostic().Context.Provider != ProviderID {
		t.Fatal("technical provider missing")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		if strings.Contains(fmt.Sprintf(format, err), "canary") {
			t.Fatal("raw error exposed")
		}
	}
	conformance.Runtime(t, err, new(fault.Error), "private-message-and-credential-canary")
}

func TestNilRuntimeLogging(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logger.Info("runtime", "client", (*Client)(nil), "consumer", (*Consumer)(nil), "options", (*OptionsV1)(nil), "record", (*Record)(nil))
	if strings.Contains(output.String(), "panic") || !strings.Contains(output.String(), "kafka[restricted]") {
		t.Fatal("typed nil runtime logging was not safe")
	}
}
func TestRuntimeDataPresentationDoesNotExposePayloads(t *testing.T) {
	values := []any{
		OptionsV1{Name: "canary", RootCAPEM: "canary"}, Message{Topic: "canary", Value: []byte("canary")},
		Header{Key: "canary", Value: []byte("canary")}, Position{ClusterID: "canary", Topic: "canary"},
		Record{key: "canary", value: "canary", headers: []header{{key: "canary", value: "canary"}}},
		Write{Err: errors.New("canary")}, Read{Err: errors.New("canary")}, Topic{Name: "canary"},
		Result{data: &resultData{writes: []Write{{Err: errors.New("canary")}}}},
	}
	for _, value := range values {
		for _, format := range []string{"%v", "%+v", "%#v", "%d", "%s", "%q"} {
			if strings.Contains(fmt.Sprintf(format, value), "canary") {
				t.Fatal("runtime formatter exposed contents")
			}
		}
	}
}
