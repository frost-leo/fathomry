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

package trino_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "github.com/trinodb/trino-go-client/trino"
)

func TestReviewSourceReadinessEvidenceReservation(t *testing.T) {
	const messageBytes = 256 << 10
	peer := newWirePeer(t, func(http.ResponseWriter, *http.Request, []byte) { t.Error("unexpected business dispatch") })
	peer.onReady = func(w http.ResponseWriter, r *http.Request) {
		writePage(t, w, map[string]any{"id": "ready_error", "error": map[string]any{
			"errorName": "INTERNAL_ERROR", "errorType": "INTERNAL_ERROR", "errorCode": 1, "message": strings.Repeat("x", messageBytes),
		}})
	}
	settings := peer.settings()
	settings.MaxPageBytes = 1 << 20
	policy, err := trino.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[trino.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := trino.Open(boundedContext(t), settings, trino.Dependencies{Runtime: runtime, Evidence: inbox})
	if err == nil || owner == nil {
		t.Fatal("failed readiness lost its owner/cause")
	}
	if err := owner.Close(boundedContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("readiness cleanup not released", err)
	}
	state, _ := runtime.Inspect()
	if state.Active != 0 || state.WorkBytes != 0 {
		t.Fatal("source work remains")
	}
	status, _ := inbox.Inspect()
	if status.Bytes != policy.SourceEvidenceBytes {
		t.Fatal("retained source evidence differs from the SQL-engine policy")
	}
	delivery, snapshot := releasedEvidence(t, inbox)
	defer delivery.Ack()
	var nativeError *sdk.ErrTrino
	if !errors.As(snapshot.Primary(), &nativeError) || len(nativeError.Message) != messageBytes {
		t.Fatal("readiness original native cause missing")
	}
	if status.Bytes < int64(len(nativeError.Message)) {
		t.Fatalf("source evidence underreserved after work release: retained native message=%d reserved=%d", len(nativeError.Message), status.Bytes)
	}
}
