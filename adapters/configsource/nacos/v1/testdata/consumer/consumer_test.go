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

package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	prepare "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestPublicNacos(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusForbidden) }))
	defer service.Close()
	raw, err := json.Marshal(nacos.Settings{Name: "consumer", Servers: []nacos.Server{{HTTPURL: service.URL + "/nacos", GRPCAddress: "127.0.0.1:1"}}, DynamicKeys: true, AllowInsecure: true, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepare.Prepare(context.Background(), prepare.Schema[nacos.Settings]{Version: 1}, []prepare.Layer{{Kind: prepare.Base, Encoding: prepare.JSON, Content: raw}})
	if err != nil {
		t.Fatal(err)
	}
	settings, err := prepared.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	if err := nacos.Validate(settings); err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), adapters.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[nacos.Evidence](adapters.EvidenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := nacos.Open(context.Background(), settings, nacos.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	_, err = owner.Client().Search(context.Background(), nacos.SearchInput{Mode: "accurate"})
	evidence, ok := nacos.InspectError(err)
	if !ok || evidence.HTTPStatus() != 403 || !errors.Is(err, nacos.ErrDenied) {
		t.Fatal("public native evidence inaccessible", err)
	}
	marker := errors.New("native-canary")
	component := i18n.Component{Module: "fathomry", Name: "configsource_nacos", BaseLocale: "en", Directory: "resources", Resources: nacos.Resources(), Definitions: nacos.Definitions()}
	catalog, err := i18n.Prepare(append(i18n.CoreComponents(), component)...)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range nacos.Definitions() {
		original, err := failure.New(definition, failure.Location{Operation: "consumer"}, marker)
		if err != nil {
			t.Fatal(err)
		}
		presented := presenter.Present(original).(*i18n.Presented)
		if presented.Issue() != nil || !errors.Is(presented, marker) || strings.Contains(presented.Error(), "native-canary") {
			t.Fatal("presentation changed evidence")
		}
		if _, found, err := catalog.Explain(definition.Code, "en"); err != nil || !found {
			t.Fatal("missing atlas entry")
		}
	}
	if err := owner.Close(context.Background()); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	status, _ := inbox.Inspect()
	for range status.Queued {
		if err := inbox.DeliverOne(context.Background(), func(context.Context, adapters.Snapshot[nacos.Evidence]) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
}
