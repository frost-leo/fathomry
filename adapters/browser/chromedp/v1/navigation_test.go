/*
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

package chromedp

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestLiveNavigationUsesSharedOrdinaryCommandBudget(t *testing.T) {
	var first, refused atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/first":
			first.Add(1)
		case "/refused":
			refused.Add(1)
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<h1>navigation budget</h1>"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL+"/first", 1)
	if first.Load() != 1 {
		t.Fatal("positive navigation control did not reach the fixture")
	}
	second, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []*Page{retained.Page(), second} {
		receipt, err := page.Navigate(bounded(t), server.URL+"/refused")
		if !errors.Is(err, ErrLimit) || receipt == nil {
			t.Fatal("navigation bypassed the shared command budget", err)
		}
		snapshot, err := receipt.WaitReleased(bounded(t))
		if err != nil || !errors.Is(snapshot.Primary(), ErrLimit) {
			t.Fatal("ignored navigation refusal lost independent evidence", err)
		}
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || refused.Load() != 0 {
		t.Fatal("refused navigation reached the peer or blocked cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
