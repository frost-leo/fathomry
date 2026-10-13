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
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestLiveOwnerWaitsForActualRunCallback(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	p, err := Prepare(Settings{Name: "callback", ExecPath: path, TempDir: t.TempDir(), NewWindow: true, MaxSessions: new(1), Flags: map[string]string{"headless": "", "no-first-run": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var receipt *adapters.Receipt[Result]
	go func() {
		defer close(done)
		receipt, _ = owner.Client().Run(context.Background(), ContextOptions{}, func(scope *Scope) error {
			close(entered)
			<-scope.Context().Done()
			<-release
			return nil
		})
	}()
	<-entered
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = owner.Close(short)
	cancel()
	if err == nil || owner.ShutdownComplete() {
		t.Fatal("live callback was declared released", err)
	}
	close(release)
	<-done
	if receipt == nil {
		t.Fatal("accepted callback lost its receipt")
	}
	if _, err := receipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	// The cancellation may leave a separately reported native cleanup failure.
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("returned callback remained owned")
	}
	status, err := d.Evidence.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	drain(t, d, status.Outstanding)
}
func TestCallbackScopeHasNoCloseAuthority(t *testing.T) {
	var scope Scope
	if scope.Context() != nil || scope.Page() != nil {
		t.Fatal("zero scope grants authority")
	}
	if _, _, err := scope.NewPage(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}
