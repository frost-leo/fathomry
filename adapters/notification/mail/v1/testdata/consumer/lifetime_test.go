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

package consumer

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

type heldReader struct {
	entered, proceed chan struct{}
	once             sync.Once
}

func (reader *heldReader) Read(data []byte) (int, error) {
	reader.once.Do(func() { close(reader.entered) })
	<-reader.proceed
	return 0, io.EOF
}
func TestCanceledSourceWaitDoesNotReleaseBorrowedReader(t *testing.T) {
	ctx, cancel := waitContext(t)
	defer cancel()
	config := mail.Settings{Name: "held", Host: "localhost", Port: 465, Hello: "test.invalid", TLSMode: "implicit", Auth: "none", MaxActive: 1, MaxMessageBytes: 4096, MaxMIMEBytes: 8192}
	policy, err := mail.Recommend(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[mail.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := mail.Open(ctx, config, mail.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	reader := &heldReader{entered: make(chan struct{}), proceed: make(chan struct{})}
	release := sync.OnceFunc(func() { close(reader.proceed) })
	defer release()
	done := make(chan error, 1)
	message := makeMessage(t, "held-reader")
	go func() {
		_, err := owner.Client().PrepareMessage(ctx, message, mail.Preparation{Files: []mail.FileInput{{Input: mail.Input{Reader: reader}, File: mail.Attachment{Name: "fixture.txt", ContentType: "text/plain"}}}})
		done <- err
	}()
	select {
	case <-reader.entered:
	case <-ctx.Done():
		t.Fatal("reader not entered")
	}
	short, stop := context.WithTimeout(context.Background(), 15*time.Millisecond)
	err = owner.Close(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
		t.Fatal("cancellation falsely released foreign work", err)
	}
	select {
	case err := <-done:
		t.Fatal("borrowed Reader was claimed terminated", err)
	default:
	}
	short, stop = context.WithTimeout(context.Background(), 15*time.Millisecond)
	_, err = inbox.NextReleased(short)
	stop()
	if err == nil {
		t.Fatal("unjoined work appeared released")
	}
	release()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation lost", err)
		}
	case <-ctx.Done():
		t.Fatal("reader did not join")
	}
	if err = owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !owner.ShutdownComplete() {
		t.Fatal("continued cleanup not complete")
	}
	for i := 0; i < 2; i++ {
		entry, _ := take(t, ctx, inbox)
		if err = entry.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
