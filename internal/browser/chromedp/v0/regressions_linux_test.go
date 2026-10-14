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
	"crypto/tls"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chromedp/cdproto/page"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestTLSPreparationFreezesClientContainers(t *testing.T) {
	configuration := &tls.Config{
		EncryptedClientHelloConfigList: []byte{1, 2, 3},
		Certificates:                   []tls.Certificate{{SupportedSignatureAlgorithms: []tls.SignatureScheme{tls.PSSWithSHA256}}},
	}
	options := inertOptions()
	options.Native.TLS = configuration
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	configuration.EncryptedClientHelloConfigList[0] = 9
	configuration.Certificates[0].SupportedSignatureAlgorithms[0] = tls.PSSWithSHA384
	if prepared.native.TLS.EncryptedClientHelloConfigList[0] != 1 {
		t.Error("prepared ECH bytes alias caller data")
	}
	if prepared.native.TLS.Certificates[0].SupportedSignatureAlgorithms[0] != tls.PSSWithSHA256 {
		t.Error("prepared client certificate signature list aliases caller data")
	}
}

func TestInternalCloseDeadlineDuringNewPage(t *testing.T) {
	options := browserOptions(t)
	options.SessionTimeout = 5 * time.Second
	f := bindFixture(t, options, 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "review-page-close"})
	if err != nil {
		t.Fatal(err)
	}
	process := f.client.owner.process.Process
	defer process.Signal(syscall.SIGCONT)
	if err := process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	newPageContext, cancelNewPage := context.WithCancel(context.Background())
	defer cancelNewPage()
	pageDone := make(chan struct{})
	go func() {
		defer close(pageDone)
		_, _ = retained.NewPage(newPageContext)
	}()
	locked := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		if strings.Contains(string(stack[:n]), "/v0.(*Context).NewPage") && strings.Contains(string(stack[:n]), "chromedp.(*Browser).Call") {
			locked = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !locked {
		_ = process.Signal(syscall.SIGCONT)
		cancelNewPage()
		<-pageDone
		t.Fatal("NewPage did not enter the protected operation")
	}
	closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelClose()
	closeDone := make(chan error, 1)
	go func() { closeDone <- retained.Close(closeContext) }()
	select {
	case <-closeDone:
		t.Log("close honored its deadline while NewPage awaited the peer")
	case <-time.After(200 * time.Millisecond):
		t.Error("Close remained blocked on NewPage mutex after its 10 ms deadline")
		_ = process.Signal(syscall.SIGCONT)
		cancelNewPage()
		<-closeDone
	}
	_ = process.Signal(syscall.SIGCONT)
	cancelNewPage()
	<-pageDone
	_ = retained.Close(context.Background())
	delivery, err := f.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunStreamCleanupUsesCleanupBudget(t *testing.T) {
	options := browserOptions(t)
	options.CleanupTimeout = 100 * time.Millisecond
	options.SessionTimeout = 5 * time.Second
	f := bindFixture(t, options, 1)
	if err := f.client.owner.start(deadline(t)); err != nil {
		t.Fatal(err)
	}
	process := f.client.owner.process.Process
	defer process.Signal(syscall.SIGCONT)
	cleanup, cancelCleanup := context.WithCancel(context.Background())
	defer cancelCleanup()
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := f.client.Run(context.Background(), cleanup, fault.Correlation{Call: "review-stream-close"}, func(session *Session) error {
			if _, err := session.PDFStream(context.Background(), page.PrintToPDFParams{}); err != nil {
				return err
			}
			if err := process.Signal(syscall.SIGSTOP); err != nil {
				return err
			}
			cancelCleanup()
			close(entered)
			return nil
		})
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal("fixture setup failed before controlled suspension", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fixture did not reach controlled suspension")
	}
	select {
	case <-done:
		t.Log("Run returned under its cleanup budget")
	case <-time.After(400 * time.Millisecond):
		t.Error("Run ignored canceled cleanup context and 100 ms cleanup budget in IO.close")
		_ = process.Signal(syscall.SIGCONT)
		<-done
	}
	_ = process.Signal(syscall.SIGCONT)
	delivery, err := f.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
}
