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

package nuki

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	nativetls "github.com/nukilabs/utls"
	"github.com/quic-go/quic-go/http3"
)

func socketIDs(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]bool{}
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && strings.HasPrefix(target, "socket:[") {
			result[target] = true
		}
	}
	return result
}

func TestProviderTerminalCloseReleasesObservedH3Socket(t *testing.T) {
	if runtime.GOOS != "linux" {
		return
	}
	certificate := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer certificate.Close()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &http3.Server{TLSConfig: &tls.Config{Certificates: certificate.TLS.Certificates}, Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "ok") })}
	done := make(chan error, 1)
	go func() { done <- peer.Serve(packet) }()
	defer func() { peer.Close(); packet.Close(); <-done }()
	options := providerOptions()
	options.Mode = HTTP3Only
	options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true}
	fixture := bindProvider(t, options)
	before := socketIDs(t)
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "socket"}, nativeRequest(t, "GET", "https://"+packet.LocalAddr().String(), nil))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	after := socketIDs(t)
	var owned []string
	for id := range after {
		if !before[id] {
			owned = append(owned, id)
		}
	}
	if len(owned) == 0 {
		t.Fatal("independent oracle did not observe the native socket")
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		remaining := false
		current := socketIDs(t)
		for _, id := range owned {
			remaining = remaining || current[id]
		}
		if !remaining {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal release retained an observed descriptor")
		}
		time.Sleep(time.Millisecond)
	}
}
