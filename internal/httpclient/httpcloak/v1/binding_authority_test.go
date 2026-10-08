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

package httpcloak

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	dnswire "github.com/miekg/dns"
	"github.com/sardanioss/httpcloak/transport"
)

func TestBindingBudgetMatchesExactNativeAuthorityKeys(t *testing.T) {
	certificate, roots := protocolCertificate(t, "mixed.invalid")
	var accepted atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, input *http.Request) { _, _ = io.WriteString(writer, input.Host) }))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			accepted.Add(1)
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	resolver := dnsPeer(t, func(writer dnswire.ResponseWriter, request *dnswire.Msg) {
		response := new(dnswire.Msg)
		response.SetReply(request)
		if len(request.Question) == 1 && request.Question[0].Qtype == dnswire.TypeA {
			response.Answer = []dnswire.RR{&dnswire.A{Hdr: dnswire.RR_Header{Name: request.Question[0].Name, Rrtype: dnswire.TypeA, Class: dnswire.ClassINET, Ttl: 60}, A: net.IPv4(127, 0, 0, 1)}}
		}
		_ = writer.WriteMsg(response)
	})
	options := OptionsV1{Name: "binding-key", PresetName: "chrome-148", Protocol: HTTP1, MaxActive: 1, MaxBindings: 1, MaxConnections: 1, ResolverAddress: resolver, Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: roots}}}
	fixture := bindFixture(t, options, 1)
	address, _ := url.Parse(server.URL)
	for _, host := range []string{"Mixed.invalid", "mixed.invalid"} {
		address.Host = net.JoinHostPort(host, address.Port())
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "authority"}, request(t, "GET", address.String(), nil))
		if err != nil {
			t.Fatal("one native binding retained a different case-sensitive pool", err)
		}
		if result := settle(t, fixture, receipt); string(result.Outcome.Value.DataCopy()) != address.Host {
			t.Fatal("wire authority was normalized or changed")
		}
	}
	if accepted.Load() != 2 {
		t.Fatal("native authority replacement did not retire its old socket", accepted.Load())
	}
}
