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

package tlsclient

import (
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
)

func profileWithID(base profiles.ClientProfile, id tls.ClientHelloID) profiles.ClientProfile {
	return profiles.NewClientProfile(id, base.GetSettings(), base.GetSettingsOrder(), base.GetPseudoHeaderOrder(), base.GetConnectionFlow(), base.GetPriorities(), base.GetHeaderPriority(),
		base.GetStreamID(), base.GetAllowHTTP(), base.GetHttp3Settings(), base.GetHttp3SettingsOrder(), base.GetHttp3PriorityParam(), base.GetHttp3PseudoHeaderOrder(), base.GetHttp3SendGreaseFrames())
}

func TestProviderExplicitFactoryErrorsPreserveIdentity(t *testing.T) {
	for _, anonymous := range []bool{false, true} {
		for _, failAt := range []int64{1, 2} {
			name := "native-name"
			if anonymous {
				name = "anonymous"
			}
			if failAt == 2 {
				name += "-handshake"
			} else {
				name += "-construction"
			}
			t.Run(name, func(t *testing.T) {
				var reached, calls atomic.Int64
				endpoint, options := providerPeer(t, Negotiated, func(writer http.ResponseWriter, request *http.Request) {
					reached.Add(1)
					_, _ = io.WriteString(writer, "unexpected")
				})
				id := tls.HelloChrome_120
				if anonymous {
					id.Client, id.Version = "", ""
				}
				cause := errors.New("synthetic profile refusal")
				id.SpecFactory = func() (tls.ClientHelloSpec, error) {
					if calls.Add(1) >= failAt {
						return tls.ClientHelloSpec{}, cause
					}
					return profiles.Chrome_120.GetClientHelloSpec()
				}
				profile := profileWithID(*options.Native.Profile, id)
				options.Native.Profile = &profile
				fixture := bindProvider(t, options, 1)
				receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("factory"), providerRequest(t, "GET", endpoint, nil))
				result := settleProvider(t, fixture, receipt)
				if !errors.Is(err, cause) || !errors.Is(result.Err(), cause) || reached.Load() != 0 || calls.Load() != failAt {
					t.Fatal("explicit factory refusal was replaced or sent", err, result.Err(), reached.Load(), calls.Load())
				}
			})
		}
	}
}

func TestProviderNativeSpecialProfilesRemainUsable(t *testing.T) {
	for _, id := range []tls.ClientHelloID{tls.HelloGolang, tls.HelloRandomized, tls.HelloRandomizedALPN, tls.HelloRandomizedNoALPN, tls.HelloChrome_120} {
		t.Run(id.Client+"-"+id.Version, func(t *testing.T) {
			id.Seed = new(tls.PRNGSeed)
			endpoint, options := providerPeer(t, Negotiated, func(writer http.ResponseWriter, request *http.Request) {
				_, _ = io.WriteString(writer, "native-profile")
			})
			profile := profileWithID(*options.Native.Profile, id)
			options.Native.Profile = &profile
			fixture := bindProvider(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("native-profile"), providerRequest(t, "GET", endpoint, nil))
			result := settleProvider(t, fixture, receipt)
			if err != nil {
				t.Fatal("native built-in profile refused", err)
			}
			if string(result.Outcome.Value.DataCopy()) != "native-profile" || !result.Outcome.Value.Complete() {
				t.Fatal("native profile response changed")
			}
		})
	}
}

func TestProviderSpecialModesRefuseIgnoredExplicitFactories(t *testing.T) {
	for _, id := range []tls.ClientHelloID{tls.HelloGolang, tls.HelloRandomized, tls.HelloRandomizedALPN, tls.HelloRandomizedNoALPN, tls.HelloCustom} {
		t.Run(id.Client, func(t *testing.T) {
			var calls atomic.Int64
			options := providerOptions()
			id.SpecFactory = func() (tls.ClientHelloSpec, error) {
				calls.Add(1)
				return profiles.Chrome_120.GetClientHelloSpec()
			}
			profile := profileWithID(*options.Native.Profile, id)
			options.Native.Profile = &profile
			if _, err := Select(options); !errors.Is(err, ErrUnsupported) || calls.Load() != 0 {
				t.Fatal("ignored custom factory accepted", err, calls.Load())
			}
		})
	}
}

func TestProviderNativeRandomALPNCannotBypassHTTP1Only(t *testing.T) {
	for _, id := range []tls.ClientHelloID{tls.HelloRandomized, tls.HelloRandomizedALPN} {
		options := providerOptions()
		options.Mode = HTTP1Only
		profile := profileWithID(*options.Native.Profile, id)
		options.Native.Profile = &profile
		if _, err := Select(options); !errors.Is(err, ErrUnsupported) {
			t.Fatal("native randomized mode silently ignores forced HTTP/1", err)
		}
	}
	for _, id := range []tls.ClientHelloID{tls.HelloGolang, tls.HelloRandomizedNoALPN, tls.HelloChrome_120} {
		t.Run(id.Client, func(t *testing.T) {
			id.Seed = new(tls.PRNGSeed)
			endpoint, options := providerPeer(t, Negotiated, func(writer http.ResponseWriter, request *http.Request) {
				_, _ = io.WriteString(writer, request.Proto)
			})
			profile := profileWithID(*options.Native.Profile, id)
			options.Native.Profile = &profile
			options.Mode = HTTP1Only
			fixture := bindProvider(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("native-http1"), providerRequest(t, "GET", endpoint, nil))
			result := settleProvider(t, fixture, receipt)
			if err != nil {
				t.Fatal("compatible native HTTP/1 profile rejected", err)
			}
			if result.Outcome.Value.Metadata().Protocol() != "HTTP/1.1" || string(result.Outcome.Value.DataCopy()) != "HTTP/1.1" {
				t.Fatal("HTTP1Only negotiated another protocol")
			}
		})
	}
}
