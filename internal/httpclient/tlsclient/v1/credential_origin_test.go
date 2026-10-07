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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	nativehttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/cookiejar"
	sdk "github.com/bogdanfinn/tls-client"
)

func TestURLUserinfoRewriteCredentialOrigins(t *testing.T) {
	for _, path := range []string{"native", "internal"} {
		for _, rewrite := range []bool{false, true} {
			name := path + "/unchanged"
			if rewrite {
				name = path + "/rewritten"
			}
			t.Run(name, func(t *testing.T) {
				type observed struct{ authorization, cookie string }
				seen := make(chan observed, 1)
				peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					seen <- observed{request.Header.Get("Authorization"), request.Header.Get("Cookie")}
					http.SetCookie(writer, &http.Cookie{Name: "response_origin", Value: "synthetic", Path: "/"})
					_, _ = io.WriteString(writer, "ok")
				}))
				defer peer.Close()
				original, _ := url.Parse(peer.URL)
				original.User = url.UserPassword("synthetic-user", "synthetic-pass")
				target := *original
				target.Host, target.User = net.JoinHostPort("localhost", original.Port()), nil
				jar, err := cookiejar.New(nil)
				if err != nil {
					t.Fatal(err)
				}
				jar.SetCookies(original, []*nativehttp.Cookie{{Name: "origin_cookie", Value: "original"}})
				jar.SetCookies(&target, []*nativehttp.Cookie{{Name: "origin_cookie", Value: "target"}})
				hook := func(request *nativehttp.Request) error {
					if rewrite {
						copy := target
						request.URL, request.Host = &copy, target.Host
					}
					return nil
				}
				options := providerOptions()
				options.Mode, options.Native.Jar = HTTP1Only, jar
				options.Native.PreHooks = []sdk.PreRequestHookFunc{hook}
				request := providerRequest(t, "GET", original.String(), nil)
				if path == "native" {
					client, err := sdk.NewHttpClient(sdk.NewNoopLogger(), sdk.WithClientProfile(*options.Native.Profile), sdk.WithForceHttp1(), sdk.WithDisableHttp3(), sdk.WithCookieJar(jar), sdk.WithPreHook(hook))
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := sdk.Close(client); err != nil {
							t.Error(err)
						}
					}()
					response, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					_, readErr := io.Copy(io.Discard, response.Body)
					if closeErr := response.Body.Close(); readErr != nil || closeErr != nil {
						t.Fatal(readErr, closeErr)
					}
				} else {
					fixture := bindProvider(t, options, 1)
					receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("credential-origin"), request)
					if err != nil {
						t.Fatal(err)
					}
					if result := settleProvider(t, fixture, receipt); !result.Outcome.Value.Complete() {
						t.Fatal("ordinary response incomplete")
					}
				}
				observation := <-seen
				responseAt := func(address *url.URL) bool {
					for _, cookie := range jar.Cookies(address) {
						if cookie.Name == "response_origin" {
							return true
						}
					}
					return false
				}
				oldResponse, newResponse := responseAt(original), responseAt(&target)
				t.Logf("authorization-present=%t outbound-cookie=%q response-cookie-original=%t response-cookie-target=%t", observation.authorization != "", observation.cookie, oldResponse, newResponse)
				if rewrite {
					if observation.authorization != "" || observation.cookie != "origin_cookie=target" || oldResponse || !newResponse {
						t.Error("effective-origin automatic credentials or response-cookie association differ")
					}
				} else if observation.authorization == "" || observation.cookie != "origin_cookie=original" || !oldResponse || newResponse {
					t.Error("unchanged-origin positive control differs")
				}
			})
		}
	}
}
