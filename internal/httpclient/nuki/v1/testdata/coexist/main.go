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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	bprofiles "github.com/bogdanfinn/tls-client/profiles"
	chrome "github.com/chromedp/chromedp"
	shttp "github.com/enetx/http"
	browser "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	"github.com/frost-leo/fathomry/internal/fault"
	cprovider "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
	netprovider "github.com/frost-leo/fathomry/internal/httpclient/nethttp/v1"
	nprovider "github.com/frost-leo/fathomry/internal/httpclient/nuki/v1"
	sprovider "github.com/frost-leo/fathomry/internal/httpclient/surf/v1"
	bprovider "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nhttp "github.com/nukilabs/http"
	nprofiles "github.com/nukilabs/tlsclient/profiles"
	chttp "github.com/sardanioss/http"
)

func main() {
	executable := flag.String("chrome", "", "explicit test browser; absent verifies browser module linkage only")
	flag.Parse()
	browserBuild, err := browser.Build()
	must(err)
	if len(browserBuild.SDKs) != 3 {
		panic("browser consuming dependencies missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "coexist") }))
	defer peer.Close()
	netOptions := netprovider.OptionsV1{Name: "net", MaxActive: 1}
	netSelection, err := netprovider.Select(netOptions)
	must(err)
	netLimits, err := netprovider.LimitsV1(netOptions)
	must(err)
	netSelection = resource.WithLimits(netSelection, netLimits)
	bprofile := bprofiles.Chrome_150
	bOptions := bprovider.OptionsV1{Name: "bogdan", Mode: bprovider.HTTP1Only, MaxActive: 1, Native: bprovider.NativeOptionsV1{Profile: &bprofile}}
	bSelection, err := bprovider.Select(bOptions)
	must(err)
	bLimits, err := bprovider.LimitsV1(bOptions)
	must(err)
	bSelection = resource.WithLimits(bSelection, bLimits)
	nprofile := nprofiles.Safari17
	nOptions := nprovider.OptionsV1{Name: "nuki", Mode: nprovider.HTTP1Only, MaxActive: 1, Native: nprovider.NativeOptionsV1{Profile: &nprofile}}
	nSelection, err := nprovider.Select(nOptions)
	must(err)
	nLimits, err := nprovider.LimitsV1(nOptions)
	must(err)
	nSelection = resource.WithLimits(nSelection, nLimits)
	cOptions := cprovider.OptionsV1{Name: "httpcloak", PresetName: "chrome-148", Protocol: cprovider.HTTP1, DisableECH: true, MaxActive: 1}
	cSelection, err := cprovider.Select(cOptions)
	must(err)
	cLimits, err := cprovider.LimitsV1(cOptions)
	must(err)
	cSelection = resource.WithLimits(cSelection, cLimits)
	sOptions := sprovider.OptionsV1{Name: "surf", Mode: sprovider.HTTP1Only, MaxActive: 1}
	sSelection, err := sprovider.Select(sOptions)
	must(err)
	sLimits, err := sprovider.LimitsV1(sOptions)
	must(err)
	sSelection = resource.WithLimits(sSelection, sLimits)
	selected := []resource.Spec{netSelection, bSelection, nSelection, cSelection, sSelection}
	var browserSelection resource.Selection[browser.Source]
	var profileParent string
	if *executable != "" {
		profileParent, err = os.MkdirTemp("", "nuki-coexist-browser-")
		must(err)
		defer func() { must(os.Remove(profileParent)) }()
		options := browser.OptionsV1{Name: "chromedp", ExecPath: *executable, TempDir: profileParent, NewWindow: true, MaxSessions: 1, MaxEvents: 2048, Flags: map[string]string{
			"headless": "", "no-first-run": "", "no-default-browser-check": "", "no-proxy-server": "",
			"disable-background-networking": "", "disable-component-update": "", "disable-sync": "", "disable-extensions": "",
		}}
		browserSelection, err = browser.Select(ctx, options)
		must(err)
		limits, err := browser.LimitsV1(options)
		must(err)
		browserSelection = resource.WithLimits(browserSelection, limits)
		selected = append(selected, browserSelection)
	}
	assembly, err := resource.Assemble(ctx, ctx, "same-consumer", selected...)
	must(err)
	defer func() { must(assembly.Close(ctx)) }()
	netInbox, err := invocation.NewInbox[netprovider.Result](2, 64<<20)
	must(err)
	netClient, err := netprovider.Bind(assembly, netSelection, netInbox, nil)
	must(err)
	bInbox, err := invocation.NewInbox[bprovider.Result](2, 64<<20)
	must(err)
	bClient, err := bprovider.Bind(assembly, bSelection, bInbox, nil)
	must(err)
	nInbox, err := invocation.NewInbox[nprovider.Result](2, 64<<20)
	must(err)
	nClient, err := nprovider.Bind(assembly, nSelection, nInbox, nil)
	must(err)
	cInbox, err := invocation.NewInbox[cprovider.Result](2, 64<<20)
	must(err)
	cClient, err := cprovider.Bind(assembly, cSelection, cInbox, nil)
	must(err)
	sInbox, err := invocation.NewInbox[sprovider.Result](2, 64<<20)
	must(err)
	sClient, err := sprovider.Bind(assembly, sSelection, sInbox, nil)
	must(err)
	netRequest, err := http.NewRequest("GET", peer.URL, nil)
	must(err)
	netReceipt, err := netClient.Do(ctx, ctx, fault.Correlation{Call: "net"}, netRequest)
	must(err)
	verify(ctx, netReceipt, netInbox)
	bRequest, err := fhttp.NewRequest("GET", peer.URL, nil)
	must(err)
	bReceipt, err := bClient.Do(ctx, ctx, fault.Correlation{Call: "bogdan"}, bRequest)
	must(err)
	verify(ctx, bReceipt, bInbox)
	nRequest, err := nhttp.NewRequest("GET", peer.URL, nil)
	must(err)
	nReceipt, err := nClient.Do(ctx, fault.Correlation{Call: "nuki"}, nRequest)
	must(err)
	verify(ctx, nReceipt, nInbox)
	cRequest, err := chttp.NewRequest("GET", peer.URL, nil)
	must(err)
	cReceipt, err := cClient.Do(ctx, ctx, fault.Correlation{Call: "httpcloak"}, cRequest)
	must(err)
	verify(ctx, cReceipt, cInbox)
	sRequest, err := shttp.NewRequest("GET", peer.URL, nil)
	must(err)
	sReceipt, err := sClient.Do(ctx, ctx, fault.Correlation{Call: "surf"}, sRequest)
	must(err)
	verify(ctx, sReceipt, sInbox)
	if *executable != "" {
		verifyBrowser(ctx, assembly, browserSelection, peer.URL)
	}
	must(assembly.Close(ctx))
	if *executable != "" {
		entries, err := os.ReadDir(profileParent)
		must(err)
		if len(entries) != 0 {
			panic("owned browser profile survived cleanup")
		}
		fmt.Println("six-provider-exact-response-and-cleanup")
	} else {
		fmt.Println("five-http-providers-and-chromedp-build-and-cleanup")
	}
}
func verifyBrowser(ctx context.Context, assembly *resource.Assembly, selected resource.Selection[browser.Source], address string) {
	inbox, err := invocation.NewInbox[browser.Result](2, 64<<20)
	must(err)
	client, err := browser.Bind(assembly, selected, inbox, nil)
	must(err)
	receipt, err := client.Run(ctx, ctx, fault.Correlation{Call: "chromedp"}, func(session *browser.Session) error {
		if err := session.Navigate(session.Context(), address); err != nil {
			return err
		}
		var text string
		if err := session.Actions(session.Context(), chrome.Evaluate("document.body.textContent", &text)); err != nil {
			return err
		}
		if text != "coexist" {
			return fmt.Errorf("browser response mismatch")
		}
		return session.Save("marker", []byte(text))
	})
	must(err)
	result, err := receipt.WaitReleased(ctx)
	must(err)
	must(result.Err())
	data, present := result.Outcome.Value.DataCopy("marker")
	if !result.Final || !result.Released || !present || string(data) != "coexist" ||
		!result.Outcome.Value.CallbackCompleted() || !result.Outcome.Value.ContextReleased() {
		panic("browser response or cleanup evidence missing")
	}
	delivery, err := inbox.Next(ctx)
	must(err)
	independent, err := delivery.Receipt().WaitReleased(ctx)
	must(err)
	must(independent.Err())
	data, present = independent.Outcome.Value.DataCopy("marker")
	if !present || string(data) != "coexist" || independent.Context != result.Context {
		panic("browser independent evidence changed")
	}
	must(delivery.Release())
}
func verify[T interface {
	DataCopy() []byte
	Complete() bool
}](ctx context.Context, receipt *invocation.Receipt[T], inbox *invocation.Inbox[T]) {
	result, err := receipt.WaitReleased(ctx)
	must(err)
	must(result.Err())
	if !result.Final || !result.Released || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "coexist" {
		panic("invalid response evidence")
	}
	delivery, err := inbox.Next(ctx)
	must(err)
	observed, err := delivery.Receipt().WaitReleased(ctx)
	must(err)
	must(observed.Err())
	if string(observed.Outcome.Value.DataCopy()) != "coexist" {
		panic("independent evidence lost")
	}
	must(delivery.Release())
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
