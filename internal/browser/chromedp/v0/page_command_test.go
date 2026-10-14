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
	"math"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
)

func TestPageCommandCatalogueDoesNotGrantIdentityOrOwnerAuthority(t *testing.T) {
	for method := range finitePageCommands {
		schema, exists := protocolCommands[method]
		if !exists || schema.params == "" || schema.result == "" {
			t.Fatalf("missing pinned command schema: %s", method)
		}
	}
	for _, method := range []string{
		"Browser.close", "Browser.setPermission", "Target.createTarget",
		"Emulation.addScreen", "Emulation.setCPUThrottlingRate",
		"Emulation.setLocaleOverride", "Emulation.setTimezoneOverride",
		"Emulation.setVirtualTimePolicy", "DOM.describeNode",
		"DOM.setFileInputFiles", "Runtime.callFunctionOn", "IO.read",
		"Page.printToPDF", "Page.startScreencast", "Page.handleJavaScriptDialog",
		"Input.dispatchDragEvent", "Input.setInterceptDrags", "Future.command",
	} {
		t.Run(method, func(t *testing.T) {
			session := unitSession(t)
			_, err := RunAction(context.Background(), session, PageCommand(cdp.Command[cdp.Empty, cdp.Empty]{Method: method}, cdp.Empty{}))
			if !errors.Is(err, ErrUnsupported) || session.data.commands != 0 {
				t.Fatal("ungranted command reached native work", err)
			}
		})
	}
}

func TestPageCommandRejectsForgedTypesBeforeEncoding(t *testing.T) {
	session := unitSession(t)
	_, err := copyPageCommand(session, cdp.Command[cdp.Empty, page.CaptureScreenshotResult]{Method: page.CommandCaptureScreenshot}, cdp.Empty{})
	if !errors.Is(err, ErrInput) {
		t.Fatal("wrong native parameter type was accepted", err)
	}
	_, err = copyPageCommand(session, cdp.Command[page.CaptureScreenshotParams, map[string]any]{Method: page.CommandCaptureScreenshot}, page.CaptureScreenshotParams{})
	if !errors.Is(err, ErrInput) {
		t.Fatal("wrong native result type was accepted", err)
	}
	var missing *page.CaptureScreenshotParams
	_, err = copyPageCommand(session, cdp.Command[*page.CaptureScreenshotParams, page.CaptureScreenshotResult]{Method: page.CommandCaptureScreenshot}, missing)
	if !errors.Is(err, ErrInput) || session.data.commands != 0 {
		t.Fatal("typed nil or forged types acquired native command allowance", err)
	}
}

func TestPageCommandCopiesNativeDataAndPreservesExplicitZero(t *testing.T) {
	session := unitSession(t)
	params := page.CaptureScreenshotParams{Quality: new(int64(0)), FromSurface: new(false), Clip: &page.Viewport{Width: 12, Height: 23, Scale: 1}}
	copied, err := copyPageCommand(session, page.CaptureScreenshot, params)
	if err != nil || copied.Quality == nil || *copied.Quality != 0 || copied.FromSurface == nil || *copied.FromSurface {
		t.Fatal("native explicit zero was lost", err)
	}
	*copied.Quality = 10
	copied.Clip.Width = 77
	if *params.Quality != 0 || params.Clip.Width != 12 || session.data.commands != 1 {
		t.Fatal("native preparation aliased caller input or miscounted admission")
	}
	styles := emulation.SetEmulatedMediaParams{Features: []*emulation.MediaFeature{{Name: "prefers-color-scheme", Value: "dark"}}}
	media, err := copyPageCommand(session, emulation.SetEmulatedMedia, styles)
	if err != nil {
		t.Fatal(err)
	}
	media.Features[0].Value = "light"
	if styles.Features[0].Value != "dark" {
		t.Fatal("nested native data retained caller aliases")
	}
}

func TestPageCommandRequestConfigurationCopiesAndAdmitsNativeValues(t *testing.T) {
	session := unitSession(t)
	headers := network.SetExtraHTTPHeadersParams{Headers: network.Headers{"X-Fixture": "original", "X-Empty": ""}}
	copied, err := copyPageCommand(session, network.SetExtraHTTPHeaders, headers)
	if err != nil || copied.Headers["X-Empty"] != "" {
		t.Fatal("page request headers are unavailable or changed", err)
	}
	copied.Headers["X-Fixture"] = "changed"
	if headers.Headers["X-Fixture"] != "original" {
		t.Fatal("request header copy aliases its caller")
	}
	params := emulation.SetUserAgentOverrideParams{UserAgent: "LocalFixture/1", AcceptLanguage: "en-US", Platform: "Linux", UserAgentMetadata: &emulation.UserAgentMetadata{Brands: []*emulation.UserAgentBrandVersion{{Brand: "LocalFixture", Version: "1"}}, Mobile: false}}
	agent, err := copyPageCommand(session, emulation.SetUserAgentOverride, params)
	if err != nil || agent.UserAgentMetadata == nil || agent.UserAgentMetadata.Mobile || agent.AcceptLanguage != "en-US" {
		t.Fatal("native user-agent options changed", err)
	}
	agent.UserAgentMetadata.Brands[0].Brand = "changed"
	if params.UserAgentMetadata.Brands[0].Brand != "LocalFixture" {
		t.Fatal("user-agent metadata aliases its caller")
	}
	session.owner.settings.MaxCommands = int(session.data.commands)
	if _, err := copyPageCommand(session, network.SetExtraHTTPHeaders, network.SetExtraHTTPHeadersParams{Headers: network.Headers{"X-Invalid": math.NaN()}}); !errors.Is(err, ErrLimit) {
		t.Fatal("header encoding preceded command admission", err)
	}
}

func TestPageCommandAdmissionPrecedesNativeEncoding(t *testing.T) {
	session := unitSession(t)
	session.owner.settings.MaxCommands = 1
	session.data.commands = 1
	_, err := copyPageCommand(session, input.DispatchMouseEvent, input.DispatchMouseEventParams{X: math.NaN()})
	if !errors.Is(err, ErrLimit) {
		t.Fatal("invalid native encoding ran before exhausted admission", err)
	}
	session.data.commands = 0
	if _, err := copyPageCommand(session, input.DispatchMouseEvent, input.DispatchMouseEventParams{X: math.NaN()}); err == nil {
		t.Fatal("malformed native number was accepted")
	}
	session = unitSession(t)
	session.owner.settings.MaxCommandBytes = 16
	if _, err := copyPageCommand(session, input.InsertText, input.InsertTextParams{Text: "an input larger than the admitted native buffer"}); !errors.Is(err, ErrLimit) {
		t.Fatal("native copying ignored the input bound", err)
	}
}

func TestPageCommandDisplayFeaturesNeedsNonemptyDataOrExplicitClear(t *testing.T) {
	for _, features := range [][]*emulation.DisplayFeature{nil, {}, {nil}} {
		if _, err := copyPageCommand(unitSession(t), emulation.SetDisplayFeaturesOverride, emulation.SetDisplayFeaturesOverrideParams{Features: features}); !errors.Is(err, ErrInput) {
			t.Fatal("invalid display features reached the native command", err)
		}
	}
	if _, err := copyPageCommand(unitSession(t), emulation.SetDisplayFeaturesOverride, emulation.SetDisplayFeaturesOverrideParams{Features: []*emulation.DisplayFeature{{Orientation: emulation.DisplayFeatureOrientationVertical, Offset: 100}}}); err != nil {
		t.Fatal("native positive display feature refused", err)
	}
	if _, err := copyPageCommand(unitSession(t), emulation.ClearDisplayFeaturesOverride, cdp.Empty{}); err != nil {
		t.Fatal("native explicit clear refused", err)
	}
}
