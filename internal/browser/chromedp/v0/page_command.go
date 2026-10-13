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
	"reflect"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/emulation"
	sdk "github.com/chromedp/chromedp"
)

// PageCommand delegates a finite, page-local native command with exact CDP types.
// Native parameters remain borrowed and immutable until execution returns.
// It does not grant authority from a node, frame, request, stream or object ID.
func PageCommand[P, R any](command cdp.Command[P, R], params P) Action[R] {
	return Action[R]{local: func(ctx context.Context, session *Session) (result R, err error) {
		copied, err := copyPageCommand(session, command, params)
		if err != nil {
			return result, err
		}
		return cdp.Call(session.commandWork(ctx, false), sdk.FromContext(session.native).Target, command, copied)
	}}
}

func copyPageCommand[P, R any](session *Session, command cdp.Command[P, R], params P) (copied P, err error) {
	if !finitePageCommands[command.Method] {
		return copied, failure(ErrUnsupported, "page-command")
	}
	schema := protocolCommands[command.Method]
	if !nativeCommandType(reflect.TypeFor[P](), schema.params) || !nativeCommandType(reflect.TypeFor[R](), schema.result) {
		return copied, failure(ErrInput, "page-command-types")
	}
	if err := session.reserveCommand(); err != nil {
		return copied, err
	}
	buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		return copied, err
	}
	err = jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions)
	if err == nil {
		if display, ok := any(copied).(emulation.SetDisplayFeaturesOverrideParams); ok {
			if len(display.Features) == 0 {
				return copied, failure(ErrInput, "display-features-use-clear")
			}
			for _, feature := range display.Features {
				if feature == nil {
					return copied, failure(ErrInput, "display-feature")
				}
			}
		}
	}
	return copied, err
}

func nativeCommandType(value reflect.Type, expected string) bool {
	return value.PkgPath()+"."+value.Name() == "github.com/chromedp/cdproto/"+expected
}

// Entries have no caller-selected browser identity or separately owned output.
// The larger protocol catalogue records vocabulary, not public authority.
var finitePageCommands = map[string]bool{
	"DOMSnapshot.captureSnapshot":                  true,
	"Emulation.clearDeviceMetricsOverride":         true,
	"Emulation.clearDevicePostureOverride":         true,
	"Emulation.clearDisplayFeaturesOverride":       true,
	"Emulation.resetPageScaleFactor":               true,
	"Emulation.setAutoDarkModeOverride":            true,
	"Emulation.setDefaultBackgroundColorOverride":  true,
	"Emulation.setDeviceMetricsOverride":           true,
	"Emulation.setDevicePostureOverride":           true,
	"Emulation.setDisplayFeaturesOverride":         true,
	"Emulation.setDocumentCookieDisabled":          true,
	"Emulation.setEmitTouchEventsForMouse":         true,
	"Emulation.setEmulatedMedia":                   true,
	"Emulation.setEmulatedOSTextScale":             true,
	"Emulation.setEmulatedVisionDeficiency":        true,
	"Emulation.setFocusEmulationEnabled":           true,
	"Emulation.setPageScaleFactor":                 true,
	"Emulation.setSafeAreaInsetsOverride":          true,
	"Emulation.setScriptExecutionDisabled":         true,
	"Emulation.setScrollbarsHidden":                true,
	"Emulation.setTouchEmulationEnabled":           true,
	"Emulation.setUserAgentOverride":               true,
	"Emulation.setVirtualKeyboardGeometryOverride": true,
	"Input.cancelDragging":                         true,
	"Input.dispatchKeyEvent":                       true,
	"Input.dispatchMouseEvent":                     true,
	"Input.dispatchTouchEvent":                     true,
	"Input.emulateTouchFromMouseEvent":             true,
	"Input.imeSetComposition":                      true,
	"Input.insertText":                             true,
	"Input.setIgnoreInputEvents":                   true,
	"Input.synthesizePinchGesture":                 true,
	"Input.synthesizeScrollGesture":                true,
	"Input.synthesizeTapGesture":                   true,
	"Network.setExtraHTTPHeaders":                  true,
	"Page.captureScreenshot":                       true,
	"Page.captureSnapshot":                         true,
	"Page.getLayoutMetrics":                        true,
	"Page.getNavigationHistory":                    true,
	"Page.resetNavigationHistory":                  true,
	"Performance.getMetrics":                       true,
}
