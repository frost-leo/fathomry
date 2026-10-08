// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package proxy

import (
	"errors"
	"net/url"
	"strings"

	"github.com/yosida95/uritemplate/v3"
)

// ValidateFathomryTemplate accepts only bounded fixed-authority HTTPS target templates.
func ValidateFathomryTemplate(address *url.URL) error {
	if address == nil || address.Scheme != "https" || address.Hostname() == "" || address.Port() == "" || !strings.HasPrefix(address.Path, "/") || address.Opaque != "" || address.Fragment != "" {
		return errors.New("invalid CONNECT-UDP template")
	}
	value := *address
	value.User = nil
	for _, field := range []string{value.Scheme, value.Host, value.Path, value.RawPath, value.RawQuery} {
		if len(field) > 8192 {
			return errors.New("CONNECT-UDP template exceeds bound")
		}
	}
	raw := unescapeBraces(value.String())
	if len(raw) > 8192 {
		return errors.New("CONNECT-UDP template exceeds bound")
	}
	for _, char := range raw {
		if char < 0x21 || char > 0x7e {
			return errors.New("CONNECT-UDP template must use ASCII URI syntax")
		}
	}
	for remaining := raw; ; {
		start := strings.IndexByte(remaining, '{')
		if start < 0 {
			break
		}
		remaining = remaining[start+1:]
		end := strings.IndexByte(remaining, '}')
		if end < 0 {
			return errors.New("incomplete CONNECT-UDP template")
		}
		expression := remaining[:end]
		if expression == "" || strings.ContainsAny(expression, "+#./;:*") {
			return errors.New("unsupported CONNECT-UDP template operator")
		}
		remaining = remaining[end+1:]
	}
	template, err := uritemplate.New(raw)
	if err != nil {
		return errors.New("invalid CONNECT-UDP template")
	}
	host, port := false, false
	for _, name := range template.Varnames() {
		switch name {
		case uriTemplateTargetHost:
			host = true
		case uriTemplateTargetPort:
			port = true
		default:
			return errors.New("unsupported CONNECT-UDP template variable")
		}
	}
	if !host || !port {
		return errors.New("CONNECT-UDP template requires target host and port")
	}
	expanded, err := template.Expand(uritemplate.Values{uriTemplateTargetHost: uritemplate.String("192.0.2.1"), uriTemplateTargetPort: uritemplate.String("443")})
	if err != nil {
		return errors.New("invalid CONNECT-UDP template expansion")
	}
	target, err := url.Parse(expanded)
	if err != nil || target.Scheme != "https" || target.Host != address.Host || target.User != nil || target.Fragment != "" {
		return errors.New("CONNECT-UDP template changes proxy authority")
	}
	return nil
}
