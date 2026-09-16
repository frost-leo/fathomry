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
	"runtime"
	"strconv"

	"github.com/frost-leo/fathomry/internal/compatibility"
	sdk "github.com/nukilabs/tlsclient"
)

// Build reads the actual consuming executable, including distinct QUIC/QPACK
// namespaces and local corrections. A declared version is not test evidence.
func Build() (compatibility.Build, error) {
	return compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{
		"github.com/nukilabs/tlsclient", "github.com/nukilabs/http", "github.com/nukilabs/utls",
		"github.com/nukilabs/quic-go", "github.com/nukilabs/qpack", "github.com/nukilabs/socks",
	}})
}

// Profile reports non-secret effective settings, not inferred deployment facts.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: string(value.Mode),
		ServiceMode: compatibility.Fact{Kind: compatibility.UnknownFact}, ServiceVersion: compatibility.Fact{Kind: compatibility.UnknownFact},
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: string(value.Mode)},
		Native:   compatibility.Fact{Kind: compatibility.UnknownFact},
		Options: []compatibility.Option{
			{Name: "platform", Value: runtime.GOOS + "-" + runtime.GOARCH},
			{Name: "config-format", Value: "1"}, {Name: "native-format", Value: "1"}, {Name: "request-format", Value: "1"},
			{Name: "local-revision", Value: sdk.FathomryCompatibilityRevision},
			{Name: "max-connections", Value: strconv.Itoa(value.MaxConnections)},
			{Name: "max-routes", Value: strconv.Itoa(value.MaxRoutes)},
			{Name: "max-exchanges", Value: strconv.Itoa(value.MaxExchanges)},
			{Name: "max-replays", Value: strconv.Itoa(value.MaxReplays)},
			{Name: "response-bytes", Value: strconv.FormatInt(value.MaxResponseBytes, 10)},
			{Name: "encoded-bytes", Value: strconv.FormatInt(value.MaxEncodedBytes, 10)},
			{Name: "redirects", Value: strconv.FormatBool(value.FollowRedirects)},
			{Name: "routing-locked", Value: strconv.FormatBool(value.RoutingLocked)},
		}}
}

// Assess applies supplied reviewed evidence to this authoritative source.
func (client *Client) Assess(build compatibility.Build, requirements []compatibility.Requirement, records []compatibility.Record) (compatibility.Report, error) {
	if client == nil || client.access == nil {
		return compatibility.Report{}, failure(ErrInput, "assess")
	}
	return compatibility.Assess(build, client.access, client.Profile(), requirements, records)
}
