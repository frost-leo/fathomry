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
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/nukilabs/tlsclient/profiles"
	tls "github.com/nukilabs/utls"
)

var ErrInvalidProfile = errors.New("tlsclient: invalid ClientHello profile")

func clientHelloSpec(profile profiles.ClientProfile) (spec *tls.ClientHelloSpec, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			spec = nil
			err = ErrInvalidProfile
			if cause, ok := recovered.(error); ok {
				err = errors.Join(err, cause)
			}
		}
	}()
	if profile.ClientHelloSpec == nil {
		return nil, ErrInvalidProfile
	}
	spec = profile.ClientHelloSpec()
	if spec == nil {
		return nil, ErrInvalidProfile
	}
	return spec, nil
}

// ValidationError reports construction-time profile failure without replacing
// a failed native factory with an implicit browser profile.
func (rt *RoundTripper) ValidationError() error { return rt.validationErr }

func resolveUDP(ctx context.Context, network, address string) (*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return nil, errors.Join(errors.New("tlsclient: invalid UDP port"), err)
	}
	candidates := []net.IPAddr{}
	if literal := net.ParseIP(host); literal != nil {
		candidates = append(candidates, net.IPAddr{IP: literal})
	} else {
		candidates, err = net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
	}
	for _, candidate := range candidates {
		if network == "udp4" && candidate.IP.To4() == nil || network == "udp6" && candidate.IP.To4() != nil {
			continue
		}
		return &net.UDPAddr{IP: candidate.IP, Port: int(number), Zone: candidate.Zone}, nil
	}
	return nil, errors.New("tlsclient: no address in enabled IP family")
}
