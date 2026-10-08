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

// ErrFathomryProfileLimit reports output beyond the selected owned installation bound.
var ErrFathomryProfileLimit = errors.New("tlsclient: ClientHello profile exceeds declared bound")

// ValidateFathomryProfile bounds owned native installation, not a trusted
// factory's allocations before returning its fresh spec.
func ValidateFathomryProfile(spec *tls.ClientHelloSpec, maximum int64) error {
	if spec == nil {
		return ErrInvalidProfile
	}
	if maximum < 1 || len(spec.Extensions) > 256 || len(spec.CipherSuites) > 512 || len(spec.CompressionMethods) > 256 {
		return ErrFathomryProfileLimit
	}
	remaining := maximum - int64(512+2*len(spec.CipherSuites)+len(spec.CompressionMethods))
	for _, extension := range spec.Extensions {
		if extension == nil {
			return ErrInvalidProfile
		}
		length := extension.Len()
		if length < 0 || int64(length) > remaining {
			return ErrFathomryProfileLimit
		}
		remaining -= int64(length)
	}
	if remaining < 0 {
		return ErrFathomryProfileLimit
	}
	return nil
}

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
	return resolveUDPWith(ctx, network, address, nil)
}

// SetFathomryResolver freezes selected DNS authority before the transport is used.
func (rt *RoundTripper) SetFathomryResolver(resolver *net.Resolver) { rt.fathomryResolver = resolver }

func resolveUDPWith(ctx context.Context, network, address string, resolver *net.Resolver) (*net.UDPAddr, error) {
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
		if resolver == nil {
			resolver = net.DefaultResolver
		}
		candidates, err = resolver.LookupIPAddr(ctx, host)
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
