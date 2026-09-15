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

package tls_client

import (
	"errors"
	"reflect"

	tls "github.com/bogdanfinn/utls"
)

// FathomryNativeSpecFactory identifies uTLS's designated built-in fallback factory.
// An explicitly supplied factory's failures must not select an unrelated preset.
func FathomryNativeSpecFactory(factory tls.ClientHelloSpecFactory) bool {
	return factory != nil && reflect.ValueOf(factory).Pointer() == reflect.ValueOf(tls.EmptyClientHelloSpecFactory).Pointer()
}

func validateClientHelloFactory(id tls.ClientHelloID) error {
	if id.SpecFactory == nil {
		return errors.New("tls-client: missing ClientHello factory")
	}
	if FathomryNativeSpecFactory(id.SpecFactory) {
		return nil
	}
	switch id.Client {
	case tls.HelloGolang.Client, tls.HelloRandomized.Client, tls.HelloRandomizedALPN.Client, tls.HelloRandomizedNoALPN.Client, tls.HelloCustom.Client:
		return errors.New("tls-client: native ClientHello mode bypasses the supplied factory")
	}
	return nil
}

func handshakeClientHelloID(id tls.ClientHelloID) (tls.ClientHelloID, error) {
	if err := validateClientHelloFactory(id); err != nil {
		return tls.ClientHelloID{}, err
	}
	if FathomryNativeSpecFactory(id.SpecFactory) {
		return id, nil
	}
	spec, err := id.ToSpec()
	if err != nil {
		return tls.ClientHelloID{}, err
	}
	// The fresh spec belongs to one handshake; uTLS keeps native ALPN filtering
	// and extension ordering, but cannot replace an already-resolved refusal.
	id.SpecFactory = func() (tls.ClientHelloSpec, error) { return spec, nil }
	return id, nil
}
