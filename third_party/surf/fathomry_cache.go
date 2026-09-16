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

package surf

import (
	"crypto/tls"

	utls "github.com/refraction-networking/utls"
)

type fathomryTLSCache struct {
	state *fathomryState
	cache tls.ClientSessionCache
}

func (cache fathomryTLSCache) Get(key string) (value *tls.ClientSessionState, ok bool) {
	cache.state.notification(func() error { value, ok = cache.cache.Get(key); return nil })
	return value, ok
}
func (cache fathomryTLSCache) Put(key string, value *tls.ClientSessionState) {
	cache.state.notification(func() error { cache.cache.Put(key, value); return nil })
}

type fathomryUTLSCache struct {
	state *fathomryState
	cache utls.ClientSessionCache
}

func (cache fathomryUTLSCache) Get(key string) (value *utls.ClientSessionState, ok bool) {
	cache.state.notification(func() error { value, ok = cache.cache.Get(key); return nil })
	return value, ok
}
func (cache fathomryUTLSCache) Put(key string, value *utls.ClientSessionState) {
	cache.state.notification(func() error { cache.cache.Put(key, value); return nil })
}
func (state *fathomryState) tlsConfig(input *tls.Config) *tls.Config {
	if input == nil {
		return nil
	}
	config := input.Clone()
	state.guardTLS(config)
	if input.ClientSessionCache != nil {
		config.ClientSessionCache = fathomryTLSCache{state: state, cache: input.ClientSessionCache}
	}
	return config
}
