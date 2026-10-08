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

package tlsclient

import (
	tls "github.com/nukilabs/utls"
	"sync"
)

// FathomrySessionCacheCapacity is the source-owned automatic cache ceiling.
const FathomrySessionCacheCapacity = 32

type ownedSessionCache struct {
	mu    sync.Mutex
	cache tls.ClientSessionCache
}

func newOwnedSessionCache() *ownedSessionCache {
	return &ownedSessionCache{cache: tls.NewLRUClientSessionCache(FathomrySessionCacheCapacity)}
}
func (cache *ownedSessionCache) Get(key string) (*tls.ClientSessionState, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.cache == nil {
		return nil, false
	}
	return cache.cache.Get(key)
}
func (cache *ownedSessionCache) Put(key string, state *tls.ClientSessionState) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.cache != nil {
		cache.cache.Put(key, state)
	}
}
func (cache *ownedSessionCache) close() {
	cache.mu.Lock()
	cache.cache = nil
	cache.mu.Unlock()
}
