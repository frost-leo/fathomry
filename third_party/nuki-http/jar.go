// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http

import (
	"net/url"
)

// A CookieJar manages storage and use of cookies in HTTP requests.
//
// Implementations of CookieJar must be safe for concurrent use by multiple
// goroutines.
//
// The net/http/cookiejar package provides a CookieJar implementation.
type CookieJar interface {
	// SetCookies handles the receipt of the cookies in a reply for the
	// given URL.  It may or may not choose to save the cookies, depending
	// on the jar's policy and implementation.
	SetCookies(u *url.URL, cookies []*Cookie)

	// Cookies returns the cookies to send in a request for the given URL.
	// It is up to the implementation to honor the standard cookie use
	// restrictions such as in RFC 6265.
	Cookies(u *url.URL) []*Cookie
}

// A SiteCookieJar is a CookieJar that can additionally report every cookie it
// stores for a URL's site, ignoring the standard send restrictions.
//
// It is an optional extension of CookieJar. Callers holding a CookieJar can
// type-assert to SiteCookieJar to access SiteCookies when the underlying
// implementation supports it.
type SiteCookieJar interface {
	CookieJar

	// SiteCookies returns every cookie stored for the registrable domain
	// (eTLD+1) of the given URL's host, regardless of the standard cookie
	// use restrictions such as path, domain and secure matching in
	// RFC 6265. It reports what the jar stores for the site, not what a
	// request to the URL would carry, so it may include cookies scoped to
	// other paths and other subdomains.
	SiteCookies(u *url.URL) []*Cookie
}

//
