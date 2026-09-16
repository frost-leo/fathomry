package proxy

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/url"
	"time"

	"github.com/nukilabs/http"
	"github.com/nukilabs/socks"
	tls "github.com/nukilabs/utls"
	"github.com/yosida95/uritemplate/v3"
)

type ContextDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	ListenPacket(ctx context.Context, network, addr string) (net.PacketConn, error)
	SupportHTTP3() bool
}

func New(proxyURL *url.URL, timeout time.Duration, tlsConf *tls.Config) (ContextDialer, error) {
	return NewWithDialer(proxyURL, timeout, tlsConf, Direct(nil, timeout), nil)
}

// NewWithDialer routes physical proxy sockets through one explicit owned base.
// Headers are copied and apply only to CONNECT, never to origin requests.
func NewWithDialer(proxyURL *url.URL, timeout time.Duration, tlsConf *tls.Config, base ContextDialer, headers http.Header, limits ...int64) (ContextDialer, error) {
	maxHeader := int64(10 << 20)
	if len(limits) > 1 || len(limits) == 1 && (limits[0] <= 0 || limits[0] > 1<<32-1) {
		return nil, ErrProxyHeaderLimit
	}
	if len(limits) == 1 {
		maxHeader = limits[0]
	}
	if proxyURL == nil {
		return base, nil
	}

	switch proxyURL.Scheme {
	case "":
		ip := net.ParseIP(proxyURL.Host)
		if ip == nil {
			return nil, errors.New("invalid ip address for direct connection: " + proxyURL.Host)
		}
		return Direct(ip, timeout), nil
	case "socks5", "socks5h":
		dialer, err := socks.NewDialer(proxyURL)
		if err != nil {
			return nil, err
		}
		dialer.Timeout = timeout
		dialer.ProxyDial = base.DialContext
		dialer.ProxyListenPacket = base.ListenPacket
		return dialer, nil
	case "http", "https":
		var authHeader string
		if proxyURL.User != nil {
			password, _ := proxyURL.User.Password()
			data := []byte(proxyURL.User.Username() + ":" + password)
			authHeader = "Basic " + base64.StdEncoding.EncodeToString(data)
		}
		template, err := uritemplate.New(unescapeBraces(proxyURL.String()))
		if err != nil {
			return nil, err
		}
		return &Dialer{
			maxHeader:  maxHeader,
			base:       base,
			headers:    headers.Clone(),
			proxyURL:   proxyURL,
			authHeader: authHeader,
			template:   template,
			timeout:    timeout,
			tlsConf:    tlsConf,
		}, nil
	default:
		return nil, errors.New("unsupported proxy scheme: " + proxyURL.Scheme)
	}
}
