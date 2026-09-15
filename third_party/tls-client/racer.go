package tls_client

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/http2"
	"github.com/bogdanfinn/tls-client/bandwidth"
	tls "github.com/bogdanfinn/utls"
)

type protocolRacer struct {
	compat          *compatibilityState
	protocolCache   map[string]string
	protocolCacheMu sync.RWMutex

	clientSessionCache  tls.ClientSessionCache
	insecureSkipVerify  bool
	serverNameOverwrite string
	transportOptions    *TransportOptions
	settings            map[http2.SettingID]uint32
	cachedTransports    map[string]http.RoundTripper
	cachedTransportsLck *sync.Mutex
	certificatePinner   CertificatePinner
	badPinHandlerFunc   BadPinHandlerFunc
	bandwidthTracker    bandwidth.BandwidthTracker

	// dropTransport forgets the transport cached for an address so the next
	// attempt builds one from a fresh handshake. It is the round tripper's own
	// eviction, handed over because a raced request returns from RoundTrip
	// before the branch that would otherwise do it.
	dropTransport func(addr string, stale http.RoundTripper)

	// HTTP/3 specific settings
	http3Settings          map[uint64]uint64
	http3SettingsOrder     []uint64
	http3PriorityParam     uint32
	http3PseudoHeaderOrder []string
	http3SendGreaseFrames  bool

	proxyURL string
}

func newProtocolRacer(
	clientSessionCache tls.ClientSessionCache,
	insecureSkipVerify bool,
	serverNameOverwrite string,
	transportOptions *TransportOptions,
	settings map[http2.SettingID]uint32,
	cachedTransports map[string]http.RoundTripper,
	cachedTransportsLck *sync.Mutex,
	dropTransport func(addr string, stale http.RoundTripper),
	certificatePinner CertificatePinner,
	badPinHandlerFunc BadPinHandlerFunc,
	bandwidthTracker bandwidth.BandwidthTracker,
	http3Settings map[uint64]uint64,
	http3SettingsOrder []uint64,
	http3PriorityParam uint32,
	http3PseudoHeaderOrder []string,
	http3SendGreaseFrames bool,
	proxyURL string,
) *protocolRacer {
	return &protocolRacer{
		protocolCache:          make(map[string]string),
		clientSessionCache:     clientSessionCache,
		insecureSkipVerify:     insecureSkipVerify,
		serverNameOverwrite:    serverNameOverwrite,
		transportOptions:       transportOptions,
		settings:               settings,
		cachedTransports:       cachedTransports,
		cachedTransportsLck:    cachedTransportsLck,
		dropTransport:          dropTransport,
		certificatePinner:      certificatePinner,
		badPinHandlerFunc:      badPinHandlerFunc,
		bandwidthTracker:       bandwidthTracker,
		http3Settings:          http3Settings,
		http3SettingsOrder:     http3SettingsOrder,
		http3PriorityParam:     http3PriorityParam,
		http3PseudoHeaderOrder: http3PseudoHeaderOrder,
		http3SendGreaseFrames:  http3SendGreaseFrames,
		proxyURL:               proxyURL,
	}
}

// race races HTTP/3 and HTTP/2 connections and uses whichever responds first.
// Similar to Chrome's "Happy Eyeballs" approach.
func (pr *protocolRacer) race(req *http.Request, addr string, getTransportFunc func(*http.Request, string) error) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	pr.protocolCacheMu.RLock()
	protocol, found := pr.protocolCache[addr]
	pr.protocolCacheMu.RUnlock()
	if !found {
		return pr.startRace(req, addr, getTransportFunc)
	}
	transport, err := pr.getOrCreateTransport(protocol, addr, req, getTransportFunc)
	if err != nil {
		pr.handleCachedProtocolError(err, addr, req)
		return nil, err
	}
	response, err := pr.roundTrip(transport, req, addr)
	if err == nil {
		return response, nil
	}
	if response != nil && response.Body != nil {
		pr.compat.failed(response.Body.Close())
	}
	pr.clearProtocolCache(addr)
	if req.Context().Err() != nil {
		return nil, err
	}
	retry := req.Clone(req.Context())
	if req.Body != nil && req.Body != http.NoBody {
		body, replayErr := replayBody(req)
		if replayErr != nil {
			return nil, errors.Join(err, replayErr)
		}
		retry.Body = body
	}
	return pr.startRace(retry, addr, getTransportFunc)
}

func (pr *protocolRacer) getOrCreateTransport(protocol, addr string, req *http.Request, getTransportFunc func(*http.Request, string) error) (http.RoundTripper, error) {
	transportKey := pr.getTransportKey(protocol, addr)

	pr.cachedTransportsLck.Lock()
	defer pr.cachedTransportsLck.Unlock()

	if transport, exists := pr.cachedTransports[transportKey]; exists {
		return transport, nil
	}

	transport, err := pr.createTransportForProtocol(protocol, addr, req, getTransportFunc)
	if err != nil {
		return nil, err
	}

	pr.cachedTransports[transportKey] = transport
	return transport, nil
}

func (pr *protocolRacer) createTransportForProtocol(protocol, addr string, req *http.Request, getTransportFunc func(*http.Request, string) error) (http.RoundTripper, error) {
	if protocol == "h3" {
		return buildHTTP3Transport(pr.getHTTP3Config())
	}

	// For HTTP/2, use the standard transport creation
	transportKey := pr.getTransportKey(protocol, addr)
	if err := getTransportFunc(req, transportKey); err != nil {
		return nil, err
	}

	return pr.cachedTransports[transportKey], nil
}

type raceLeg struct {
	request  *http.Request
	cancel   context.CancelFunc
	protocol string
}

func replayBody(req *http.Request) (io.ReadCloser, error) {
	if req.GetBody == nil {
		return nil, ErrBodyNotReplayable
	}
	body, err := req.GetBody()
	if nilInterface(body) {
		return nil, errors.Join(ErrBodyNotReplayable, err)
	}
	if err != nil {
		return nil, errors.Join(err, body.Close())
	}
	return newCompatBody(body, nil), nil
}

func (pr *protocolRacer) startRace(req *http.Request, addr string, getTransportFunc func(*http.Request, string) error) (*http.Response, error) {
	firstCtx, firstCancel := context.WithCancel(req.Context())
	secondCtx, secondCancel := context.WithCancel(req.Context())
	first, second := req.Clone(firstCtx), req.Clone(secondCtx)
	if req.Body != nil && req.Body != http.NoBody {
		replay, err := replayBody(req)
		if err != nil {
			firstCancel()
			secondCancel()
			return nil, errors.Join(err, req.Body.Close())
		}
		first.Body = newCompatBody(req.Body, nil)
		second.Body = replay
	}
	legs := []*raceLeg{
		{request: first, cancel: firstCancel, protocol: "h3"},
		{request: second, cancel: secondCancel, protocol: "h2"},
	}
	results := make(chan racingResult, 2)
	for _, leg := range legs {
		done := pr.compat.child()
		go func() { defer done(); pr.runLeg(leg, addr, getTransportFunc, results) }()
	}
	remaining := 2
	var causes []error
	for remaining > 0 {
		select {
		case result := <-results:
			remaining--
			if result.err == nil && result.response != nil {
				pr.cacheWinningProtocol(addr, result.leg.protocol)
				for _, leg := range legs {
					if leg != result.leg {
						leg.cancel()
					}
				}
				pr.drainRace(results, remaining)
				body := result.response.Body
				if body == nil {
					body = http.NoBody
				}
				result.response.Body = newCompatBody(body, func() { pr.cleanupLeg(result.leg, nil) })
				return result.response, nil
			}
			if result.err == nil {
				result.err = errors.New("tls-client: empty racing result")
			}
			causes = append(causes, result.err)
			pr.cleanupLeg(result.leg, result.response)
		case <-req.Context().Done():
			for _, leg := range legs {
				leg.cancel()
			}
			pr.drainRace(results, remaining)
			return nil, errors.Join(req.Context().Err(), errors.Join(causes...))
		}
	}
	return nil, errors.Join(causes...)
}

func (pr *protocolRacer) runLeg(leg *raceLeg, addr string, getTransportFunc func(*http.Request, string) error, results chan<- racingResult) {
	result := racingResult{leg: leg}
	defer func() { results <- result }()
	ctx := leg.request.Context()
	if leg.protocol == "h2" {
		timer := time.NewTimer(300 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			result.err = ctx.Err()
			return
		}
	}
	if result.err = ctx.Err(); result.err != nil {
		return
	}
	transport, err := pr.getOrCreateTransport(leg.protocol, addr, leg.request, getTransportFunc)
	if err != nil {
		result.err = err
		return
	}
	if result.err = ctx.Err(); result.err != nil {
		return
	}
	result.response, result.err = pr.roundTrip(transport, leg.request, addr)
}

func (pr *protocolRacer) cleanupLeg(leg *raceLeg, response *http.Response) {
	leg.cancel()
	if response != nil && response.Body != nil {
		pr.compat.failed(response.Body.Close())
	}
	if leg.request.Body != nil {
		pr.compat.failed(leg.request.Body.Close())
	}
}
func (pr *protocolRacer) drainRace(results <-chan racingResult, remaining int) {
	if remaining == 0 {
		return
	}
	done := pr.compat.child()
	go func() {
		defer done()
		for range remaining {
			result := <-results
			pr.cleanupLeg(result.leg, result.response)
		}
	}()
}

// roundTrip sends the request over transport and, when the dial underneath
// reports that the server has moved to a protocol this transport cannot speak,
// forgets the transport so the next attempt builds one that fits.
//
// Clearing the protocol cache alone does not recover from that: the race it
// falls back to reaches for the same cached transport and fails on the same
// mismatch, for every request from then on.
//
// addr is the dial address rather than the transport's cache key, because only
// the transport stored under that key dials through the round tripper. The
// HTTP/3 transport brings its own dialer and never reports this.
func (pr *protocolRacer) roundTrip(transport http.RoundTripper, req *http.Request, addr string) (*http.Response, error) {
	resp, err := transport.RoundTrip(req)
	if err != nil && errors.Is(err, errProtocolChanged) && pr.dropTransport != nil {
		pr.dropTransport(addr, transport)
	}

	return resp, err
}

func (pr *protocolRacer) getTransportKey(protocol, addr string) string {
	if protocol == "h3" {
		return addr + ":h3"
	}
	return addr
}

func (pr *protocolRacer) clearProtocolCache(addr string) {
	pr.protocolCacheMu.Lock()
	delete(pr.protocolCache, addr)
	pr.protocolCacheMu.Unlock()
}

func (pr *protocolRacer) cacheWinningProtocol(addr, protocol string) {
	pr.protocolCacheMu.Lock()
	pr.protocolCache[addr] = protocol
	pr.protocolCacheMu.Unlock()
}

func (pr *protocolRacer) handleCachedProtocolError(err error, addr string, req *http.Request) {
	if errors.Is(err, ErrBadPinDetected) && pr.badPinHandlerFunc != nil {
		pr.badPinHandlerFunc(req)
	}
	pr.clearProtocolCache(addr)
}

func (pr *protocolRacer) getHTTP3Config() *http3Config {
	return &http3Config{
		compat:                 pr.compat,
		clientSessionCache:     pr.clientSessionCache,
		insecureSkipVerify:     pr.insecureSkipVerify,
		serverNameOverwrite:    pr.serverNameOverwrite,
		transportOptions:       pr.transportOptions,
		http3Settings:          pr.http3Settings,
		http3SettingsOrder:     pr.http3SettingsOrder,
		http3PriorityParam:     pr.http3PriorityParam,
		http3PseudoHeaderOrder: pr.http3PseudoHeaderOrder,
		http3SendGreaseFrames:  pr.http3SendGreaseFrames,
		proxyURL:               pr.proxyURL,
	}
}

type racingResult struct {
	leg      *raceLeg
	response *http.Response
	err      error
}
