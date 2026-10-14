package chromedp

import (
	"errors"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"strconv"
	"sync"
)

// subscribers holds the event subscriptions of a session, indexed by the
// method of the event.
type subscribers struct {
	mu     sync.Mutex
	subs   map[string]map[*subscription]struct{}
	closed bool
	limits EventBuffer
}

// ErrEventOverflow reports an event that exceeds the selected buffer limits.
var ErrEventOverflow = errors.New("event buffer overflow")

// EventBuffer bounds each native subscription, including its blocked delivery.
// Zero limits preserve the upstream unbounded behavior. Overflow stops delivery.
// OnOverflow must not block, panic, or call the browser. It can cancel its owner.
type EventBuffer struct {
	MaxEvents  int
	MaxBytes   int64
	OnOverflow func(error)
	// Observe runs after queue admission and before delivery. It borrows params.
	// It must not block, panic, or call the browser. Its error stops this queue.
	// Tagged events include the sequence passed to Observe.
	Observe func(method string, params jsonv2.Value, sequence uint64) error
	OnError func(error)

	// filter routes bounded parameters before this queue's admission.
	filter func(method string, params jsonv2.Value) bool
}

// WithEventBuffer applies limits to browser and target subscriptions.
func WithEventBuffer(limits EventBuffer) BrowserOption {
	return func(b *Browser) { b.events.limits = limits }
}

// subscribe adds a subscription for the events with the method. It buffers
// every event from the moment that it returns. The returned func stops the
// subscription and closes the channel.
func (s *subscribers) subscribe(method string) (<-chan jsonv2.Value, func()) {
	sub := &subscription{
		out:        make(chan jsonv2.Value),
		wake:       make(chan struct{}, 1),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
		limits:     s.limits,
		routeBytes: s.limits.MaxBytes,
	}
	s.mu.Lock()
	if s.closed {
		sub.stop()
		close(sub.out)
		close(sub.stopped)
		s.mu.Unlock()
		return sub.out, func() {}
	} else {
		if s.subs == nil {
			s.subs = make(map[string]map[*subscription]struct{})
		}
		if s.subs[method] == nil {
			s.subs[method] = make(map[*subscription]struct{})
		}
		s.subs[method][sub] = struct{}{}
	}
	s.mu.Unlock()
	go sub.pump()

	return sub.out, func() {
		s.mu.Lock()
		delete(s.subs[method], sub)
		s.mu.Unlock()
		sub.stop()
		<-sub.stopped
	}
}

// subscribeMany is like subscribe for several methods, with one queue. The
// channel gets the events of all the methods in the order that they arrived.
// Each value is a JSON object with the fields method and params, because a
// reader cannot tell the method from the parameters alone.
func (s *subscribers) subscribeMany(methods ...string) (<-chan jsonv2.Value, func()) {
	return s.subscribeBounded(s.limits, methods...)
}

func (s *subscribers) subscribeBounded(limits EventBuffer, methods ...string) (<-chan jsonv2.Value, func()) {
	methods = append([]string(nil), methods...)
	if len(methods) == 0 {
		methods = []string{""}
	}
	sub := &subscription{
		out:        make(chan jsonv2.Value),
		wake:       make(chan struct{}, 1),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
		tagged:     true,
		limits:     limits,
		routeBytes: s.limits.MaxBytes,
	}
	s.mu.Lock()
	if s.closed {
		sub.stop()
		close(sub.out)
		close(sub.stopped)
		s.mu.Unlock()
		return sub.out, func() {}
	} else {
		if s.subs == nil {
			s.subs = make(map[string]map[*subscription]struct{})
		}
		for _, method := range methods {
			if s.subs[method] == nil {
				s.subs[method] = make(map[*subscription]struct{})
			}
			s.subs[method][sub] = struct{}{}
		}
	}
	s.mu.Unlock()
	go sub.pump()

	return sub.out, func() {
		s.mu.Lock()
		for _, method := range methods {
			delete(s.subs[method], sub)
		}
		s.mu.Unlock()
		sub.stop()
		<-sub.stopped
	}
}

// publish hands the raw parameters of an event to the subscriptions for the
// method. It never blocks.
func (s *subscribers) publish(method string, params jsonv2.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range []string{method, ""} {
		for sub := range s.subs[key] {
			sub.pushTagged(method, params)
		}
		if method == "" {
			break
		}
	}
}

// tagEvent wraps the parameters of an event in a JSON object with the method.
func tagEvent(method string, params jsonv2.Value) jsonv2.Value {
	b := append([]byte(`{"method":`), strconv.Quote(method)...)
	b = append(b, `,"params":`...)
	if len(params) == 0 {
		params = jsonv2.Value("null")
	}
	b = append(b, params...)
	return append(b, '}')
}

// close ends all subscriptions, and makes later subscriptions end at once. A
// subscription first delivers the events that it already holds, so that a
// reader sees every event up to the end of the session.
func (s *subscribers) close() {
	s.mu.Lock()
	s.closed = true
	joining := make(map[*subscription]struct{})
	for _, m := range s.subs {
		for sub := range m {
			if sub.limits.MaxEvents > 0 || sub.limits.MaxBytes > 0 {
				sub.stop()
				joining[sub] = struct{}{}
			} else {
				sub.finish()
			}
		}
	}
	s.subs = nil
	s.mu.Unlock()
	for sub := range joining {
		<-sub.stopped
	}
}

// subscription is one subscriber. Its queue uses the selected limits, if any.
// Publishing never blocks the handling of the browser events.
// A goroutine moves the events to the channel of the subscriber.
type subscription struct {
	out     chan jsonv2.Value
	wake    chan struct{}
	done    chan struct{}
	stopped chan struct{}

	// tagged makes publish wrap each event with its method.
	tagged bool

	mu         sync.Mutex
	queue      []jsonv2.Value
	finished   bool
	stopOnce   sync.Once
	limits     EventBuffer
	count      int
	bytes      int64
	sequence   uint64
	routeBytes int64
}

func (s *subscription) push(v jsonv2.Value) {
	s.pushTagged("", v)
}

func (s *subscription) pushTagged(method string, v jsonv2.Value) {
	s.mu.Lock()
	select {
	case <-s.done:
		s.mu.Unlock()
		return
	default:
	}
	size := int64(len(v))
	if s.tagged && method != "" {
		size = int64(len(strconv.Quote(method)) + len(v) + len(`{"method":,"params":}`))
		if len(v) == 0 {
			size += 4
		}
	}
	if s.limits.Observe != nil {
		size += 64
	}
	routingLimit := s.limits.MaxBytes
	if s.limits.filter != nil {
		routingLimit = s.routeBytes
	}
	if routingLimit > 0 && int64(len(v)) > routingLimit {
		s.stop()
		s.mu.Unlock()
		if s.limits.OnOverflow != nil {
			s.limits.OnOverflow(ErrEventOverflow)
		}
		return
	}
	if s.limits.filter != nil && !s.limits.filter(method, v) {
		s.mu.Unlock()
		return
	}
	if s.limits.MaxEvents > 0 && s.count >= s.limits.MaxEvents || s.limits.MaxBytes > 0 && size > s.limits.MaxBytes-s.bytes {
		s.stop()
		s.mu.Unlock()
		if s.limits.OnOverflow != nil {
			s.limits.OnOverflow(ErrEventOverflow)
		}
		return
	}
	s.sequence++
	if s.limits.Observe != nil {
		if err := s.limits.Observe(method, v, s.sequence); err != nil {
			s.stop()
			s.mu.Unlock()
			if s.limits.OnError != nil {
				s.limits.OnError(err)
			}
			return
		}
	}
	if s.tagged && method != "" {
		v = tagEvent(method, v)
		if s.limits.Observe != nil {
			v = append(v[:len(v)-1], `,"sequence":`...)
			v = strconv.AppendUint(v, s.sequence, 10)
			v = append(v, '}')
		}
	}
	s.count++
	s.bytes += int64(len(v))
	s.queue = append(s.queue, v)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// finish ends the subscription after it delivered the queued events.
func (s *subscription) finish() {
	s.mu.Lock()
	s.finished = true
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// stop ends the subscription at once, and drops the queued events. It is safe
// to call more than once.
func (s *subscription) stop() {
	s.stopOnce.Do(func() { close(s.done) })
}

// pump sends the queued events to the channel, until the subscription ends.
func (s *subscription) pump() {
	defer close(s.stopped)
	defer close(s.out)
	defer func() {
		s.mu.Lock()
		s.queue = nil
		s.count, s.bytes = 0, 0
		s.mu.Unlock()
	}()
	for {
		s.mu.Lock()
		queue, finished := s.queue, s.finished
		s.queue = nil
		s.mu.Unlock()

		for index, v := range queue {
			select {
			case s.out <- v:
				queue[index] = nil
				s.mu.Lock()
				s.count--
				s.bytes -= int64(len(v))
				s.mu.Unlock()
			case <-s.done:
				return
			}
		}
		if len(queue) > 0 {
			continue
		}
		if finished {
			return
		}
		select {
		case <-s.wake:
		case <-s.done:
			return
		}
	}
}
