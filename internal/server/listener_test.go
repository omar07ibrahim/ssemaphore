package server

import (
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestBoundedListenerBlocksAtCapacityUntilConnectionCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		underlying := newControlledListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080})
		firstRaw := &countingConnection{}
		secondRaw := &countingConnection{}
		underlying.acceptResults <- listenerAcceptResult{connection: firstRaw}
		underlying.acceptResults <- listenerAcceptResult{connection: secondRaw}

		listener := newBoundedListener(underlying, 1)
		first, err := listener.Accept()
		if err != nil {
			t.Fatalf("first Accept() error = %v", err)
		}
		receive(t, underlying.acceptStarted, "first underlying Accept")

		secondResult := make(chan listenerAcceptResult, 1)
		go func() {
			connection, err := listener.Accept()
			secondResult <- listenerAcceptResult{connection: connection, err: err}
		}()
		synctest.Wait()

		select {
		case <-underlying.acceptStarted:
			t.Fatal("underlying Accept called while the connection slot was occupied")
		default:
		}
		select {
		case result := <-secondResult:
			t.Fatalf("second Accept() completed at capacity: connection = %v, error = %v", result.connection, result.err)
		default:
		}

		if err := first.Close(); err != nil {
			t.Fatalf("first connection Close() error = %v", err)
		}
		result := receive(t, secondResult, "second Accept after release")
		if result.err != nil {
			t.Fatalf("second Accept() error = %v", result.err)
		}
		if result.connection == nil {
			t.Fatal("second Accept() connection is nil")
		}
		receive(t, underlying.acceptStarted, "second underlying Accept")

		if err := result.connection.Close(); err != nil {
			t.Fatalf("second connection Close() error = %v", err)
		}
		if err := listener.Close(); err != nil {
			t.Fatalf("listener Close() error = %v", err)
		}
	})
}

func TestBoundedListenerReleasesSlotAfterAcceptError(t *testing.T) {
	wantErr := errors.New("accept failed")
	underlying := newControlledListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080})
	underlying.acceptResults <- listenerAcceptResult{err: wantErr}
	listener := newBoundedListener(underlying, 1)

	connection, err := listener.Accept()
	if connection != nil {
		t.Fatalf("failed Accept() connection = %v, want nil", connection)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("failed Accept() error = %v, want %v", err, wantErr)
	}
	if got := len(listener.slots); got != 0 {
		t.Fatalf("occupied slots after Accept error = %d, want 0", got)
	}

	underlying.acceptResults <- listenerAcceptResult{connection: &countingConnection{}}
	connection, err = listener.Accept()
	if err != nil {
		t.Fatalf("Accept() after error = %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("accepted connection Close() error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener Close() error = %v", err)
	}
}

func TestBoundedConnectionConcurrentCloseReleasesSlotOnce(t *testing.T) {
	const callers = 32
	wantErr := errors.New("connection close failed")
	underlying := newControlledListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080})
	raw := &countingConnection{closeErr: wantErr}
	underlying.acceptResults <- listenerAcceptResult{connection: raw}
	listener := newBoundedListener(underlying, 1)
	connection, err := listener.Accept()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			results <- connection.Close()
		}()
	}
	ready.Wait()
	close(start)
	for range callers {
		if err := receive(t, results, "concurrent connection Close"); !errors.Is(err, wantErr) {
			t.Fatalf("connection Close() error = %v, want %v", err, wantErr)
		}
	}
	if err := connection.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("repeated connection Close() error = %v, want %v", err, wantErr)
	}
	if got := raw.closeCalls.Load(); got != 1 {
		t.Fatalf("underlying connection Close calls = %d, want 1", got)
	}
	if got := len(listener.slots); got != 0 {
		t.Fatalf("occupied slots after concurrent Close = %d, want 0", got)
	}

	underlying.acceptResults <- listenerAcceptResult{connection: &countingConnection{}}
	next, err := listener.Accept()
	if err != nil {
		t.Fatalf("Accept() after concurrent Close error = %v", err)
	}
	if err := next.Close(); err != nil {
		t.Fatalf("next connection Close() error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener Close() error = %v", err)
	}
}

func TestBoundedListenerCloseUnblocksWaitForSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		underlying := newControlledListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080})
		underlying.acceptResults <- listenerAcceptResult{connection: &countingConnection{}}
		listener := newBoundedListener(underlying, 1)
		first, err := listener.Accept()
		if err != nil {
			t.Fatalf("first Accept() error = %v", err)
		}
		receive(t, underlying.acceptStarted, "first underlying Accept")

		result := make(chan listenerAcceptResult, 1)
		go func() {
			connection, err := listener.Accept()
			result <- listenerAcceptResult{connection: connection, err: err}
		}()
		synctest.Wait()
		select {
		case <-underlying.acceptStarted:
			t.Fatal("underlying Accept called while waiting for a connection slot")
		default:
		}

		if err := listener.Close(); err != nil {
			t.Fatalf("listener Close() error = %v", err)
		}
		got := receive(t, result, "Accept waiting for slot")
		if got.connection != nil {
			t.Fatalf("unblocked Accept() connection = %v, want nil", got.connection)
		}
		if !errors.Is(got.err, net.ErrClosed) {
			t.Fatalf("unblocked Accept() error = %v, want %v", got.err, net.ErrClosed)
		}
		select {
		case <-underlying.acceptStarted:
			t.Fatal("underlying Accept was reached after listener Close")
		default:
		}
		if err := first.Close(); err != nil {
			t.Fatalf("first connection Close() error = %v", err)
		}
	})
}

func TestBoundedListenerCloseUnblocksUnderlyingAccept(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		underlying := newControlledListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080})
		listener := newBoundedListener(underlying, 1)
		result := make(chan listenerAcceptResult, 1)
		go func() {
			connection, err := listener.Accept()
			result <- listenerAcceptResult{connection: connection, err: err}
		}()

		receive(t, underlying.acceptStarted, "underlying Accept")
		synctest.Wait()
		if err := listener.Close(); err != nil {
			t.Fatalf("listener Close() error = %v", err)
		}
		got := receive(t, result, "underlying Accept after Close")
		if got.connection != nil {
			t.Fatalf("Accept() connection = %v, want nil", got.connection)
		}
		if !errors.Is(got.err, net.ErrClosed) {
			t.Fatalf("Accept() error = %v, want %v", got.err, net.ErrClosed)
		}
		if occupied := len(listener.slots); occupied != 0 {
			t.Fatalf("occupied slots after interrupted Accept = %d, want 0", occupied)
		}
	})
}

func TestBoundedListenerDiscardsConnectionReturnedAfterClose(t *testing.T) {
	raw := &countingConnection{}
	underlying := &acceptAfterCloseListener{
		address: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080},
		started: make(chan struct{}),
		release: make(chan struct{}),
		conn:    raw,
	}
	listener := newBoundedListener(underlying, 1)
	result := make(chan listenerAcceptResult, 1)
	go func() {
		connection, err := listener.Accept()
		result <- listenerAcceptResult{connection: connection, err: err}
	}()
	receive(t, underlying.started, "underlying Accept before close")

	if err := listener.Close(); err != nil {
		t.Fatalf("listener Close() error = %v", err)
	}
	close(underlying.release)
	got := receive(t, result, "connection returned after listener close")
	if got.connection != nil || !errors.Is(got.err, net.ErrClosed) {
		t.Fatalf("Accept() after concurrent Close = (%v, %v), want (nil, net.ErrClosed)", got.connection, got.err)
	}
	if calls := raw.closeCalls.Load(); calls != 1 {
		t.Fatalf("late accepted connection Close calls = %d, want 1", calls)
	}
	if occupied := len(listener.slots); occupied != 0 {
		t.Fatalf("occupied slots after discarding late connection = %d, want 0", occupied)
	}
}

func TestBoundedListenerConcurrentCloseUsesCachedResult(t *testing.T) {
	const callers = 32
	wantErr := errors.New("listener close failed")
	underlying := newControlledListener(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080})
	underlying.closeErr = wantErr
	listener := newBoundedListener(underlying, 1)

	start := make(chan struct{})
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			results <- listener.Close()
		}()
	}
	ready.Wait()
	close(start)
	for range callers {
		if err := receive(t, results, "concurrent listener Close"); !errors.Is(err, wantErr) {
			t.Fatalf("listener Close() error = %v, want %v", err, wantErr)
		}
	}
	if err := listener.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("repeated listener Close() error = %v, want %v", err, wantErr)
	}
	if got := underlying.closeCalls.Load(); got != 1 {
		t.Fatalf("underlying listener Close calls = %d, want 1", got)
	}
}

func TestValidateListener(t *testing.T) {
	tcpListener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("create loopback TCP listener: %v", err)
	}
	t.Cleanup(func() {
		if err := tcpListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close loopback TCP listener: %v", err)
		}
	})

	unixListener, err := net.ListenUnix("unix", &net.UnixAddr{
		Name: filepath.Join(t.TempDir(), "ssemaphore.sock"),
		Net:  "unix",
	})
	if err != nil {
		t.Fatalf("create Unix listener: %v", err)
	}
	t.Cleanup(func() {
		if err := unixListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close Unix listener: %v", err)
		}
	})
	unixPacketListener, err := net.ListenUnix("unixpacket", &net.UnixAddr{
		Name: filepath.Join(t.TempDir(), "ssemaphore.packet.sock"),
		Net:  "unixpacket",
	})
	if err != nil {
		t.Fatalf("create Unix packet listener: %v", err)
	}
	t.Cleanup(func() {
		if err := unixPacketListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close Unix packet listener: %v", err)
		}
	})

	var nilTCPListener *net.TCPListener
	var nilUnixListener *net.UnixListener

	tests := []struct {
		name     string
		listener net.Listener
		wantErr  bool
	}{
		{
			name:     "concrete IPv4 loopback",
			listener: tcpListener,
		},
		{
			name:     "concrete Unix stream",
			listener: unixListener,
		},
		{
			name:     "Unix packet is not a byte stream",
			listener: unixPacketListener,
			wantErr:  true,
		},
		{name: "nil listener", wantErr: true},
		{
			name:     "typed nil TCP listener",
			listener: nilTCPListener,
			wantErr:  true,
		},
		{
			name:     "typed nil Unix listener",
			listener: nilUnixListener,
			wantErr:  true,
		},
		{
			name:     "wrapped concrete TCP listener",
			listener: &listenerWrapper{Listener: tcpListener},
			wantErr:  true,
		},
		{
			name:     "spoofed loopback TCP address",
			listener: &addressListener{address: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}},
			wantErr:  true,
		},
		{
			name:     "spoofed Unix address",
			listener: &addressListener{address: &net.UnixAddr{Name: "/tmp/ssemaphore.sock", Net: "unix"}},
			wantErr:  true,
		},
		{
			name:     "nil address",
			listener: &addressListener{},
			wantErr:  true,
		},
		{
			name:     "hostname address",
			listener: &addressListener{address: staticAddress{network: "tcp", value: "localhost:8080"}},
			wantErr:  true,
		},
		{
			name:     "wildcard address",
			listener: &addressListener{address: &net.TCPAddr{IP: net.IPv4zero, Port: 8080}},
			wantErr:  true,
		},
		{
			name:     "non-loopback address",
			listener: &addressListener{address: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 8080}},
			wantErr:  true,
		},
		{
			name:     "zoned loopback address",
			listener: &addressListener{address: &net.TCPAddr{IP: net.ParseIP("::1"), Port: 8080, Zone: "lo"}},
			wantErr:  true,
		},
		{
			name:     "invalid port",
			listener: &addressListener{address: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65536}},
			wantErr:  true,
		},
		{
			name:     "unknown address type",
			listener: &addressListener{address: staticAddress{network: "memory", value: "test"}},
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateListener(test.listener)
			if test.wantErr && err == nil {
				t.Fatal("validateListener() error = nil, want error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validateListener() error = %v, want nil", err)
			}
		})
	}
}

type listenerAcceptResult struct {
	connection net.Conn
	err        error
}

type controlledListener struct {
	address       net.Addr
	acceptResults chan listenerAcceptResult
	acceptStarted chan struct{}
	closed        chan struct{}
	closeOnce     sync.Once
	closeCalls    atomic.Int64
	closeErr      error
}

type acceptAfterCloseListener struct {
	address net.Addr
	started chan struct{}
	release chan struct{}
	conn    net.Conn
}

func (l *acceptAfterCloseListener) Accept() (net.Conn, error) {
	close(l.started)
	<-l.release
	return l.conn, nil
}

func (*acceptAfterCloseListener) Close() error { return nil }
func (l *acceptAfterCloseListener) Addr() net.Addr {
	return l.address
}

func newControlledListener(address net.Addr) *controlledListener {
	return &controlledListener{
		address:       address,
		acceptResults: make(chan listenerAcceptResult, 16),
		acceptStarted: make(chan struct{}, 16),
		closed:        make(chan struct{}),
	}
}

func (l *controlledListener) Accept() (net.Conn, error) {
	l.acceptStarted <- struct{}{}
	select {
	case result := <-l.acceptResults:
		return result.connection, result.err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *controlledListener) Close() error {
	l.closeCalls.Add(1)
	l.closeOnce.Do(func() {
		close(l.closed)
	})
	return l.closeErr
}

func (l *controlledListener) Addr() net.Addr {
	return l.address
}

type countingConnection struct {
	closeCalls atomic.Int64
	closeErr   error
}

func (*countingConnection) Read([]byte) (int, error)    { return 0, net.ErrClosed }
func (*countingConnection) Write(p []byte) (int, error) { return len(p), nil }
func (c *countingConnection) Close() error              { c.closeCalls.Add(1); return c.closeErr }
func (*countingConnection) LocalAddr() net.Addr {
	return staticAddress{network: "memory", value: "local"}
}
func (*countingConnection) RemoteAddr() net.Addr {
	return staticAddress{network: "memory", value: "remote"}
}
func (*countingConnection) SetDeadline(time.Time) error      { return nil }
func (*countingConnection) SetReadDeadline(time.Time) error  { return nil }
func (*countingConnection) SetWriteDeadline(time.Time) error { return nil }

type addressListener struct {
	address net.Addr
}

func (*addressListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (*addressListener) Close() error              { return nil }
func (l *addressListener) Addr() net.Addr          { return l.address }

type listenerWrapper struct {
	net.Listener
}

type staticAddress struct {
	network string
	value   string
}

func (a staticAddress) Network() string { return a.network }
func (a staticAddress) String() string  { return a.value }

func receive[T any](t *testing.T, channel <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", operation)
		var zero T
		return zero
	}
}
