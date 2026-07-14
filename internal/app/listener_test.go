package app

import (
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestMakeListenerPlanAcceptsNumericLoopbackAddresses(t *testing.T) {
	tests := []struct {
		name    string
		policy  listenerPolicy
		address netip.Addr
		port    uint16
	}{
		{
			name:    "IPv4",
			policy:  listenerPolicy{Type: "tcp", Host: "127.0.0.1", Port: 8080},
			address: netip.MustParseAddr("127.0.0.1"),
			port:    8080,
		},
		{
			name:    "IPv6",
			policy:  listenerPolicy{Type: "tcp", Host: "::1", Port: 65535},
			address: netip.MustParseAddr("::1"),
			port:    65535,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := makeListenerPlan(test.policy)
			if err != nil {
				t.Fatalf("makeListenerPlan() error = %v", err)
			}
			if plan.address != test.address || plan.port != test.port {
				t.Fatalf("makeListenerPlan() = {%v, %d}, want {%v, %d}", plan.address, plan.port, test.address, test.port)
			}
		})
	}
}

func TestMakeListenerPlanRejectsUnsafePoliciesBeforeListen(t *testing.T) {
	tests := []struct {
		name   string
		policy listenerPolicy
	}{
		{name: "empty", policy: listenerPolicy{}},
		{name: "type case mismatch", policy: listenerPolicy{Type: "TCP", Host: "127.0.0.1", Port: 8080}},
		{name: "type whitespace", policy: listenerPolicy{Type: "tcp ", Host: "127.0.0.1", Port: 8080}},
		{name: "unsupported type", policy: listenerPolicy{Type: "unix", Host: "127.0.0.1", Port: 8080}},
		{name: "DNS localhost", policy: listenerPolicy{Type: "tcp", Host: "localhost", Port: 8080}},
		{name: "IPv4 whitespace", policy: listenerPolicy{Type: "tcp", Host: " 127.0.0.1", Port: 8080}},
		{name: "IPv6 whitespace", policy: listenerPolicy{Type: "tcp", Host: "::1 ", Port: 8080}},
		{name: "IPv4 wildcard", policy: listenerPolicy{Type: "tcp", Host: "0.0.0.0", Port: 8080}},
		{name: "IPv6 wildcard", policy: listenerPolicy{Type: "tcp", Host: "::", Port: 8080}},
		{name: "non-loopback IPv4", policy: listenerPolicy{Type: "tcp", Host: "192.0.2.1", Port: 8080}},
		{name: "non-loopback IPv6", policy: listenerPolicy{Type: "tcp", Host: "2001:db8::1", Port: 8080}},
		{name: "zoned IPv6", policy: listenerPolicy{Type: "tcp", Host: "::1%lo", Port: 8080}},
		{name: "mapped IPv4", policy: listenerPolicy{Type: "tcp", Host: "::ffff:127.0.0.1", Port: 8080}},
		{name: "zero port", policy: listenerPolicy{Type: "tcp", Host: "127.0.0.1", Port: 0}},
		{name: "large port", policy: listenerPolicy{Type: "tcp", Host: "127.0.0.1", Port: 65536}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := makeListenerPlan(test.policy)
			if plan != (listenerPlan{}) {
				t.Fatalf("makeListenerPlan() plan = %+v, want zero", plan)
			}
			if !errors.Is(err, errListenerPolicyInvalid) {
				t.Fatalf("makeListenerPlan() error = %v, want static policy error", err)
			}
		})
	}
}

func TestListenerPlanListenPassesExactIPv4Arguments(t *testing.T) {
	opened := openTestLoopbackListener(t)
	bound := opened.Addr().(*net.TCPAddr)
	plan, err := makeListenerPlan(listenerPolicy{
		Type: "tcp",
		Host: bound.IP.String(),
		Port: uint64(bound.Port),
	})
	if err != nil {
		t.Fatalf("makeListenerPlan() error = %v", err)
	}

	var calls int
	listener, err := plan.listen(func(network string, address *net.TCPAddr) (*net.TCPListener, error) {
		calls++
		if network != "tcp4" {
			t.Errorf("network = %q, want tcp4", network)
		}
		if address == nil || !address.IP.Equal(bound.IP) || address.Port != bound.Port || address.Zone != "" {
			t.Errorf("address = %#v, want %#v", address, bound)
		}
		return opened, nil
	})
	if err != nil {
		t.Fatalf("listen() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("listen callback calls = %d, want 1", calls)
	}
	if listener == nil {
		t.Fatal("listen() listener is nil")
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close() error = %v", err)
	}
}

func TestListenerPlanListenPassesExactIPv6Arguments(t *testing.T) {
	plan, err := makeListenerPlan(listenerPolicy{Type: "tcp", Host: "::1", Port: 43210})
	if err != nil {
		t.Fatalf("makeListenerPlan() error = %v", err)
	}

	canary := errors.New("SENSITIVE CALLBACK ERROR")
	var calls int
	listener, gotErr := plan.listen(func(network string, address *net.TCPAddr) (*net.TCPListener, error) {
		calls++
		if network != "tcp6" {
			t.Errorf("network = %q, want tcp6", network)
		}
		if address == nil || !address.IP.Equal(net.ParseIP("::1")) || address.Port != 43210 || address.Zone != "" {
			t.Errorf("address = %#v, want [::1]:43210", address)
		}
		return nil, canary
	})
	if listener != nil {
		t.Fatalf("listen() listener = %v, want nil", listener)
	}
	if calls != 1 {
		t.Fatalf("listen callback calls = %d, want 1", calls)
	}
	assertStaticOpenError(t, gotErr, canary)
}

func TestInvalidListenerPlanNeverCallsListen(t *testing.T) {
	invalidPlans := []listenerPlan{
		{},
		{address: netip.MustParseAddr("192.0.2.1"), port: 8080},
		{address: netip.MustParseAddr("::ffff:127.0.0.1"), port: 8080},
		{address: netip.MustParseAddr("127.0.0.1"), port: 0},
	}

	for i, plan := range invalidPlans {
		calls := 0
		listener, err := plan.listen(func(string, *net.TCPAddr) (*net.TCPListener, error) {
			calls++
			return nil, nil
		})
		if listener != nil || !errors.Is(err, errListenerOpenFailed) {
			t.Fatalf("case %d listen() = (%v, %v), want (nil, static error)", i, listener, err)
		}
		if calls != 0 {
			t.Fatalf("case %d callback calls = %d, want 0", i, calls)
		}
	}
}

func TestListenerPlanListenRejectsNilCallback(t *testing.T) {
	plan := listenerPlan{address: netip.MustParseAddr("127.0.0.1"), port: 8080}
	listener, err := plan.listen(nil)
	if listener != nil || !errors.Is(err, errListenerOpenFailed) {
		t.Fatalf("listen(nil) = (%v, %v), want (nil, static error)", listener, err)
	}
}

func TestListenerPlanListenDoesNotLeakCallbackError(t *testing.T) {
	plan := listenerPlan{address: netip.MustParseAddr("127.0.0.1"), port: 8080}
	canary := errors.New("SENSITIVE CALLBACK ERROR")
	listener, err := plan.listen(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		return nil, canary
	})
	if listener != nil {
		t.Fatalf("listen() listener = %v, want nil", listener)
	}
	assertStaticOpenError(t, err, canary)
}

func TestListenerPlanListenClosesListenerReturnedWithError(t *testing.T) {
	plan := listenerPlan{address: netip.MustParseAddr("127.0.0.1"), port: 8080}
	opened := openTestLoopbackListener(t)
	canary := errors.New("SENSITIVE CALLBACK ERROR")

	listener, err := plan.listen(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		return opened, canary
	})
	if listener != nil {
		t.Fatalf("listen() listener = %v, want nil", listener)
	}
	assertStaticOpenError(t, err, canary)
	assertListenerClosed(t, opened)
}

func TestListenerPlanListenRejectsNilListenerWithoutError(t *testing.T) {
	plan := listenerPlan{address: netip.MustParseAddr("127.0.0.1"), port: 8080}
	listener, err := plan.listen(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		return nil, nil
	})
	if listener != nil || !errors.Is(err, errListenerOpenFailed) {
		t.Fatalf("listen() = (%v, %v), want (nil, static error)", listener, err)
	}
}

func TestListenerPlanListenRejectsUnsafeListenerAndClosesIt(t *testing.T) {
	plan := listenerPlan{address: netip.MustParseAddr("::1"), port: 8080}
	opened := openTestLoopbackListener(t)

	listener, err := plan.listen(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		return opened, nil
	})
	if listener != nil || !errors.Is(err, errListenerOpenFailed) {
		t.Fatalf("listen() = (%v, %v), want (nil, static error)", listener, err)
	}
	assertListenerClosed(t, opened)
}

func TestListenerPlanListenRejectsDifferentConfiguredAddress(t *testing.T) {
	plan := listenerPlan{address: netip.MustParseAddr("127.0.0.2"), port: 8080}
	opened := openTestLoopbackListener(t)

	listener, err := plan.listen(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		return opened, nil
	})
	if listener != nil || !errors.Is(err, errListenerOpenFailed) {
		t.Fatalf("listen() = (%v, %v), want (nil, static error)", listener, err)
	}
	assertListenerClosed(t, opened)
}

func TestListenerPlanListenRejectsUninitializedListener(t *testing.T) {
	plan := listenerPlan{address: netip.MustParseAddr("127.0.0.1"), port: 8080}
	listener, err := plan.listen(func(string, *net.TCPAddr) (*net.TCPListener, error) {
		return &net.TCPListener{}, nil
	})
	if listener != nil || !errors.Is(err, errListenerOpenFailed) {
		t.Fatalf("listen() = (%v, %v), want (nil, static error)", listener, err)
	}
}

func openTestLoopbackListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("open test loopback listener: %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})
	return listener
}

func assertListenerClosed(t *testing.T, listener *net.TCPListener) {
	t.Helper()
	if err := listener.SetDeadline(time.Now()); err == nil {
		t.Fatal("listener remains open")
	}
}

func assertStaticOpenError(t *testing.T, got, canary error) {
	t.Helper()
	if !errors.Is(got, errListenerOpenFailed) {
		t.Fatalf("listen() error = %v, want static open error", got)
	}
	if errors.Is(got, canary) || strings.Contains(got.Error(), canary.Error()) {
		t.Fatalf("listen() error leaked callback content: %v", got)
	}
}
