package app

import (
	"errors"
	"net"
	"net/netip"
)

var (
	errListenerPolicyInvalid = errors.New("listener policy is invalid")
	errListenerOpenFailed    = errors.New("listener could not be opened safely")
)

type listenerPolicy struct {
	Type string `json:"type"`
	Host string `json:"host"`
	Port uint64 `json:"port"`
}

type listenerPlan struct {
	address netip.Addr
	port    uint16
}

type listenTCPFunc func(network string, laddr *net.TCPAddr) (*net.TCPListener, error)

func makeListenerPlan(policy listenerPolicy) (listenerPlan, error) {
	if policy.Type != "tcp" || policy.Port == 0 || policy.Port > 65535 {
		return listenerPlan{}, errListenerPolicyInvalid
	}

	address, err := netip.ParseAddr(policy.Host)
	if err != nil || address.Zone() != "" || address.Is4In6() || !address.IsLoopback() {
		return listenerPlan{}, errListenerPolicyInvalid
	}

	return listenerPlan{address: address, port: uint16(policy.Port)}, nil
}

func (plan listenerPlan) listen(listenTCP listenTCPFunc) (*net.TCPListener, error) {
	if listenTCP == nil || !validListenerPlan(plan) {
		return nil, errListenerOpenFailed
	}

	network := "tcp6"
	if plan.address.Is4() {
		network = "tcp4"
	}
	requested := &net.TCPAddr{
		IP:   net.IP(plan.address.AsSlice()),
		Port: int(plan.port),
	}

	listener, err := listenTCP(network, requested)
	if err != nil {
		if listener != nil {
			closeTCPListener(listener)
		}
		return nil, errListenerOpenFailed
	}
	if listener == nil {
		return nil, errListenerOpenFailed
	}
	if !validBoundListener(listener, plan) {
		closeTCPListener(listener)
		return nil, errListenerOpenFailed
	}

	return listener, nil
}

func validListenerPlan(plan listenerPlan) bool {
	return plan.address.IsValid() && plan.address.Zone() == "" && !plan.address.Is4In6() &&
		plan.address.IsLoopback() && plan.port != 0
}

func validBoundListener(listener *net.TCPListener, plan listenerPlan) (valid bool) {
	if listener == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			valid = false
		}
	}()
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address == nil || len(address.IP) == 0 || address.Zone != "" ||
		address.Port <= 0 || address.Port > 65535 {
		return false
	}
	bound, ok := netip.AddrFromSlice(address.IP)
	if !ok || bound.Is4In6() || !bound.IsLoopback() {
		return false
	}

	return bound == plan.address && address.Port == int(plan.port)
}

func closeTCPListener(listener *net.TCPListener) {
	defer func() {
		_ = recover()
	}()
	_ = listener.Close()
}
