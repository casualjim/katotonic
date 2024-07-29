package katotonic

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type Dialer func(addr string, tlsConfig *tls.Config, connectTimeout time.Duration) (net.Conn, error)

func dial(addr string, tlsConfig *tls.Config, connectTimeout time.Duration) (net.Conn, error) {
	if tlsConfig != nil {
		return tls.DialWithDialer(&net.Dialer{
			Timeout: connectTimeout,
		}, "tcp", addr, tlsConfig)
	} else {
		return net.DialTimeout("tcp", addr, connectTimeout)
	}
}

// connectionPool represents a pool of TCP connections.
type connectionPool struct {
	mu             sync.Mutex
	conns          chan net.Conn
	dialer         Dialer
	maxConn        int
	addr           string
	tlsConfig      *tls.Config
	connectTimeout time.Duration
	sem            *semaphore.Weighted
}

// newConnectionPool initializes a new connection pool.
func newConnectionPool(addr string, maxConn int, tlsConfig *tls.Config, connectTimeout time.Duration, dialer Dialer) (*connectionPool, error) {
	conns := make(chan net.Conn, maxConn)
	for i := 0; i < maxConn; i++ {
		conn, err := dial(addr, tlsConfig.Clone(), connectTimeout)
		if err != nil {
			close(conns)
			for conn := range conns {
				conn.Close()
			}
			return nil, err
		}
		conns <- conn
	}
	return &connectionPool{
		addr:           addr,
		tlsConfig:      tlsConfig,
		connectTimeout: connectTimeout,
		maxConn:        maxConn,
		conns:          conns,
		dialer:         dialer,
		sem:            semaphore.NewWeighted(int64(maxConn)),
	}, nil
}

// Get retrieves a connection from the pool or creates a new one if the pool is empty.
func (p *connectionPool) Get() (net.Conn, error) {
	slog.Debug("Acquiring connection")
	defer slog.Debug("Connection acquired")
	if err := p.sem.Acquire(context.Background(), 1); err != nil {
		return nil, err
	}
	return <-p.conns, nil
}

// Put returns a connection to the pool.
func (p *connectionPool) Put(conn net.Conn, err error) {
	slog.Debug("Releasing connection")
	defer slog.Debug("Connection released")
	defer p.sem.Release(1)

	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return
	}

	select {
	case p.conns <- conn:
	default:
		conn.Close() // Close the connection if the pool is full.
	}
}

// SwitchAddr updates the pool with a new address and reconnects active connections.
func (p *connectionPool) SwitchAddr(newAddr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.addr == newAddr {
		return nil
	}
	slog.Info("Switching address", slog.String("old", p.addr), slog.String("new", newAddr))

	if err := p.sem.Acquire(context.Background(), int64(p.maxConn)); err != nil {
		return err
	}
	defer p.sem.Release(int64(p.maxConn))
	slog.Debug("All permits acquired")

	oldConns := make([]net.Conn, 0, p.maxConn)
	for i := 0; i < p.maxConn; i++ {
		conn := <-p.conns
		oldConns = append(oldConns, conn)
	}
	slog.Debug("All connections acquired")

	p.addr = newAddr
	for _, oldConn := range oldConns {
		oldConn.Close()
		newConn, err := p.dialer(p.addr, p.tlsConfig.Clone(), p.connectTimeout)
		if err != nil {
			close(p.conns)
			for conn := range p.conns {
				conn.Close()
			}
			return err
		}
		p.conns <- newConn
	}
	slog.Debug("All connections re-established", slog.String("newAddr", p.addr))

	return nil
}

func (p *connectionPool) Addr() string {
	return p.addr
}

func (p *connectionPool) TLSConfig() *tls.Config {
	return p.tlsConfig.Clone()
}

func (p *connectionPool) Connect() (net.Conn, error) {
	return dial(p.addr, p.tlsConfig.Clone(), p.connectTimeout)
}

// Close closes all connections in the pool.
func (p *connectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
}
