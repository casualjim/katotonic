package katotonic

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type opts struct {
	addr      string
	maxConn   int
	tlsConfig *tls.Config

	caCert     string
	cert       string
	key        string
	serverName string
}

type Option func(*opts)

func WithAddr(addr string) Option {
	return func(o *opts) {
		o.addr = addr
	}
}

func WithMaxConn(maxConn int) Option {
	return func(o *opts) {
		o.maxConn = maxConn
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *opts) {
		o.tlsConfig = tlsConfig
	}
}

func WithCACert(caCert string) Option {
	return func(o *opts) {
		o.caCert = caCert
	}
}

func WithCert(cert string) Option {
	return func(o *opts) {
		o.cert = cert
	}
}

func WithKey(key string) Option {
	return func(o *opts) {
		o.key = key
	}
}

func WithServerName(serverName string) Option {
	return func(o *opts) {
		o.serverName = serverName
	}
}

type Client struct {
	pool *connectionPool
}

func NewClient(options ...Option) (*Client, error) {
	o := &opts{
		addr:    "localhost:9000",
		maxConn: 50,
	}

	for _, apply := range options {
		apply(o)
	}

	var tlsConfig *tls.Config
	if o.tlsConfig == nil {
	} else if o.caCert != "" {
		caCert, err := os.ReadFile(o.caCert)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to append CA certificate")
		}

		if o.key != "" || o.cert != "" {
			clientCert, err := tls.LoadX509KeyPair(o.cert, o.key)
			if err != nil {
				return nil, err
			}

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{clientCert},
				RootCAs:      caCertPool,
				ServerName:   o.serverName,
			}
		} else {
			tlsConfig = &tls.Config{
				RootCAs:    caCertPool,
				ServerName: o.serverName,
			}
		}
	}

	pool, err := newConnectionPool(o.addr, o.maxConn, tlsConfig)
	if err != nil {
		return nil, err
	}
	return &Client{pool: pool}, nil
}

func (c *Client) Close() {
	c.pool.Close()
}

func (c *Client) NextId() (ulid.ULID, error) {
	conn, err := c.pool.Get()
	if err != nil {
		return ulid.ULID{}, err
	}
	defer c.pool.Put(conn)

	_, err = conn.Write([]byte{1})
	if err != nil {
		return ulid.ULID{}, err
	}

	var buffer [16]byte
	_, err = io.ReadFull(conn, buffer[:])
	if err != nil {
		return ulid.ULID{}, err
	}

	return ulid.ULID(buffer), nil
}

// connectionPool represents a pool of TCP connections.
type connectionPool struct {
	mu    sync.Mutex
	conns chan net.Conn
}

// newConnectionPool initializes a new connection pool.
func newConnectionPool(addr string, maxConn int, tlsConfig *tls.Config) (*connectionPool, error) {
	conns := make(chan net.Conn, maxConn)
	for i := 0; i < maxConn; i++ {
		var conn net.Conn
		var err error
		if tlsConfig != nil {
			conn, err = tls.DialWithDialer(&net.Dialer{
				Timeout: 5 * time.Second,
			}, "tcp", addr, tlsConfig)
		} else {
			conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
		}

		if err != nil {
			close(conns)
			return nil, err
		}
		conns <- conn
	}
	return &connectionPool{
		conns: conns,
	}, nil
}

// Get retrieves a connection from the pool or creates a new one if the pool is empty.
func (p *connectionPool) Get() (net.Conn, error) {
	return <-p.conns, nil
}

// Put returns a connection to the pool.
func (p *connectionPool) Put(conn net.Conn) {
	select {
	case p.conns <- conn:
	default:
		conn.Close() // Close the connection if the pool is full.
	}
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
