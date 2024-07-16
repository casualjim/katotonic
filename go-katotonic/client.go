package katotonic

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"os"
	"time"

	"github.com/oklog/ulid/v2"
)

type opts struct {
	addr           string
	maxConn        int
	connectTimeout time.Duration

	tlsConfig *tls.Config

	caCert     string
	cert       string
	key        string
	serverName string
}

func (o *opts) TLSConfig() (*tls.Config, error) {
	var tlsConfig *tls.Config
	if o.tlsConfig == nil {
		if o.caCert != "" {
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
			}
		}
	} else {
		tlsConfig = &tls.Config{
			ServerName: o.serverName,
		}
	}
	return tlsConfig, nil
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

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(o *opts) {
		o.connectTimeout = connectTimeout
	}
}

type Client struct {
	pool *connectionPool
}

func New(options ...Option) (*Client, error) {
	o := &opts{
		addr:           "localhost:9000",
		maxConn:        10,
		connectTimeout: 5 * time.Second,
	}

	for _, apply := range options {
		apply(o)
	}

	tlsConfig, err := o.TLSConfig()
	if err != nil {
		return nil, err
	}

	pool, err := newConnectionPool(o.addr, o.maxConn, tlsConfig, o.connectTimeout, dial)
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
	defer c.pool.Put(conn, nil)

	requestType := []byte{1}
	_, err = conn.Write(requestType)
	if err != nil {
		return ulid.ULID{}, err
	}
	var buffer [16]byte
	_, err = io.ReadFull(conn, buffer[:])
	if err != nil {
		return ulid.ULID{}, err
	}

	return ulid.ULID(buffer), nil

	// for i := 0; i < 3; i++ {
	// 	requestType := []byte{1}
	// 	_, err = conn.Write(requestType)
	// 	if err != nil {
	// 		return ulid.ULID{}, err
	// 	}

	// 	var responseType [1]byte
	// 	_, err = io.ReadFull(conn, responseType[:])
	// 	if err != nil {
	// 		return ulid.ULID{}, err
	// 	}
	// 	switch responseType[0] {
	// 	case 0:
	// 		return ulid.ULID{}, errors.New("server error")
	// 	case 1: // this is a ulid
	// 		var buffer [16]byte
	// 		_, err = io.ReadFull(conn, buffer[:])
	// 		if err != nil {
	// 			return ulid.ULID{}, err
	// 		}
	// 	case 2: // redirect to new leader
	// 		var addrLen [1]byte
	// 		_, err = io.ReadFull(conn, addrLen[:])
	// 		if err != nil {
	// 			return ulid.ULID{}, err
	// 		}

	// 		addr := make([]byte, addrLen[0])
	// 		_, err = io.ReadFull(conn, addr)
	// 		if err != nil {
	// 			return ulid.ULID{}, err
	// 		}
	// 		conn.Close()

	// 		err = c.pool.SwitchAddr(string(addr))
	// 		if err != nil {
	// 			return ulid.ULID{}, err
	// 		}
	// 		con, err := c.pool.Get()
	// 		if err != nil {
	// 			return ulid.ULID{}, err
	// 		}
	// 		conn = con
	// 		continue
	// 	}

	// requestType := []byte{1}
	// _, err = conn.Write(requestType)
	// if err != nil {
	// 	return ulid.ULID{}, err
	// }
	// var buffer [16]byte
	// _, err = io.ReadFull(conn, buffer[:])
	// if err != nil {
	// 	return ulid.ULID{}, err
	// }

	// return ulid.ULID(buffer), nil
	// }
	// return ulid.ULID{}, errors.New("too many redirects")
}
