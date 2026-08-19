package bulkprocessor

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// registrylistenerPGServer is a small, isolated PostgreSQL-wire fixture. It implements
// only the simple-query messages used by RegistryListenThread and
// InstanceRouter.readRegistry. Unlike the equivalence database, it supports
// LISTEN/notification messages, so the production listener path can be tested
// without changing shared schemas, triggers, or registry rows.
type registrylistenerPGServer struct {
	listener net.Listener

	mu          sync.Mutex
	conns       map[*registrylistenerPGConn]struct{}
	listenConn  *registrylistenerPGConn
	status      string
	listenReady chan struct{}
	readyOnce   sync.Once
}

type registrylistenerPGConn struct {
	conn net.Conn
	mu   sync.Mutex // serializes query responses and asynchronous notifications
}

func registrylistenerStartPGServer(t *testing.T) *registrylistenerPGServer {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("start isolated pgwire fixture: %v", err)
	}

	s := &registrylistenerPGServer{
		listener:    listener,
		conns:       make(map[*registrylistenerPGConn]struct{}),
		status:      instanceStatusActive,
		listenReady: make(chan struct{}),
	}
	go s.accept()
	t.Cleanup(s.close)
	return s
}

func (s *registrylistenerPGServer) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		c := &registrylistenerPGConn{conn: conn}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go s.serve(c)
	}
}

func (s *registrylistenerPGServer) serve(c *registrylistenerPGConn) {
	defer func() {
		_ = c.conn.Close()
		s.mu.Lock()
		delete(s.conns, c)
		if s.listenConn == c {
			s.listenConn = nil
		}
		s.mu.Unlock()
	}()

	backend := pgproto3.NewBackend(c.conn, c.conn)
	for {
		startup, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch startup.(type) {
		case *pgproto3.SSLRequest:
			if _, err := c.conn.Write([]byte("N")); err != nil {
				return
			}
			continue
		case *pgproto3.StartupMessage:
			if err := c.send(
				&pgproto3.AuthenticationOk{},
				&pgproto3.ParameterStatus{Name: "server_version", Value: "15.0"},
				&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"},
				&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
				&pgproto3.BackendKeyData{ProcessID: 7, SecretKey: 11},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			); err != nil {
				return
			}
		default:
			return
		}
		break
	}

	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		switch message := message.(type) {
		case *pgproto3.Query:
			if err := s.query(c, message.String); err != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		default:
			// The client is configured for simple protocol. An unexpected
			// message is a fixture failure and closing the socket surfaces it.
			return
		}
	}
}

func (c *registrylistenerPGConn) send(messages ...pgproto3.BackendMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var buf []byte
	for _, message := range messages {
		buf = message.Encode(buf)
	}
	_, err := c.conn.Write(buf)
	return err
}

func registrylistenerTextFields(names ...string) []pgproto3.FieldDescription {
	fields := make([]pgproto3.FieldDescription, len(names))
	for i, name := range names {
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(name),
			DataTypeOID:  25, // text
			DataTypeSize: -1,
			TypeModifier: -1,
			Format:       0,
		}
	}
	return fields
}

func (s *registrylistenerPGServer) query(c *registrylistenerPGConn, sql string) error {
	normalized := strings.ToUpper(strings.TrimSpace(sql))
	switch {
	case strings.HasPrefix(normalized, "LISTEN "):
		if err := c.send(
			&pgproto3.CommandComplete{CommandTag: []byte("LISTEN")},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		); err != nil {
			return err
		}
		s.mu.Lock()
		s.listenConn = c
		s.mu.Unlock()
		s.readyOnce.Do(func() { close(s.listenReady) })
		return nil

	case strings.Contains(normalized, "FROM RELYT_SYS.RELYT_INSTANCE_REGISTRY"):
		s.mu.Lock()
		status := s.status
		s.mu.Unlock()
		return c.send(
			&pgproto3.RowDescription{Fields: registrylistenerTextFields("instance_id", "connstr", "status")},
			&pgproto3.DataRow{Values: [][]byte{[]byte("instance-1"), []byte("unused"), []byte(status)}},
			&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		)

	default:
		// Trigger function/trigger setup consists only of Exec calls; the
		// listener needs successful completion, not result rows.
		return c.send(
			&pgproto3.CommandComplete{CommandTag: []byte("CREATE")},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		)
	}
}

func (s *registrylistenerPGServer) notifyRegistryChanged(t *testing.T, status string) {
	t.Helper()
	s.mu.Lock()
	s.status = status
	listener := s.listenConn
	s.mu.Unlock()
	if listener == nil {
		t.Fatal("registry listener was not connected")
	}
	if err := listener.send(&pgproto3.NotificationResponse{
		PID:     7,
		Channel: "relyt_instance_registry_channel",
		Payload: "registry changed",
	}); err != nil {
		t.Fatalf("send registry notification: %v", err)
	}
}

func (s *registrylistenerPGServer) close() {
	_ = s.listener.Close()
	s.mu.Lock()
	for c := range s.conns {
		_ = c.conn.Close()
	}
	s.mu.Unlock()
}

func registrylistenerPool(t *testing.T, address string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(fmt.Sprintf(
		"postgres://registrylistener@%s/registrylistener?sslmode=disable", address))
	if err != nil {
		t.Fatalf("parse isolated pgwire config: %v", err)
	}
	config.MaxConns = 1
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("create single-connection pool: %v", err)
	}
	return pool
}

// TestRegistryListenerRegistryNotificationRefreshesWithSingleConnection verifies the
// production registry listener path. A wire-level notification changes the
// fixture's registry status from active to draining; successful reconciliation
// is observed through InstanceRouter, rather than inferred from a nil error.
func TestRegistryListenFailureBackoffIsCancellationAware(t *testing.T) {
	start := time.Now()
	if !waitForContext(context.Background(), 40*time.Millisecond) {
		t.Fatal("backoff unexpectedly canceled")
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("LISTEN retry had no backoff: %v", elapsed)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start = time.Now()
	if waitForContext(ctx, 5*time.Second) {
		t.Fatal("canceled backoff reported success")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("canceled LISTEN backoff took too long: %v", elapsed)
	}
}

func TestRegistryListenerRegistryNotificationRefreshesWithSingleConnection(t *testing.T) {
	server := registrylistenerStartPGServer(t)
	address := server.listener.Addr().String()
	pool := registrylistenerPool(t, address)
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split fixture address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse fixture port: %v", err)
	}
	pgConfig := PostgreSQLConfig{
		Host:        host,
		Port:        port,
		Username:    "registrylistener",
		Database:    "registrylistener",
		Schema:      "public",
		Table:       "registrylistener_items",
		MaxPoolSize: 1,
	}
	mainClient := &PostgreSQLClient{pool: pool, config: pgConfig}

	router := &InstanceRouter{
		main:         mainClient,
		cfg:          pgConfig,
		table:        "registrylistener_items",
		clients:      map[string]*instanceEntry{"instance-1": {client: &PostgreSQLClient{}, connstr: "unused", status: instanceStatusActive}},
		routingCache: make(map[string]string),
		inFlight:     make(map[string]chan struct{}),
		lastRefresh:  time.Now().Add(-minRefreshInterval - time.Second),
	}

	ctx, cancel := context.WithCancel(context.Background())
	processor := &BulkProcessor{
		config:         Config{PostgreSQL: pgConfig},
		pgClient:       mainClient,
		instanceRouter: router,
		isSharded:      true,
		ctx:            ctx,
		cancel:         cancel,
	}

	listenerDone := make(chan struct{})
	processor.importerWg.Add(1)
	go func() {
		processor.RegistryListenThread()
		close(listenerDone)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-listenerDone:
		case <-time.After(2 * time.Second):
			t.Errorf("registry listener did not stop after cancellation")
		}
		router.Close()
		pool.Close()
	})

	select {
	case <-server.listenReady:
	case <-listenerDone:
		t.Fatal("RegistryListenThread exited before LISTEN")
	case <-time.After(2 * time.Second):
		t.Fatal("RegistryListenThread did not begin LISTEN")
	}

	if status, _ := router.instanceStatus("instance-1"); status != instanceStatusActive {
		t.Fatalf("initial router status = %q, want %q", status, instanceStatusActive)
	}
	server.notifyRegistryChanged(t, "draining")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if status, known := router.instanceStatus("instance-1"); known && status == "draining" {
			return
		}
		select {
		case <-listenerDone:
			t.Fatal("RegistryListenThread exited before reconciling notification")
		case <-time.After(10 * time.Millisecond):
		}
	}

	status, _ := router.instanceStatus("instance-1")
	t.Fatalf("registry notification was not reconciled with MaxPoolSize=1: status remains %q", status)
}
