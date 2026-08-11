package bulkprocessor

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

const (
	instanceRoutingTableSuffix = "_relyt_instance_routing"
	instanceStatusActive       = "active"
	// sentinel row holding the default instance id; mutable unlike real
	// mappings, so it is rejected as a tenant routing_id and never cached
	defaultRoutingSentinel = "-1"
)

type instanceEntry struct {
	client  *PostgreSQLClient // nil when connect/verify failed, retried on refresh
	connstr string
	status  string
}

type registryRow struct {
	instanceID string
	connstr    string
	status     string
}

// InstanceRouter maps routing_ids to instances for instance-sharded tables.
// The main client is the control plane and is owned by the caller.
type InstanceRouter struct {
	main           *PostgreSQLClient
	cfg            PostgreSQLConfig
	sharedPassword string
	table          string
	schema         string

	clientsMutex sync.RWMutex
	clients      map[string]*instanceEntry

	cacheMutex   sync.RWMutex
	routingCache map[string]string // routing_id -> instance_id, positive entries only, permanent

	flightMutex sync.Mutex
	inFlight    map[string]chan struct{} // per-routing_id singleflight

	refreshMutex sync.Mutex // serializes registry refreshes
	lastRefresh  time.Time  // last successful refresh, guarded by refreshMutex
	closed       bool       // set by Close, guarded by refreshMutex
}

// minRefreshInterval short-circuits non-forced refreshes to bound registry load.
const minRefreshInterval = 3 * time.Second

func (r *InstanceRouter) instanceTableName() string {
	// pg_tables folds unquoted names to lowercase; match it
	return fmt.Sprintf("%s%s", strings.ToLower(r.table), instanceRoutingTableSuffix)
}

// NewInstanceRouter connects to every active instance in
// relyt_sys.relyt_instance_registry. Unreachable or mismatched instances are
// logged and retried lazily; only a registry read failure fails construction.
func NewInstanceRouter(main *PostgreSQLClient, cfg PostgreSQLConfig) (*InstanceRouter, error) {
	r := &InstanceRouter{
		main:           main,
		cfg:            cfg,
		sharedPassword: cfg.Password,
		table:          cfg.Table,
		schema:         cfg.Schema,
		clients:        make(map[string]*instanceEntry),
		routingCache:   make(map[string]string),
		inFlight:       make(map[string]chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := r.refreshRegistry(ctx, true); err != nil {
		return nil, err
	}

	return r, nil
}

// newClientFromConnStr builds a client from a registry connstr. The registry
// stores a standard postgres URL or keyword connstr WITHOUT a password; the
// shared password and pool size from cfg are appended here. cfg is kept on
// the client only for the Schema/Table strings used in SQL generation.
func newClientFromConnStr(ctx context.Context, connstr string, cfg PostgreSQLConfig) (*PostgreSQLClient, error) {
	var full string
	if strings.HasPrefix(connstr, "postgres://") || strings.HasPrefix(connstr, "postgresql://") {
		sep := "?"
		if strings.Contains(connstr, "?") {
			sep = "&"
		}
		full = fmt.Sprintf("%s%spassword=%s&pool_max_conns=%d", connstr, sep, url.QueryEscape(cfg.Password), cfg.MaxPoolSize)
	} else {
		// keyword DSN values must be single-quoted with ' and \ escaped
		escaped := strings.ReplaceAll(strings.ReplaceAll(cfg.Password, `\`, `\\`), `'`, `\'`)
		full = fmt.Sprintf("%s password='%s' pool_max_conns=%d", connstr, escaped, cfg.MaxPoolSize)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, full)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create instance connection pool")
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.Wrap(err, "failed to ping instance")
	}

	return &PostgreSQLClient{pool: pool, config: cfg}, nil
}

// createVerifiedClient connects and checks the server reports the expected instance id.
func createVerifiedClient(ctx context.Context, instanceID, connstr string, cfg PostgreSQLConfig) (*PostgreSQLClient, error) {
	client, err := newClientFromConnStr(ctx, connstr, cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var got string
	if err := client.pool.QueryRow(ctx, "SELECT relyt.instance_id()").Scan(&got); err != nil {
		client.Close()
		return nil, errors.Wrap(err, "failed to verify instance id")
	}
	if got != instanceID {
		client.Close()
		return nil, errors.Errorf("instance id mismatch: registry says %q, server reports %q", instanceID, got)
	}

	return client, nil
}

func (r *InstanceRouter) readRegistry(ctx context.Context) ([]registryRow, error) {
	rows, err := r.main.pool.Query(ctx,
		`SELECT instance_id, connstr, COALESCE(status, 'active') FROM relyt_sys.relyt_instance_registry`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read instance registry")
	}
	defer rows.Close()

	var result []registryRow
	for rows.Next() {
		var row registryRow
		if err := rows.Scan(&row.instanceID, &row.connstr, &row.status); err != nil {
			return nil, errors.Wrap(err, "failed to scan instance registry row")
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating instance registry rows")
	}
	return result, nil
}

// RefreshRegistry re-reads the registry and reconciles clients: new rows get
// clients (failure tolerated), changed connstrs get new pools (old pool closed
// asynchronously), vanished rows keep their clients, statuses are updated.
// Refreshes are serialized and skipped if one succeeded < minRefreshInterval ago.
func (r *InstanceRouter) RefreshRegistry(ctx context.Context) error {
	_, err := r.refreshRegistry(ctx, false)
	return err
}

// refreshRegistry reports whether a refresh actually ran; force bypasses the
// min-interval guard.
func (r *InstanceRouter) refreshRegistry(ctx context.Context, force bool) (bool, error) {
	r.refreshMutex.Lock()
	defer r.refreshMutex.Unlock()

	if r.closed {
		return false, errors.New("instance router is closed")
	}
	if !force && time.Since(r.lastRefresh) < minRefreshInterval {
		return false, nil
	}

	regRows, err := r.readRegistry(ctx)
	if err != nil {
		return false, err
	}

	// decide which clients to (re)build without holding the write lock;
	// draining instances get clients too: mappings are immutable, so their
	// tenants must stay served (only new-tenant registration excludes them)
	var pending []registryRow
	r.clientsMutex.RLock()
	for _, row := range regRows {
		entry, ok := r.clients[row.instanceID]
		if !ok || entry.client == nil || entry.connstr != row.connstr {
			pending = append(pending, row)
		}
	}
	r.clientsMutex.RUnlock()

	newClients := make(map[string]*PostgreSQLClient, len(pending))
	for _, row := range pending {
		client, err := createVerifiedClient(ctx, row.instanceID, row.connstr, r.cfg)
		if err != nil {
			log.Printf("InstanceRouter: failed to connect instance %s: %v", row.instanceID, err)
			continue
		}
		newClients[row.instanceID] = client
	}

	var stale []*PostgreSQLClient
	seen := make(map[string]struct{}, len(regRows))

	r.clientsMutex.Lock()
	for _, row := range regRows {
		seen[row.instanceID] = struct{}{}
		entry, ok := r.clients[row.instanceID]
		if !ok {
			entry = &instanceEntry{connstr: row.connstr}
			r.clients[row.instanceID] = entry
		}
		if newClient, ok := newClients[row.instanceID]; ok {
			if entry.client != nil {
				stale = append(stale, entry.client)
			}
			entry.client = newClient
			entry.connstr = row.connstr
		}
		entry.status = row.status
	}
	for id := range r.clients {
		if _, ok := seen[id]; !ok {
			log.Printf("InstanceRouter: instance %s vanished from registry, keeping client", id)
		}
	}
	r.clientsMutex.Unlock()

	// close asynchronously: Close waits for in-flight queries, and callers
	// (listener, BGWorker, GetClient) must never block on them
	for _, c := range stale {
		go c.Close()
	}

	r.lastRefresh = time.Now()
	return true, nil
}

// GetDefaultInstanceID reads the sentinel row of the instance routing table.
// Never cached: the sentinel is mutable, unlike real mappings.
func (r *InstanceRouter) GetDefaultInstanceID(ctx context.Context) (string, error) {
	id, found, err := r.queryRouting(ctx, defaultRoutingSentinel)
	if err != nil {
		return "", errors.Wrap(err, "failed to get default instance id")
	}
	if !found {
		return "", errors.Wrapf(ErrNoDefaultInstance, "no sentinel row ('%s') for table %s", defaultRoutingSentinel, r.table)
	}
	return id, nil
}

// checkRoutingID rejects the empty string and the sentinel as tenant
// routing_ids: the sentinel would collide with the default marker and poison
// the permanent cache.
func checkRoutingID(routingID string) error {
	if routingID == "" {
		return errors.New("routing_id must be non-empty")
	}
	if routingID == defaultRoutingSentinel {
		return errors.Wrapf(ErrReservedRoutingID, "routing_id %q", routingID)
	}
	return nil
}

func (r *InstanceRouter) cachedInstance(routingID string) (string, bool) {
	r.cacheMutex.RLock()
	id, ok := r.routingCache[routingID]
	r.cacheMutex.RUnlock()
	return id, ok
}

func (r *InstanceRouter) cacheInstance(routingID, instanceID string) {
	r.cacheMutex.Lock()
	r.routingCache[routingID] = instanceID
	r.cacheMutex.Unlock()
}

// acquireFlight returns (channel, true) for the leader; followers get the
// leader's channel to wait on.
func (r *InstanceRouter) acquireFlight(routingID string) (chan struct{}, bool) {
	r.flightMutex.Lock()
	defer r.flightMutex.Unlock()
	if ch, ok := r.inFlight[routingID]; ok {
		return ch, false
	}
	ch := make(chan struct{})
	r.inFlight[routingID] = ch
	return ch, true
}

func (r *InstanceRouter) releaseFlight(routingID string, ch chan struct{}) {
	r.flightMutex.Lock()
	delete(r.inFlight, routingID)
	r.flightMutex.Unlock()
	close(ch)
}

func (r *InstanceRouter) queryRouting(ctx context.Context, routingID string) (string, bool, error) {
	sqlStatement := fmt.Sprintf(`SELECT instance_id FROM relyt_sys.%s WHERE routing_id = $1`, r.instanceTableName())

	var id string
	err := r.main.pool.QueryRow(ctx, sqlStatement, routingID).Scan(&id)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, errors.Wrap(err, "failed to query instance routing table")
	}
	return id, true, nil
}

// LookupInstance resolves the owning instance without registering: cache hit
// returns immediately; misses hit the routing table and are not cached.
func (r *InstanceRouter) LookupInstance(ctx context.Context, routingID string) (string, bool, error) {
	if err := checkRoutingID(routingID); err != nil {
		return "", false, err
	}
	for {
		if id, ok := r.cachedInstance(routingID); ok {
			return id, true, nil
		}

		ch, leader := r.acquireFlight(routingID)
		if !leader {
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return "", false, ctx.Err()
			}
		}

		id, found, err := r.queryRouting(ctx, routingID)
		if err == nil && found {
			r.cacheInstance(routingID, id)
		}
		r.releaseFlight(routingID, ch)
		return id, found, err
	}
}

// RegisterAndLookup registers routingID on the table's current default
// instance (resolved inside the statement, race-free across SDKs) and returns
// the owning instance id. The result is cached permanently.
func (r *InstanceRouter) RegisterAndLookup(ctx context.Context, routingID string) (string, error) {
	if err := checkRoutingID(routingID); err != nil {
		return "", err
	}
	for {
		if id, ok := r.cachedInstance(routingID); ok {
			return id, nil
		}

		ch, leader := r.acquireFlight(routingID)
		if !leader {
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		id, err := r.registerAndFetch(ctx, routingID)
		if err == nil {
			r.cacheInstance(routingID, id)
		}
		r.releaseFlight(routingID, ch)
		return id, err
	}
}

func (r *InstanceRouter) registerAndFetch(ctx context.Context, routingID string) (string, error) {
	// self-join on the sentinel row: the default is resolved inside the
	// statement, race-free across SDKs and default changes; the registry join
	// inserts nothing when the sentinel's target is not an active instance
	insertSQL := fmt.Sprintf(`
	INSERT INTO relyt_sys.%s (routing_id, instance_id)
	SELECT $1, r.instance_id FROM relyt_sys.%s r
	JOIN relyt_sys.relyt_instance_registry g ON g.instance_id = r.instance_id AND COALESCE(g.status, 'active') = 'active'
	WHERE r.routing_id = $2
	ON CONFLICT (routing_id) DO NOTHING`, r.instanceTableName(), r.instanceTableName())

	if _, err := r.main.pool.Exec(ctx, insertSQL, routingID, defaultRoutingSentinel); err != nil {
		return "", errors.Wrap(err, "failed to register routing_id")
	}

	id, found, err := r.queryRouting(ctx, routingID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.Wrapf(ErrNoDefaultInstance,
			"cannot register routing_id %s for table %s: sentinel missing, or its target instance is missing or not active in the registry",
			routingID, r.table)
	}
	return id, nil
}

// clientFor returns (client, known): (nil, false) when unknown, (nil, true)
// when known but unconnected. Status never gates data-plane access: mappings
// are immutable, so tenants on a draining instance must remain served.
func (r *InstanceRouter) clientFor(instanceID string) (*PostgreSQLClient, bool) {
	// copy the client under the lock: RefreshRegistry mutates entries in place
	r.clientsMutex.RLock()
	defer r.clientsMutex.RUnlock()
	entry, ok := r.clients[instanceID]
	if !ok {
		return nil, false
	}
	return entry.client, true
}

// instanceStatus returns the registry status recorded for an instance and
// whether the instance is known.
func (r *InstanceRouter) instanceStatus(instanceID string) (string, bool) {
	r.clientsMutex.RLock()
	defer r.clientsMutex.RUnlock()
	entry, ok := r.clients[instanceID]
	if !ok {
		return "", false
	}
	return entry.status, true
}

// GetClient returns the client for an instance, refreshing the registry on an
// unknown or unconnected instance. A still-unknown instance after a real
// refresh forces one more re-read to close the race with a row committed
// while that refresh was already reading.
func (r *InstanceRouter) GetClient(ctx context.Context, instanceID string) (*PostgreSQLClient, error) {
	if client, _ := r.clientFor(instanceID); client != nil {
		return client, nil
	}

	refreshed, err := r.refreshRegistry(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to refresh instance registry")
	}

	client, known := r.clientFor(instanceID)
	if client != nil {
		return client, nil
	}
	if !refreshed || !known {
		// refresh was skipped, or ran against a snapshot missing the
		// instance: force one re-read so lazy pull stays correct
		if _, err := r.refreshRegistry(ctx, true); err != nil {
			return nil, errors.Wrap(err, "failed to refresh instance registry")
		}
		client, known = r.clientFor(instanceID)
		if client != nil {
			return client, nil
		}
	}
	if known {
		return nil, errors.Errorf("instance %q is registered but unreachable", instanceID)
	}
	return nil, errors.Errorf("unknown instance %q: not found in relyt_instance_registry", instanceID)
}

// Close closes all instance clients; the owner closes the main client.
// Holding refreshMutex means no refresh is in flight during the swap and any
// later refresh sees the closed flag, so no freshly dialed pool can leak.
func (r *InstanceRouter) Close() {
	r.refreshMutex.Lock()
	r.closed = true
	r.clientsMutex.Lock()
	entries := r.clients
	r.clients = make(map[string]*instanceEntry)
	r.clientsMutex.Unlock()
	r.refreshMutex.Unlock()

	for _, entry := range entries {
		if entry.client != nil {
			entry.client.Close()
		}
	}
}
