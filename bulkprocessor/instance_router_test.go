package bulkprocessor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDialRegistryRowsDoesNotSerializeDeadInstances(t *testing.T) {
	rows := []registryRow{{instanceID: "one"}, {instanceID: "two"}, {instanceID: "three"}}
	started := make(chan struct{}, len(rows))
	release := make(chan struct{})
	factory := func(ctx context.Context, instanceID, _ string, _ PostgreSQLConfig) (*PostgreSQLClient, error) {
		started <- struct{}{}
		select {
		case <-release:
			return nil, errors.New("unreachable " + instanceID)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	done := make(chan struct{})
	go func() {
		dialRegistryRows(context.Background(), rows, PostgreSQLConfig{}, factory)
		close(done)
	}()
	for range rows {
		select {
		case <-started:
		case <-time.After(250 * time.Millisecond):
			close(release)
			t.Fatal("registry dials were serialized")
		}
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent registry dials did not finish")
	}
}

// TestBufferKeyDistinct verifies main/aux/instance buffer keys never collide.
func TestBufferKeyDistinct(t *testing.T) {
	mainKey := bufferKey("", false)
	auxKey := bufferKey("", true)
	instanceKey := bufferKey("instance-1", false)

	if mainKey == auxKey {
		t.Fatalf("main key %q collides with aux key %q", mainKey, auxKey)
	}
	if mainKey == instanceKey {
		t.Fatalf("main key %q collides with instance key %q", mainKey, instanceKey)
	}
	if auxKey == instanceKey {
		t.Fatalf("aux key %q collides with instance key %q", auxKey, instanceKey)
	}
	if auxKey != "aux" {
		t.Fatalf("expected aux key to be %q, got %q", "aux", auxKey)
	}
	if instanceKey != "instance-1" {
		t.Fatalf("expected instance key to be %q, got %q", "instance-1", instanceKey)
	}

	// isAux always wins regardless of instanceID, aux buffers are never sharded
	if bufferKey("instance-1", true) != "aux" {
		t.Fatalf("expected aux key to ignore instanceID")
	}
}

// TestSentinelRoutingIDRejected verifies the default-instance sentinel is
// rejected as a tenant routing_id by both lookup and registration.
func TestSentinelRoutingIDRejected(t *testing.T) {
	r := newTestRouter()
	ctx := context.Background()

	if _, _, err := r.LookupInstance(ctx, defaultRoutingSentinel); !errors.Is(err, ErrReservedRoutingID) {
		t.Fatalf("LookupInstance: expected ErrReservedRoutingID, got %v", err)
	}
	if _, err := r.RegisterAndLookup(ctx, defaultRoutingSentinel); !errors.Is(err, ErrReservedRoutingID) {
		t.Fatalf("RegisterAndLookup: expected ErrReservedRoutingID, got %v", err)
	}
	if _, ok := r.cachedInstance(defaultRoutingSentinel); ok {
		t.Fatalf("sentinel must never enter the cache")
	}
}

// TestCheckRoutingID verifies both the empty string and the sentinel are
// rejected as tenant routing_ids while normal ids pass.
func TestCheckRoutingID(t *testing.T) {
	if err := checkRoutingID(""); err == nil {
		t.Fatalf("expected error for empty routing_id")
	}
	if err := checkRoutingID(defaultRoutingSentinel); !errors.Is(err, ErrReservedRoutingID) {
		t.Fatalf("expected ErrReservedRoutingID for %q, got %v", defaultRoutingSentinel, err)
	}
	if err := checkRoutingID("tenant-1"); err != nil {
		t.Fatalf("expected nil for valid routing_id, got %v", err)
	}
}

// TestBufferTaskFeedbackKeys verifies a fresh task carries the buffer's
// instance id and feedback keys.
func TestBufferTaskFeedbackKeys(t *testing.T) {
	bm := NewBufferManager("prefix", "process")
	buffer := bm.NewBuffer(t.TempDir(), 10, false, "instance-1")
	buffer.FeedbackKeys["key-1"] = true

	task := NewBufferTask(buffer, nil, nil, nil, 0)
	if task.InstanceID != "instance-1" {
		t.Fatalf("expected task to carry instance id, got %q", task.InstanceID)
	}
	if len(task.FeedbackKeys) != 1 || task.FeedbackKeys[0] != "key-1" {
		t.Fatalf("expected task to carry buffer feedback keys, got %v", task.FeedbackKeys)
	}
	if buffer.FeedbackKeys != nil {
		t.Fatalf("expected buffer feedback keys to be released to the task")
	}
}

// TestInstanceTableNameLowercase verifies routing SQL uses the folded
// lowercase name pg_tables stores, even for a mixed-case cfg.Table.
func TestInstanceTableNameLowercase(t *testing.T) {
	r := &InstanceRouter{table: "MyTable"}
	if got, want := r.instanceTableName(), "mytable"+instanceRoutingTableSuffix; got != want {
		t.Fatalf("instanceTableName() = %q, want %q", got, want)
	}
}

func newTestRouter() *InstanceRouter {
	return &InstanceRouter{
		clients:      make(map[string]*instanceEntry),
		routingCache: make(map[string]string),
		inFlight:     make(map[string]chan struct{}),
	}
}

// TestInstanceRouterCacheRoundTrip exercises cachedInstance/cacheInstance
// without touching any database.
func TestInstanceRouterCacheRoundTrip(t *testing.T) {
	r := newTestRouter()

	if _, ok := r.cachedInstance("tenant-1"); ok {
		t.Fatalf("expected cache miss before any write")
	}

	r.cacheInstance("tenant-1", "instance-a")

	id, ok := r.cachedInstance("tenant-1")
	if !ok {
		t.Fatalf("expected cache hit after cacheInstance")
	}
	if id != "instance-a" {
		t.Fatalf("expected cached instance %q, got %q", "instance-a", id)
	}

	if _, ok := r.cachedInstance("tenant-2"); ok {
		t.Fatalf("expected cache miss for a different routing_id")
	}
}

// TestInstanceRouterFlightLeaderElection ensures exactly one goroutine becomes
// leader for a given routing_id while others block on the leader's channel.
func TestInstanceRouterFlightLeaderElection(t *testing.T) {
	r := newTestRouter()

	const n = 50
	var leaderCount int32
	var followerCount int32

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ch, leader := r.acquireFlight("tenant-x")
			if leader {
				atomic.AddInt32(&leaderCount, 1)
				time.Sleep(20 * time.Millisecond)
				r.releaseFlight("tenant-x", ch)
				return
			}
			atomic.AddInt32(&followerCount, 1)
			<-ch // must not block forever: leader releases and closes ch
		}()
	}

	close(start)
	wg.Wait()

	if leaderCount != 1 {
		t.Fatalf("expected exactly one leader, got %d", leaderCount)
	}
	if followerCount != n-1 {
		t.Fatalf("expected %d followers, got %d", n-1, followerCount)
	}

	// flight must be cleared after release, so a fresh acquire is leader again
	if _, ok := r.inFlight["tenant-x"]; ok {
		t.Fatalf("expected inFlight entry to be cleared after release")
	}
	_, leader := r.acquireFlight("tenant-x")
	if !leader {
		t.Fatalf("expected a new acquire after release to become leader")
	}
}

// TestInstanceRouterFlightDistinctKeys verifies singleflight is scoped per
// routing_id: concurrent leaders for different keys don't block each other.
func TestInstanceRouterFlightDistinctKeys(t *testing.T) {
	r := newTestRouter()

	ch1, leader1 := r.acquireFlight("tenant-1")
	ch2, leader2 := r.acquireFlight("tenant-2")

	if !leader1 || !leader2 {
		t.Fatalf("expected both distinct keys to elect their own leader")
	}

	r.releaseFlight("tenant-1", ch1)
	r.releaseFlight("tenant-2", ch2)
}
