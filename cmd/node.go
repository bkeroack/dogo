package cmd

import (
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dollarshaveclub/go-lib/set"
	httpd "github.com/rqlite/rqlite/http"
	rqlite "github.com/rqlite/rqlite/store"
	tcp "github.com/rqlite/rqlite/tcp"
	"google.golang.org/grpc"
)

// StorePolicy represents desired behavior of a store operation
type StorePolicy int

// ConsistencyLevel represents the desired consistency of an operation
type ConsistencyLevel int

// Magic byte at the start of intra-cluster TCP connection
const (
	muxRaftHeader = 1
	muxMetaheader = 2
	muxRPCHeader  = 3
)

const waitForLeaderTimeoutSeconds = 30
const rpcTimeoutSeconds = 30

// Store operation semantics
const (
	InsertOrUpdate    StorePolicy = iota // Insert if not exists or update existing item
	InsertIfNotExists                    //Insert only if it doesn't already exist
	UpdateIfExists                       //Update only if it already exists
)

// Represents desired operation consistency
const (
	None   ConsistencyLevel = iota // No guaranteed consistency (fast: query local store only)
	Strong                         // Put operation through Raft state machine (slower but consistent)
)

//ConnectionHandler handles new incoming connections
type ConnectionHandler interface {
	NewConnection(*Node, net.Conn) //Called for each new connection (new goroutine)
}

//NodeListener is an optional network listener for a Node
type NodeListener struct {
	Type    string            //net.Dial network type ("tcp", "unix")
	Addr    string            //network bind address/path
	Handler ConnectionHandler // Handler for new connections
}

// NodeConfig controls how a Node is created
type NodeConfig struct {
	Persistent    bool   //Use persistent (disk-based) storage for data items (Raft state is always persistent)
	DataPath      string //Path for data files (if persistent) and Raft state
	RaftAddr      string //TCP address and port to listen for Raft traffic
	RaftAdvAddr   string //Address to advertise to the rest of the cluster
	JoinAddr      string //Initial cluster member to join
	Listener      *NodeListener
	RaftVerifyTLS bool //Verify TLS certificates for Raft connections
}

// Defines the RPC connection context between this node and a particular remote node
type rpcContext struct {
	conn   *grpc.ClientConn
	client DogoRPCExecuterClient
}

type rpcContextMap struct {
	sync.RWMutex
	ctxmap map[string]*rpcContext // [node] -> RPC Execution context
}

// Node represents a running cluster node
type Node struct {
	config   *NodeConfig
	store    *rqlite.Store
	rpcLayer *tcp.Layer //RPC mux listener
	rpcCtx   rpcContextMap
}

//Item represents a datastore Item
type Item struct {
	Name       string
	Value      []byte
	Size       uint64
	Flags      int64
	Expiration time.Time
	Created    time.Time
	LastUsed   time.Time
}

var reqTables = map[string]string{
	"key_value_map": `CREATE TABLE key_value_map (
		                   key TEXT PRIMARY KEY,
											 value BLOB,
											 size_bytes INTEGER,
											 flags INTEGER,
											 exptime INTEGER,
											 created INTEGER,
											 created_nano INTEGER,
											 last_used INTEGER,
											 last_used_nano INTEGER
											 );`,
}

// nowTimestamp returns unix timestamp and nanosecond remainder for the current time
func nowTimestamp() (unix int64, nanoRemainder int64) {
	return splitTimestamp(time.Now())
}

func splitTimestamp(ts time.Time) (unix int64, nanoRemainer int64) {
	unix = ts.Unix()
	nanoRemainer = ts.UnixNano() - (unix * 1000000000)
	return unix, nanoRemainer
}

// NewNode creates a new Node object but does not initialize or start it
// A node must be started with RunNode()
func NewNode(config *NodeConfig) *Node {
	n := Node{}
	n.config = config
	return &n
}

func (n *Node) listenRPC() {
	log.Printf("listening for RPC connections")
	s := grpc.NewServer()
	rpc := grpcServer{
		n: n,
	}
	RegisterDogoRPCExecuterServer(s, &rpc)
	s.Serve(n.rpcLayer)
}

//RunNode creates and starts a new cluster node according to NodeConfig
//It will initially block on finding or becoming the cluster leader. A non-nil
//return value indicates that the cluster is up and ready to accept operations.
//If a listener is configured and block is true, RunNode will block forever
//listening for new connections. If block is false, the listener will be started
//asynchronously.
func (n *Node) RunNode(block bool) error {

	// Set up TCP communication between nodes.
	ln, err := net.Listen("tcp", n.config.RaftAddr)
	if err != nil {
		log.Fatalf("failed to listen on %v: %v", n.config.RaftAddr, err)
	}
	var adv net.Addr
	if n.config.RaftAdvAddr != "" {
		adv, err = net.ResolveTCPAddr("tcp", n.config.RaftAdvAddr)
		if err != nil {
			log.Fatalf("failed to resolve advertise address %v: %v", n.config.RaftAdvAddr, err)
		}
	}
	mux := tcp.NewMux(ln, adv)
	go mux.Serve()

	// Start up mux and get transports for cluster.
	ctrans := mux.Listen(muxRaftHeader)
	n.rpcLayer = mux.Listen(muxRPCHeader)

	dataPath, err := filepath.Abs(n.config.DataPath)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data path: %s", err.Error())
	}
	dc := rqlite.NewDBConfig("cache=shared", !n.config.Persistent)
	n.store = rqlite.New(dc, dataPath, ctrans)
	if err := n.store.Open(n.config.JoinAddr == ""); err != nil {
		return fmt.Errorf("failed to open store: %v", err)
	}

	if n.config.JoinAddr != "" {
		if err := n.join(); err != nil {
			log.Fatalf("failed to join node at %v: %v", n.config.JoinAddr, err)
		}
		log.Printf("successfully joined node at %v", n.config.JoinAddr)
	}

	leader, err := n.store.WaitForLeader(30 * time.Second)
	if err != nil {
		return fmt.Errorf("error waiting for leader: %v", err)
	}
	log.Printf("cluster leader: %v", leader)

	if err := n.checkTables(); err != nil {
		log.Fatalf("error checking tables: %v", err)
	}

	go n.listenRPC()

	listen := func() error {
		l, err := net.Listen(n.config.Listener.Type, n.config.Listener.Addr)
		if err != nil {
			errstr := fmt.Sprintf("error listening: %v", err)
			log.Printf(errstr)
			return fmt.Errorf(errstr)
		}
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("error accepting connection: %v", err)
			}
			ch := n.config.Listener.Handler
			go ch.NewConnection(n, conn)
		}
	}

	if n.config.Listener != nil {
		if block {
			log.Printf("listening: %v://%v", n.config.Listener.Type, n.config.Listener.Addr)
			return listen()
		}
		log.Printf("listening asynchronously: %v://%v", n.config.Listener.Type, n.config.Listener.Addr)
		go listen()
	}
	return nil
}

func (n *Node) execute(q string, txn bool) (leader bool, exerr error) {
	res, err := n.store.Execute([]string{q}, false, txn)
	if err != nil {
		if err == rqlite.ErrNotLeader {
			return false, nil
		}
		return true, newInternalErrorError(err)
	}
	if res[0].Error != "" {
		return true, newBadQueryError(fmt.Errorf(res[0].Error))
	}
	return true, nil
}

func (n *Node) getRPCClient() (DogoRPCExecuterClient, error) {
	var c DogoRPCExecuterClient
	if n.store.IsLeader() {
		return nil, newInternalErrorError(fmt.Errorf("getRPCClient called but we are the leader"))
	}
	leader := n.store.Leader()
	if leader == "" { // election pending
		var err error
		leader, err = n.store.WaitForLeader(waitForLeaderTimeoutSeconds * time.Second)
		if err != nil {
			return c, newInternalErrorError(fmt.Errorf("getRPCClient: timeout waiting for leader election"))
		}
	}
	n.rpcCtx.RLock()
	if val, ok := n.rpcCtx.ctxmap[leader]; ok {
		s, err := val.conn.State()
		if err != nil {
			n.rpcCtx.RUnlock()
			return c, err
		}
		if s == grpc.Ready || s == grpc.Idle {
			c = n.rpcCtx.ctxmap[leader].client
		}
	}
	n.rpcCtx.RUnlock()
	if c == nil {
		conn, err := grpc.Dial(leader, grpc.WithTimeout(rpcTimeoutSeconds*time.Second), grpc.WithBlock(), grpc.WithInsecure())
		if err != nil {
			return c, err
		}
		client := NewDogoRPCExecuterClient(conn)
		rpcctx := rpcContext{
			conn:   conn,
			client: client,
		}
		n.rpcCtx.Lock()
		n.rpcCtx.ctxmap[leader] = &rpcctx
		n.rpcCtx.Unlock()
		c = client
	}
	return c, nil
}

// DeleteItem deletes a value from the datastore
func (n *Node) DeleteItem(key string) error {
	q := fmt.Sprintf("DELETE FROM key_value_map WHERE key = '%v';", key)
	ldr, err := n.execute(q, false)
	if err != nil {
		return err
	}
	if !ldr {
		return n.ProxyDelete(key)
	}
	return nil
}

func getString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("unexpected type: %T", val)
	}
}

func getInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("unexpected type: %T", val)
	}
}

func getValue(val interface{}) ([]byte, error) {
	switch v := val.(type) {
	case []byte:
		return v, nil
	default:
		return []byte{}, fmt.Errorf("unexpected type: %T", val)
	}
}

// FetchItems retrieves items from the datastore
func (n *Node) FetchItems(keys []string, clvl ConsistencyLevel) ([]*Item, error) {
	var rcl rqlite.ConsistencyLevel
	switch clvl {
	case Strong:
		rcl = rqlite.Strong
	case None:
		rcl = rqlite.None
	default:
		return []*Item{}, newInternalErrorError(fmt.Errorf("unknown ConsistencyLevel: %v", clvl))
	}
	q := `SELECT key, value, size_bytes, flags, exptime FROM key_value_map WHERE`
	where := []string{}
	for _, k := range keys {
		where = append(where, "key", "=", fmt.Sprintf("'%v'", k), "OR")
	}
	where = where[0 : len(where)-1] //drop last "OR"
	q = fmt.Sprintf("%v %v;", q, strings.Join(where, " "))
	rows, err := n.store.Query([]string{q}, false, false, rcl)
	if err != nil {
		if err == rqlite.ErrNotLeader {
			items, err2 := n.ProxyFetch(keys)
			if err2 != nil {
				return []*Item{}, newInternalErrorError(err2)
			}
			return items, nil
		}
		return []*Item{}, newInternalErrorError(err)
	}
	if rows[0].Error != "" {
		return []*Item{}, newBadQueryError(fmt.Errorf(rows[0].Error))
	}
	items := []*Item{}
	for _, v := range rows[0].Values {
		key, err := getString(v[0])
		if err != nil {
			return []*Item{}, newInternalErrorError(fmt.Errorf("key: %v", err))
		}
		exp, err := getInt64(v[4])
		if err != nil {
			return []*Item{}, newInternalErrorError(fmt.Errorf("expires: %v", err))
		}
		expires := time.Unix(exp, 0)
		if exp != 0 && time.Now().After(expires) {
			log.Printf("expiring value: %v", key)
			go n.DeleteItem(key)
			continue
		}
		value, err := getValue(v[1])
		if err != nil {
			return []*Item{}, newInternalErrorError(fmt.Errorf("value: %v", err))
		}
		flags, err := getInt64(v[3])
		if err != nil {
			return []*Item{}, newInternalErrorError(fmt.Errorf("flags: %v", err))
		}
		size, err := getInt64(v[2])
		if err != nil {
			return []*Item{}, newInternalErrorError(fmt.Errorf("size: %v", err))
		}
		if size <= 0 {
			return []*Item{}, newInternalErrorError(fmt.Errorf("bad size (must be >= 1): %v", size))
		}
		item := &Item{
			Name:       key,
			Value:      value,
			Size:       uint64(size),
			Flags:      flags,
			Expiration: expires,
		}
		items = append(items, item)
	}
	return items, nil
}

// AppendItem appends the supplied data to an existing key
func (n *Node) AppendItem(item *Item) error {
	unix, nano := nowTimestamp()
	q := `UPDATE key_value_map SET value = COALESCE(value, X'') || COALESCE(X'%v', X''), last_used = %v, last_used_nano = %v WHERE key = '%v';`
	q = fmt.Sprintf(q, hex.EncodeToString(item.Value), unix, nano, item.Name)
	ldr, err := n.execute(q, false)
	if err != nil {
		return fmt.Errorf("error running node execute: %v", err)
	}
	if !ldr {
		return n.ProxyAppend(item)
	}
	return nil
}

// PrependItem prepends the supplied data to an existing key
func (n *Node) PrependItem(item *Item) error {
	unix, nano := nowTimestamp()
	q := `UPDATE key_value_map SET value = COALESCE(X'%v', X'') || COALESCE(value, X''), last_used = %v, last_used_nano = %v WHERE key = '%v';`
	q = fmt.Sprintf(q, hex.EncodeToString(item.Value), unix, nano, item.Name)
	ldr, err := n.execute(q, false)
	if err != nil {
		return fmt.Errorf("error running node execute: %v", err)
	}
	if !ldr {
		return n.ProxyPrepend(item)
	}
	return nil
}

//StoreItem stores an item in the datastore.
// replace: overwrite item if it already exists, otherwise return error
// implementing KeyExists
func (n *Node) StoreItem(item *Item, policy StorePolicy) error {
	switch policy {
	case InsertOrUpdate:
		return n.storeInsertOrUpdate(item)
	case InsertIfNotExists:
		return n.storeInsertIfNotExists(item)
	case UpdateIfExists:
		return n.storeUpdateIfExists(item)
	default:
		return newInternalErrorError(fmt.Errorf("unknown StorePolicy: %v", policy))
	}
}

func (n *Node) storeInsertOrUpdate(item *Item) error {
	q := `INSERT OR REPLACE INTO key_value_map (key, value, size_bytes, flags, exptime, last_used, last_used_nano, created, created_nano)
	      VALUES (`
	unix, nano := nowTimestamp()
	values := []string{
		fmt.Sprintf("'%v'", item.Name),
		fmt.Sprintf("X'%v'", hex.EncodeToString(item.Value)),
		fmt.Sprintf("%v", item.Size),
		fmt.Sprintf("%v", item.Flags),
		fmt.Sprintf("%v", item.Expiration.Unix()),
		fmt.Sprintf("%v", unix),
		fmt.Sprintf("%v", nano),
		fmt.Sprintf("COALESCE((SELECT created FROM key_value_map WHERE key = '%v'), %v)", item.Name, unix),
		fmt.Sprintf("COALESCE((SELECT created_nano FROM key_value_map WHERE key = '%v'), %v)", item.Name, nano),
	}
	q = fmt.Sprintf("%v%v);", q, strings.Join(values, ", "))
	ldr, err := n.execute(q, false)
	if err != nil {
		return fmt.Errorf("error running node execute: %v", err)
	}
	if !ldr {
		return n.ProxyStore(item, InsertOrUpdate)
	}
	return nil
}

func (n *Node) storeInsertIfNotExists(item *Item) error {
	q := `INSERT OR IGNORE INTO key_value_map (key, value, size_bytes, flags, exptime, created, created_nano, last_used, last_used_nano)
	      VALUES (`
	unix, nano := nowTimestamp()
	values := []string{
		fmt.Sprintf("'%v'", item.Name),
		fmt.Sprintf("X'%v'", hex.EncodeToString(item.Value)),
		fmt.Sprintf("%v", item.Size),
		fmt.Sprintf("%v", item.Flags),
		fmt.Sprintf("%v", item.Expiration.Unix()),
		fmt.Sprintf("%v", unix),
		fmt.Sprintf("%v", nano),
		fmt.Sprintf("%v", unix),
		fmt.Sprintf("%v", nano),
	}
	q = fmt.Sprintf("%v%v);", q, strings.Join(values, ", "))
	ldr, err := n.execute(q, false)
	if err != nil {
		return fmt.Errorf("error running node execute: %v", err)
	}
	if !ldr {
		return n.ProxyStore(item, InsertIfNotExists)
	}
	return nil
}

func (n *Node) storeUpdateIfExists(item *Item) error {
	q := `UPDATE OR IGNORE key_value_map SET %v WHERE key = '%v';`
	unix, nano := nowTimestamp()
	columns := []string{
		fmt.Sprintf("value = X'%v'", hex.EncodeToString(item.Value)),
		fmt.Sprintf("size_bytes = %v", item.Size),
		fmt.Sprintf("flags = %v", item.Flags),
		fmt.Sprintf("exptime = %v", item.Expiration.Unix()),
		fmt.Sprintf("last_used = %v", unix),
		fmt.Sprintf("last_used_nano = %v", nano),
	}
	q = fmt.Sprintf(q, strings.Join(columns, ", "), item.Name)
	ldr, err := n.execute(q, false)
	if err != nil {
		return fmt.Errorf("error running node execute: %v", err)
	}
	if !ldr {
		return n.ProxyStore(item, UpdateIfExists)
	}
	return nil
}

// copypasta
// https://github.com/otoolep/rqlite/blob/master/cmd/rqlited/main.go
func (n *Node) join() error {
	b, err := json.Marshal(map[string]string{"addr": n.config.RaftAddr})
	if err != nil {
		return err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := httpd.NormalizeAddr(fmt.Sprintf("%s/join", n.config.JoinAddr))

	// Enable skipVerify as requested.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !n.config.RaftVerifyTLS},
	}
	client := &http.Client{Transport: tr}

	// Attempt to join.
	resp, err := client.Post(fullAddr, "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to join, node returned: %s", resp.Status)
	}

	return nil
}

// check if tables exist and create if necessary
func (n *Node) checkTables() error {
	q := `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;`
	rows, err := n.store.Query([]string{q}, false, false, rqlite.Strong)
	if err != nil {
		return fmt.Errorf("error querying for tables: %v", err)
	}
	extant := set.NewStringSet([]string{})
	required := set.NewStringSet([]string{})
	for k := range reqTables {
		required.Add(k)
	}
	for _, r := range rows {
		if len(r.Columns) != 1 {
			return fmt.Errorf("unexpected query result: %v", r.Columns)
		}
		if len(r.Values) > 0 {
			switch v := r.Values[0][0].(type) {
			case string:
				extant.Add(v)
			default:
				return fmt.Errorf("unexpected type for value: %T", v)
			}
		}
	}
	if !extant.IsSuperset(required) {
		missing := required.Difference(extant).Items()
		log.Printf("missing table(s): %v", missing)
		for _, m := range missing {
			q = reqTables[m]
			log.Printf("creating table: %v", m)
			_, err := n.store.Execute([]string{q}, false, false)
			if err != nil {
				return fmt.Errorf("error creating table %v: %v", m, err)
			}
		}
		log.Printf("done with table creation")
	}
	return nil
}
