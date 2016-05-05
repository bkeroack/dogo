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
	"time"

	"github.com/dollarshaveclub/go-lib/set"
	httpd "github.com/otoolep/rqlite/http"
	rqlite "github.com/otoolep/rqlite/store"
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
	JoinAddr      string //Initial cluster member to join
	Listener      *NodeListener
	RaftVerifyTLS bool //Verify TLS certificates for Raft connections
}

// Node represents a running cluster node
type Node struct {
	config *NodeConfig
	store  *rqlite.Store
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

// BadQuery is implemented by any error returned as a result of malformed
// query (syntax error, etc).
//
// Use by doing a checked type assertion (below assumes err != nil):
//
// bq, ok := err.(BadQuery)
// if ok && bq.BadQuery() {
//		// error was caused by a bad query
// } else {
//    // not a bad query error
// }
//
// see: http://dave.cheney.net/2014/12/24/inspecting-errors
type BadQuery interface {
	BadQuery() bool
}

type badQueryError struct {
	err error
}

func (bqe badQueryError) BadQuery() bool {
	return true
}

func (bqe badQueryError) Error() string {
	return bqe.err.Error()
}

// InternalError is implemented by errors returned for internal/unspecified
// errors (see BadQuery for usage)
type InternalError interface {
	InternalError() bool
}

type internalErrorError struct {
	err error
}

func (iee internalErrorError) InternalError() bool {
	return true
}

func (iee internalErrorError) Error() string {
	return iee.err.Error()
}

func newBadQueryError(err error) error {
	return badQueryError{err: err}
}

func newInternalErrorError(err error) error {
	return internalErrorError{err: err}
}

// Given Unix timestamp and UnixNano timestamp of same instant, compute
// the nanoseconds remainder
func remainderNano(unix int64, nano int64) int64 {
	return nano - (unix * 1000000000)
}

// NewNode creates a new Node object but does not initialize or start it
// A node must be started with RunNode()
func NewNode(config *NodeConfig) *Node {
	n := Node{}
	n.config = config
	return &n
}

//RunNode creates and starts a new cluster node according to NodeConfig
//It will initially block on finding or becoming the cluster leader. A non-nil
//return value indicates that the cluster is up and ready to accept operations.
//If a listener is configured and block is true, RunNode will block forever
//listening for new connections. If block is false, the listener will be started
//asynchronously.
func (n *Node) RunNode(block bool) error {
	dataPath, err := filepath.Abs(n.config.DataPath)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data path: %s", err.Error())
	}
	dc := rqlite.NewDBConfig("cache=shared", !n.config.Persistent)
	n.store = rqlite.New(dc, dataPath, n.config.RaftAddr)
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

// ProxyFetch determines cluster leader and performs a remote fetch from it
func (n *Node) ProxyFetch(keys []string) ([]*Item, error) {
	//leader := n.store.Leader()
	return []*Item{}, nil
}

// ProxyStore determines cluster leader and performs a remote store
func (n *Node) ProxyStore(item *Item) error {
	//leader := n.store.Leader()
	return nil
}

// ProxyExpire determines cluster leader and performs a remote expiration
func (n *Node) ProxyExpire(key string) error {
	return nil
}

// ExpireValue deletes an expired value from the datastore
func (n *Node) ExpireValue(key string) error {
	q := fmt.Sprintf("DELETE FROM key_value_map WHERE key = '%v';", key)
	ldr, err := n.execute(q, false)
	if err != nil {
		return err
	}
	if !ldr {
		return n.ProxyExpire(key)
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
	case string: //wtf
		log.Printf("val is a string: %v", v)
		return []byte(v), nil
	default:
		return []byte{}, fmt.Errorf("unexpected type: %T", val)
	}
}

// FetchItems retrieves items from the datastore
func (n *Node) FetchItems(keys []string, fast bool) ([]*Item, error) {
	var clvl rqlite.ConsistencyLevel
	if fast {
		clvl = rqlite.None
	} else {
		clvl = rqlite.Strong
	}
	q := `SELECT key, value, size_bytes, flags, exptime FROM key_value_map WHERE`
	where := []string{}
	for _, k := range keys {
		where = append(where, "key", "=", fmt.Sprintf("'%v'", k), "OR")
	}
	where = where[0 : len(where)-1] //drop last "OR"
	q = fmt.Sprintf("%v %v;", q, strings.Join(where, " "))
	rows, err := n.store.Query([]string{q}, false, false, clvl)
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
		if time.Now().After(expires) {
			log.Printf("expiring value: %v", key)
			go n.ExpireValue(key)
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

//StoreItem stores an item in the datastore
func (n *Node) StoreItem(item *Item) error {
	q := `INSERT INTO key_value_map (key, value, size_bytes, flags, exptime, created, created_nano, last_used, last_used_nano)
	      VALUES (`
	now := time.Now()
	nu := now.Unix()
	nn := remainderNano(nu, now.UnixNano())
	values := []string{
		fmt.Sprintf("'%v'", item.Name),
		fmt.Sprintf("X'%v'", hex.EncodeToString(item.Value)),
		fmt.Sprintf("%v", item.Size),
		fmt.Sprintf("%v", item.Flags),
		fmt.Sprintf("%v", item.Expiration.Unix()),
		fmt.Sprintf("%v", nu),
		fmt.Sprintf("%v", nn),
		fmt.Sprintf("%v", nu),
		fmt.Sprintf("%v", nn),
	}
	q = fmt.Sprintf("%v%v);", q, strings.Join(values, ", "))
	log.Printf("store query: %v", q)
	ldr, err := n.execute(q, false)
	if err != nil {
		return fmt.Errorf("error running node execute: %v", err)
	}
	if !ldr {
		return n.ProxyStore(item)
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
