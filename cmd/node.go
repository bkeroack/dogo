package cmd

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"

  "github.com/dollarshaveclub/go-lib/set"
	httpd "github.com/otoolep/rqlite/http"
	rqlite "github.com/otoolep/rqlite/store"
)

type nodeConfig struct {
	persistent  bool
	port        uint16
	usock       bool
	sockpath    string
	bindaddress string
	datapath    string
	raftaddr    string
	joinaddr    string
	verifytls   bool
	store       rqlite.Store
}

var reqTables = map[string]string{
  "key_value_map": `CREATE TABLE key_value_map (key TEXT PRIMARY KEY, value BLOB, created INTEGER, last_used INTEGER, ttl INTEGER);`,
}

func (n *nodeConfig) RunNode() error {
	dataPath, err := filepath.Abs(n.datapath)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data path: %s", err.Error())
	}
	dc := rqlite.NewDBConfig("cache=shared", !n.persistent)
	n.store := rqlite.New(dc, dataPath, n.raftaddr)
	if err := n.store.Open(n.joinaddr == ""); err != nil {
		return fmt.Errorf("failed to open store: %v", err)
	}

	if n.joinaddr != "" {
		if err := n.join(); err != nil {
			log.Fatalf("failed to join node at %v: %v", n.joinaddr, err)
		}
		log.Printf("successfully joined node at %v", n.joinaddr)
	}

  if err := n.checkTables(); err != nil {
    log.Fatalf("error checking tables: %v", err)
  }

	var nt string
	var laddr string
	if n.usock {
		nt = "unix"
		laddr = n.sockpath
	} else {
		nt = "tcp"
		laddr = fmt.Sprintf("%v:%v", n.bindaddress, n.port)
	}

	l, err := net.Listen(nt, laddr)
	if err != nil {
		return fmt.Errorf("error listening: %v", err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("error accepting connection: %v", err)
		}
		go newConnection(n, conn)
	}
	return nil
}

// copypasta
// https://github.com/otoolep/rqlite/blob/master/cmd/rqlited/main.go
func (n *nodeConfig) join() error {
	b, err := json.Marshal(map[string]string{"addr": n.raftaddr})
	if err != nil {
		return err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := httpd.NormalizeAddr(fmt.Sprintf("%s/join", n.joinaddr))

	// Enable skipVerify as requested.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !n.verifytls},
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
func (n *nodeConfig) checkTables() error {
	q := `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;`
  rows, err := n.store.Query([]string{q}, false, false, rqlite.Strong)
  if err != nil {
    return fmt.Errorf("error querying for tables: %v", err)
  }
  extant := set.NewStringSet([]string{})
  required := set.NewStringSet([]string{})
  for k, _ := range reqTables {
    required.Add(k)
  }
  for _, r := range rows {
    if len(r.Columns) != 1 || len(r.Values) != 1 {
      return fmt.Errorf("unexpected query result: %v %v", r.Columns, r.Values)
    }
    switch v := r.Values[0][0].(type) {
    case string:
      extant.Add(v)
    default:
      return fmt.Errorf("unexpected type for value: %T", v)
    }
  }
  if !extant.IsSuperset(required) {
    missing := extant.Difference(required).Items()
    for _, m := range missing {
      q = reqTables[m]
      log.Printf("creating table: %v", m)
      _, err := n.store.Execute([]string{q}, false, false)
      if err != nil {
        return fmt.Errorf("error creating table %v: %v", m, err)
      }
    }
  }
  return nil
}
