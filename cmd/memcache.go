package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	connectionTimeoutSecs = 60
)

type clientError interface {
	ClientError() bool
}

type clientErr struct {
	err error
}

// Static responses
var storedBytes = []byte("STORED\r\n")
var crlfBytes = []byte("\r\n")
var endBytes = []byte("END\r\n")
var errorBytes = []byte("ERROR\r\n")
var deletedBytes = []byte("DELETED\r\n")
var notfoundBytes = []byte("NOT_FOUND\r\n")

func (ce clientErr) ClientError() bool {
	return true
}

func (ce clientErr) Error() string {
	return ce.err.Error()
}

func newClientError(err error) clientErr {
	return clientErr{err: err}
}

func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	i := bytes.IndexByte(data, '\n')
	j := bytes.IndexByte(data, '\r')
	if i >= 0 && j >= 0 && i == (j+1) {
		// We have a full CRLF-terminated line.
		return i + 1, data[0:j], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

type memcacheHandler struct {
	n       *Node
	conn    net.Conn
	scanner *bufio.Scanner
}

func (ch *memcacheHandler) serverError(err error) {
	errstr := fmt.Sprintf("SERVER_ERROR %v\r\n", err)
	ch.conn.Write([]byte(errstr))
}

func (ch *memcacheHandler) clientError(err error) {
	errstr := fmt.Sprintf("CLIENT_ERROR %v\r\n", err)
	ch.conn.Write([]byte(errstr))
}

func (ch *memcacheHandler) bareError(cmd string) {
	log.Printf("unknown command from %v: %v", ch.conn.RemoteAddr(), cmd)
	ch.conn.Write(errorBytes)
}

func (ch *memcacheHandler) doCmd(cmdfunc func([]string) error, cmdline []string) bool {
	if err := cmdfunc(cmdline); err != nil {
		ie, ok := err.(InternalError)
		if ok && ie.InternalError() {
			log.Printf("internal error: %v: %v", cmdline[0], err)
			ch.serverError(err)
			return false
		}
		bq, ok := err.(BadQuery)
		if ok && bq.BadQuery() {
			log.Printf("bad query error: %v: %v", cmdline[0], err)
			ch.serverError(err)
			return false
		}
		ce, ok := err.(clientError)
		if ok && ce.ClientError() {
			log.Printf("client error: %v: %v", cmdline[0], err)
			ch.clientError(err)
			return true
		}
		// unknown error
		log.Printf("unknown error: %v: %v", cmdline[0], err)
		ch.serverError(err)
		return false
	}
	return true
}

func (ch *memcacheHandler) scanBlock() error {
	ch.conn.SetDeadline(time.Now().Add(connectionTimeoutSecs * time.Second))
	if ok := ch.scanner.Scan(); !ok || ch.scanner.Err() != nil {
		return ch.scanner.Err()
	}
	return nil
}

func (ch *memcacheHandler) readDataBlock() ([]byte, error) {
	err := ch.scanBlock()
	if err != nil {
		return []byte{}, err
	}
	return ch.scanner.Bytes(), nil
}

func (ch *memcacheHandler) readTextBlock() (string, error) {
	err := ch.scanBlock()
	if err != nil {
		return "", err
	}
	return ch.scanner.Text(), nil
}

func (ch *memcacheHandler) NewConnection(n *Node, conn net.Conn) {
	defer conn.Close()
	ch.n = n
	ch.conn = conn
	ch.scanner = bufio.NewScanner(bufio.NewReader(conn))
	ch.scanner.Split(scanCRLF)
	for {
		raw, err := ch.readTextBlock()
		if err != nil {
			return
		}
		cmdline := strings.Split(raw, " ")
		cmd := cmdline[0]
		var cmdfunc func([]string) error
		switch cmd {
		case "get":
			cmdfunc = ch.get
		case "gets":
			cmdfunc = ch.get
		case "getc":
			cmdfunc = ch.getc
		case "set":
			cmdfunc = ch.set
		case "add":
			cmdfunc = ch.add
		case "replace":
			cmdfunc = ch.replace
		case "append":
			cmdfunc = ch.appendOrPrepend
		case "prepend":
			cmdfunc = ch.appendOrPrepend
		case "cas":
			cmdfunc = ch.stub
		case "delete":
			cmdfunc = ch.delete
		case "incr":
			cmdfunc = ch.stub
		case "decr":
			cmdfunc = ch.stub
		case "touch":
			cmdfunc = ch.stub
		case "slabs":
			cmdfunc = ch.stub
		case "lru_crawler":
			cmdfunc = ch.stub
		case "stats":
			cmdfunc = ch.stub
		case "flush_all":
			cmdfunc = ch.stub
		case "version":
			cmdfunc = ch.stub
		case "verbosity":
			cmdfunc = ch.stub
		case "quit":
			return
		default:
			ch.bareError(cmd)
			continue
		}
		if !ch.doCmd(cmdfunc, cmdline) {
			return
		}
	}
}

func (ch *memcacheHandler) retrResponse(items []*Item) error {
	for _, item := range items {
		header := fmt.Sprintf("VALUE %v %v %v\r\n", item.Name, item.Flags, item.Size)
		_, err := ch.conn.Write([]byte(header))
		if err != nil {
			return err
		}
		_, err = ch.conn.Write(item.Value)
		if err != nil {
			return err
		}
		_, err = ch.conn.Write(crlfBytes)
		if err != nil {
			return err
		}
	}
	_, err := ch.conn.Write(endBytes)
	if err != nil {
		return err
	}
	return nil
}

func (ch *memcacheHandler) stub(cmdline []string) error {
	log.Printf("unsupported cmd: %v", cmdline[0])
	return nil
}

func (ch *memcacheHandler) get(cmdline []string) error {
	items, err := ch.n.FetchItems(cmdline[1:len(cmdline)], None)
	if err != nil {
		return err
	}
	return ch.retrResponse(items)
}

// getc - "get consistently", extension to standard memcache command set
func (ch *memcacheHandler) getc(cmdline []string) error {
	items, err := ch.n.FetchItems(cmdline[1:len(cmdline)], Strong)
	if err != nil {
		return err
	}
	return ch.retrResponse(items)
}

func (ch *memcacheHandler) set(cmdline []string) error {
	return ch._store(cmdline, InsertOrUpdate)
}

func (ch *memcacheHandler) add(cmdline []string) error {
	return ch._store(cmdline, InsertIfNotExists)
}

func (ch *memcacheHandler) replace(cmdline []string) error {
	return ch._store(cmdline, UpdateIfExists)
}

func (ch *memcacheHandler) _store(cmdline []string, policy StorePolicy) error {
	noreply := false
	if len(cmdline) < 5 || len(cmdline) > 6 {
		return newClientError(fmt.Errorf("malformed command string"))
	}
	if len(cmdline) == 6 {
		if cmdline[5] != "noreply" {
			return newClientError(fmt.Errorf("expected noreply but got: %v", cmdline[5]))
		}
		noreply = true
	}
	flags, err := strconv.ParseInt(cmdline[2], 0, 64)
	if err != nil {
		return newClientError(fmt.Errorf("malformed flags (expected int64)"))
	}
	exp, err := strconv.ParseInt(cmdline[3], 0, 64)
	if err != nil {
		return newClientError(fmt.Errorf("malformed expiration time (expected Unix timestamp)"))
	}
	size, err := strconv.ParseInt(cmdline[4], 0, 64)
	if err != nil || size < 1 {
		return newClientError(fmt.Errorf("malformed data size (expected int64 >= 1)"))
	}
	item := Item{
		Name:       cmdline[1],
		Flags:      flags,
		Expiration: time.Unix(exp, 0),
		Size:       uint64(size),
	}
	data, err := ch.readDataBlock()
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return newClientError(fmt.Errorf("data length (%v) does not equal declared length (%v)", len(data), size))
	}
	item.Value = data
	err = ch.n.StoreItem(&item, policy)
	if err != nil {
		return newInternalErrorError(fmt.Errorf("error storing item: %v", err))
	}
	if !noreply {
		_, err = ch.conn.Write(storedBytes)
		if err != nil {
			return fmt.Errorf("error writing STORED: %v", err)
		}
	}
	return nil
}

func (ch *memcacheHandler) delete(cmdline []string) error {
	noreply := false
	if len(cmdline) < 2 || len(cmdline) > 3 {
		return newClientError(fmt.Errorf("malformed command string"))
	}
	if len(cmdline) == 3 {
		if cmdline[2] != "noreply" {
			return newClientError(fmt.Errorf("expected noreply but got: %v", cmdline[2]))
		}
		noreply = true
	}
	err := ch.n.DeleteItem(cmdline[1])
	if err != nil {
		bq, ok := err.(BadQuery)
		if ok && bq.BadQuery() {
			log.Printf("delete response: %v", err)
			if !noreply {
				_, err = ch.conn.Write(notfoundBytes)
				return err
			}
		}
	}
	if !noreply {
		_, err = ch.conn.Write(deletedBytes)
	}
	return err
}

func (ch *memcacheHandler) appendOrPrepend(cmdline []string) error {
	noreply := false
	if len(cmdline) < 5 || len(cmdline) > 6 {
		return newClientError(fmt.Errorf("malformed command string"))
	}
	if len(cmdline) == 6 {
		if cmdline[5] != "noreply" {
			return newClientError(fmt.Errorf("expected noreply but got: %v", cmdline[5]))
		}
		noreply = true
	}
	size, err := strconv.ParseInt(cmdline[4], 0, 64)
	if err != nil || size < 1 {
		return newClientError(fmt.Errorf("malformed data size (expected int64 >= 1)"))
	}
	item := Item{
		Name: cmdline[1],
		Size: uint64(size),
	}
	data, err := ch.readDataBlock()
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return newClientError(fmt.Errorf("data length (%v) does not equal declared length (%v)", len(data), size))
	}
	item.Value = data
	if cmdline[0] == "append" {
		err = ch.n.AppendItem(&item)
	} else {
		err = ch.n.PrependItem(&item)
	}
	if err != nil {
		return newInternalErrorError(fmt.Errorf("error appending item: %v", err))
	}
	if !noreply {
		_, err = ch.conn.Write(storedBytes)
		if err != nil {
			return fmt.Errorf("error writing STORED: %v", err)
		}
	}
	return nil
}
