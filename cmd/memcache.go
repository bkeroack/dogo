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
	getq = "SELECT key, value, size_bytes, flags, exptime FROM key_value_map WHERE %v;"
)

type clientError interface {
	ClientError() bool
}

type clientErr struct {
	err error
}

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
	ch.conn.Write([]byte("ERROR\r\n"))
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
	ch.conn.SetDeadline(time.Now().Add(10 * time.Second))
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
		switch cmd {
		case "get":
			if !ch.doCmd(ch.get, cmdline) {
				return
			}
		case "gets":
			if !ch.doCmd(ch.get, cmdline) {
				return
			}
		case "set":
			if !ch.doCmd(ch.set, cmdline) {
				return
			}
		case "add":
			ch.stub(cmdline)
		case "replace":
			ch.stub(cmdline)
		case "append":
			ch.stub(cmdline)
		case "prepend":
			ch.stub(cmdline)
		case "cas":
			ch.stub(cmdline)
		case "delete":
			ch.stub(cmdline)
		case "incr":
			ch.stub(cmdline)
		case "decr":
			ch.stub(cmdline)
		case "touch":
			ch.stub(cmdline)
		case "slabs":
			ch.stub(cmdline)
		case "lru_crawler":
			ch.stub(cmdline)
		case "stats":
			ch.stub(cmdline)
		case "flush_all":
			ch.stub(cmdline)
		case "version":
			ch.stub(cmdline)
		case "verbosity":
			ch.stub(cmdline)
		case "quit":
			return
		default:
			ch.bareError(cmd)
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
		_, err = ch.conn.Write([]byte("\r\n"))
		if err != nil {
			return err
		}
	}
	_, err := ch.conn.Write([]byte("END\r\n"))
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
	log.Printf("%v", cmdline)
	items, err := ch.n.FetchItems(cmdline[1:len(cmdline)], true)
	if err != nil {
		return err
	}
	return ch.retrResponse(items)
}

func (ch *memcacheHandler) set(cmdline []string) error {
	log.Printf("%v", cmdline)
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
	err = ch.n.StoreItem(&item)
	if err != nil {
		return newInternalErrorError(err)
	}
	if !noreply {
		_, err = ch.conn.Write([]byte("STORED\r\n"))
		if err != nil {
			return err
		}
	}
	return nil
}
