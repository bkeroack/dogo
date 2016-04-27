package cmd

import (
	"bufio"
	"net"
	"time"
)

func newConnection(n *nodeConfig, conn net.Conn) {
  defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
  for {
    conn.SetDeadline(time.Now().Add(10 * time.Second))
    raw, err := rw.ReadString(byte("\r\n"))
    if err != nil {
      return
    }
    cmdline := strings.Split(raw, " ")
    switch cmd := cmdline[0] {
			case "get":
				if err := get(cmdline, conn); err != nil {
					log.Printf("error executing %v: %v", cmdline[0], err)
				}
      case "gets":
			if err := get(cmdline, conn); err != nil {
				log.Printf("error executing %v: %v", cmdline[0], err)
			}
      case "set":
				stub(cmdline, conn)
      case "add":
				stub(cmdline, conn)
      case "replace":
			stub(cmdline, conn)
      case "append":
			stub(cmdline, conn)
      case "prepend":
			stub(cmdline, conn)
      case "cas":
			stub(cmdline, conn)
      case "delete":
			stub(cmdline, conn)
      case "incr":
			stub(cmdline, conn)
      case "decr":
			stub(cmdline, conn)
      case "touch":
			stub(cmdline, conn)
      case "slabs":
			stub(cmdline, conn)
      case "lru_crawler":
			stub(cmdline, conn)
      case "stats":
			stub(cmdline, conn)
      case "flush_all":
			stub(cmdline, conn)
      case "version":
			stub(cmdline, conn)
      case "verbosity":
			stub(cmdline, conn)
      case "quit":
        return
      default:
        log.Printf("unknown command from %v: %v", conn.RemoteAddr(), cmd)
    }
  }
}

func stub(cmdline []string, conn net.Conn) error {
	log.Printf("unsupported cmd: %v", cmdline[0])
	return nil
}

func get(cmdline []string, conn net.Conn) error {
  q := `SELECT `
}
