// Copyright © 2016 Benjamen Keroack
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/spf13/cobra"
)

var serverConfig NodeConfig
var tcpListener = NodeListener{
	Type:    "tcp",
	Handler: &memcacheHandler{},
}
var unixListener = NodeListener{
	Type:    "unix",
	Handler: &memcacheHandler{},
}
var tcpPort uint16
var tcpAddr string
var unixSocketPath string
var socketListen bool

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: fmt.Sprintf("Run a %v server", appName),
	Long:  `Start up in server mode.`,
	Run:   server,
}

func randomSocketName() string {
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0987654321")
	max := big.NewInt(int64(len(chars)))
	name := []rune{}
	for i := 0; i < 16; i++ {
		ci, err := rand.Int(rand.Reader, max)
		if err != nil {
			log.Fatalf("error generating random character: %v", err)
		}
		name = append(name, chars[ci.Uint64()])
	}
	return fmt.Sprintf("/tmp/dogo-%v.socket", string(name))
}

func displayBindWarning() {
	warnmsg := `WARNING: Bind address is set to '%v'. %v is designed/intended
to listen on localhost only or via Unix socket, for local clients on the same
machine. For remote clients, run a separate instance on each machine and form a
coherent cluster from them all. If clients are connecting remotely you may be
better off using standard memcache/redis/etc. For more info, see documentation.
`
	fmt.Fprintf(os.Stderr, warnmsg, &tcpAddr, appName)
}

func init() {
	serverCmd.PersistentFlags().BoolVarP(&serverConfig.Persistent, "persistent", "p", false, "Use persistent (disk) storage")
	serverCmd.PersistentFlags().Uint16VarP(&tcpPort, "port", "o", 11211, "TCP port to listen on for client traffic")
	serverCmd.PersistentFlags().StringVarP(&tcpAddr, "bind-addr", "b", "localhost", "Listen on this address")
	serverCmd.PersistentFlags().BoolVarP(&socketListen, "unix-socket", "u", false, "Listen on Unix socket instead of TCP")
	serverCmd.PersistentFlags().StringVarP(&unixSocketPath, "socket-path", "s", randomSocketName(), "Unix socket path")
	serverCmd.PersistentFlags().StringVarP(&serverConfig.DataPath, "data-path", "d", "/tmp/dogo", "Path for data and raft state. Must exist and be writable.")
	serverCmd.PersistentFlags().StringVarP(&serverConfig.RaftAddr, "raft-addr", "r", "0.0.0.0:48761", "address/port to listen on for Raft consensus traffic (must be the same for all nodes in cluster)")
	serverCmd.PersistentFlags().StringVarP(&serverConfig.JoinAddr, "join-addr", "j", "", "address/port of the cluster leader to join")
	serverCmd.PersistentFlags().BoolVarP(&serverConfig.RaftVerifyTLS, "verify-tls", "f", true, "Verify TLS certificates for Raft traffic")
	RootCmd.AddCommand(serverCmd)
}

func server(cmd *cobra.Command, args []string) {
	if socketListen {
		unixListener.Addr = unixSocketPath
		serverConfig.Listener = &unixListener
	} else {
		if tcpAddr != "localhost" && tcpAddr != "127.0.0.1" && tcpAddr != "::1" {
			displayBindWarning()
		}
		tcpListener.Addr = fmt.Sprintf("%v:%v", tcpAddr, tcpPort)
		serverConfig.Listener = &tcpListener
	}
	node := NewNode(&serverConfig)
	if err := node.RunNode(true); err != nil {
		log.Printf("error running node; %v", err)
		os.Exit(1)
	}
}
