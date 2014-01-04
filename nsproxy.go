package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/codegangsta/cli"
	"github.com/coreos/go-etcd/etcd"
	nameNet "github.com/coreos/go-namespaces/net"
)

func proxyConn(conn *net.Conn, addr string) {
	rConn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf(err.Error())
		(*conn).Close()
		return
	}

	go io.Copy(rConn, *conn)
	go io.Copy(*conn, rConn)
}

func proxy(c *cli.Context) {
	addr := c.String("addr")
	target := c.Int("target")
	if target == 0 {
		fmt.Fprintln(os.Stderr, "error: a target pid is required")
		return
	}

	path := c.String("path")
	if path == "" {
		fmt.Fprintln(os.Stderr, "error: an etcd path is required")
		return
	}

	peers := trimsplit(c.String("peers"), ",")
	client := etcd.NewClient(peers)

	log.Printf("Proxying for keys in %s on host %s in namespace %d", path, addr, target)

	// Keep an eye on the backend path
	b := backends{path: path}
	err := b.Sync(client)
	if err != nil {
		log.Fatal(err)
	}

	go b.Watch(client)

	listener, err := nameNet.ListenNamespace(uintptr(target), "tcp", addr)
	if err != nil {
		panic(err)
	}

	for {
		// Wait for a connection.
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		next := b.Next()
		if next == "" {
			conn.Close()
			log.Printf("No backends! Closing the connection")
			continue
		}

		log.Printf("PROXY: targetPid:%d targetAddr:%v addr:%v\n", target, addr, next)
		go proxyConn(&conn, next)
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "nsproxy"
	app.Usage = "Proxy into a network namespace"
	app.Flags = []cli.Flag{
		cli.IntFlag{"target, t", 0, "target namespace pid", false},
		cli.StringFlag{"path, p", "", "path to a dir", false},
		cli.StringFlag{"addr, a", "127.0.0.1:8080", "target address inside the namespace", false},
		cli.StringFlag{"peers, C", "http://127.0.0.1:4001", "a comma seperated list of machine addresses in the cluster", false},
	}
	app.Action = proxy
	app.Run(os.Args)
}
