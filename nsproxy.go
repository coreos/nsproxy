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
	"github.com/coreos/go-namespaces/namespace"
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
	namespacePid := c.Int("namespace-pid")
	namespacePath := c.String("namespace-path")
	if namespacePid == 0 && namespacePath == "" {
		fmt.Fprintln(os.Stderr, "error: a namespace pid or path is required")
		return
	}

	path := c.String("path")
	if path == "" {
		fmt.Fprintln(os.Stderr, "error: an etcd path is required")
		return
	}

	if namespacePid != 0 {
		p, err := namespace.ProcessPath(namespacePid, namespace.CLONE_NEWNET)
		if err != nil {
			panic(err)
		}
		namespacePath = p
	}

	peers := trimsplit(c.String("peers"), ",")
	client := etcd.NewClient(peers)

	log.Printf("Proxying for keys in %s on host %s", path, addr)

	// Keep an eye on the backend path
	b := backends{path: path}
	err := b.Sync(client)
	if err != nil {
		log.Fatal(err)
	}

	go b.Watch(client)

	log.Printf("Proxying for keys in %s on host %s in namespace %d", path, addr, namespacePath)
	listener, err := nameNet.ListenNamespace(namespacePath, "tcp", addr)

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

		log.Printf("PROXY: targetAddr:%v addr:%v\n", addr, next)
		go proxyConn(&conn, next)
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "nsproxy"
	app.Usage = "Proxy into a network namespace"
	app.Flags = []cli.Flag{
		cli.IntFlag{"namespace-pid, ns-pid", 0, "target namespace pid", false},
		cli.StringFlag{"namespace-path, ns-path", "", "target namespace path", false},
		cli.StringFlag{"path, p", "", "path to a etcd dir", false},
		cli.StringFlag{"addr, a", "127.0.0.1:8080", "target address inside the namespace", false},
		cli.StringFlag{"peers, C", "http://127.0.0.1:4001", "a comma seperated list of machine addresses in the etcd cluster", false},
	}
	app.Action = proxy
	app.Run(os.Args)
}
