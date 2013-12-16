package main

import (
	"encoding/json"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/coreos/go-etcd/etcd"
)

type service struct {
	Host string `json:"host"`
	Port int `json:"port"`
}

type backends struct {
	path       string
	hosts      map[string]string
	lastIndex  int
	watchIndex uint64
	lock       sync.RWMutex
}

func (b *backends) Dump() {
	for h, k := range(b.hosts) {
		log.Printf("%s -> %s", h, k)
	}
}

func (b *backends) Update(node *etcd.Node, action string) {
	b.lock.Lock()
	defer b.lock.Unlock()

	s := &service{}
	err := json.Unmarshal([]byte(node.Value), s)
	if err != nil {
		panic(err)
	}

	host := net.JoinHostPort(s.Host, strconv.Itoa(s.Port))

	if action == "delete" {
		delete(b.hosts, host)
		return
	}

	b.hosts[host] = node.Key

	b.Dump()
}

func (b *backends) Watch(client *etcd.Client) {
	receiver := make(chan *etcd.Response)
	go client.Watch(b.path, uint64(b.watchIndex), true, receiver, nil)

	for {
		resp := <-receiver
		b.Update(resp.Node, resp.Action)
	}
}

func (b *backends) Sync(client *etcd.Client) error {
	b.hosts = make(map[string]string)
	resp, err := client.Get(b.path, false, true)

	if err != nil {
		return err
	}

	for _, n := range(resp.Node.Nodes) {
		b.Update(&n, resp.Action)
	}

	// Begin the watch after this sync from the next sync
	b.watchIndex = resp.EtcdIndex + 1

	return nil
}

func (b *backends) Next() string {
	b.lock.RLock()
	defer b.lock.RUnlock()

	if len(b.hosts) == 0 {
		return ""
	}

	index := (b.lastIndex + 1) % len(b.hosts)
	b.lastIndex = index

	for k, _ := range(b.hosts) {
		return k
	}

	return ""
}
