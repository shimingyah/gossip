package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/shimingyah/gossip"
)

var (
	ip         = flag.String("ip", "127.0.0.1", "http and gossip node ip address")
	gossipPort = flag.Int("gossipPort", 60000, "gossip port")
	httpPort   = flag.Int("httpPort", 8080, "http port")
	join       = flag.String("join", "", "comma seperated list of peers")
)

var store = NewKVStore()

func init() {
	flag.Parse()
}

func startGosip() error {
	c := gossip.DefaultLocalConfig()
	c.Delegate = NewDelegate(nil, store)
	c.BindAddr = *ip
	c.BindPort = *gossipPort
	c.Name = fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort)

	g, err := gossip.Create(c)
	if err != nil {
		return err
	}

	if len(*join) > 0 {
		peers := strings.Split(*join, ",")
		if _, err := g.Join(peers); err != nil {
			return err
		}
	}

	node := g.LocalNode()
	fmt.Printf("Local node %s\n", node.Address())
	return nil
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	val := r.Form.Get("val")

	if err := store.Set(key, val); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func hasHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	has := store.Has(key)
	if has {
		w.Write([]byte("yes"))
	} else {
		w.Write([]byte("no"))
	}
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	has := store.Has(key)
	if !has {
		http.Error(w, "not found", 404)
		return
	}

	w.Write([]byte(store.Get(key)))
}

func delHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	if err := store.Del(key); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func main() {
	if err := startGosip(); err != nil {
		log.Fatalf("start gossip error: %v", err)
	}

	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/has", hasHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/del", delHandler)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), nil); err != nil {
		log.Fatalf("http listen error: %v", err)
	}
}
