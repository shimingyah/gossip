package gossip

import (
	"reflect"
	"testing"
)

func TestGossipBroadcast_Invalidates(t *testing.T) {
	g1 := &gossipBroadcast{"test", nil, nil}
	g2 := &gossipBroadcast{"foo", nil, nil}

	if g1.Invalidates(g2) || g2.Invalidates(g1) {
		t.Fatalf("unexpected invalidation")
	}

	if !g1.Invalidates(g1) {
		t.Fatalf("expected invalidation")
	}
}

func TestGossipBroadcast_Message(t *testing.T) {
	g1 := &gossipBroadcast{"test", []byte("test"), nil}
	msg := g1.Message()
	if !reflect.DeepEqual(msg, []byte("test")) {
		t.Fatalf("messages do not match")
	}
}
