package gossip

// The broadcast mechanism works by maintaining a sorted list of messages to be
// sent out. When a message is to be broadcast, the retransmit count
// is set to zero and appended to the queue. The retransmit count serves
// as the "priority", ensuring that newer messages get sent first. Once
// a message hits the retransmit limit, it is removed from the queue.
// Additionally, older entries can be invalidated by new messages that
// are contradictory. For example, if we send "{suspect M1 inc: 1},
// then a following {alive M1 inc: 2} will invalidate that message

// Broadcast is something that can be broadcasted via gossip to
// the gossip cluster.
type Broadcast interface {
	// Invalidates checks if enqueuing the current broadcast
	// invalidates a previous broadcast
	Invalidates(b Broadcast) bool

	// Returns a byte form of the message
	Message() []byte

	// Finished is invoked when the message will no longer
	// be broadcast, either due to invalidation or to the
	// transmit limit being reached
	Finished()
}

// NamedBroadcast is an optional extension of the Broadcast interface that
// gives each message a unique string name, and that is used to optimize
//
// You shoud ensure that Invalidates() checks the same uniqueness as the
// example below:
//
// func (b *foo) Invalidates(other Broadcast) bool {
// 	nb, ok := other.(NamedBroadcast)
// 	if !ok {
// 		return false
// 	}
// 	return b.Name() == nb.Name()
// }
//
// Invalidates() isn't currently used for NamedBroadcasts, but that may change
// in the future.
type NamedBroadcast interface {
	Broadcast
	// The unique identity of this broadcast message.
	Name() string
}

// UniqueBroadcast is an optional interface that indicates that each message is
// intrinsically unique and there is no need to scan the broadcast queue for
// duplicates.
//
// You should ensure that Invalidates() always returns false if implementing
// this interface. Invalidates() isn't currently used for UniqueBroadcasts, but
// that may change in the future.
type UniqueBroadcast interface {
	Broadcast
	// UniqueBroadcast is just a marker method for this interface.
	UniqueBroadcast()
}

type gossipBroadcast struct {
	node   string
	msg    []byte
	notify chan struct{}
}

func (b *gossipBroadcast) Invalidates(other Broadcast) bool {
	// Check if that broadcast is a gossipBroadcast type
	mb, ok := other.(*gossipBroadcast)
	if !ok {
		return false
	}

	// Invalidates any message about the same node
	return b.node == mb.node
}

// gossip.NamedBroadcast optional interface
func (b *gossipBroadcast) Name() string {
	return b.node
}

func (b *gossipBroadcast) Message() []byte {
	return b.msg
}

func (b *gossipBroadcast) Finished() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}
