package gossip

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"
)

type nodeStateType int

const (
	stateAlive nodeStateType = iota
	stateSuspect
	stateDead
)

// Node represents a node in the cluster.
type Node struct {
	Name string
	Addr net.IP
	Port uint16
	Meta []byte // Metadata from the delegate for this node.
	PMin uint8  // Minimum protocol version this understands
	PMax uint8  // Maximum protocol version this understands
	PCur uint8  // Current version node is speaking
	DMin uint8  // Min protocol version for the delegate to understand
	DMax uint8  // Max protocol version for the delegate to understand
	DCur uint8  // Current version delegate is speaking
}

// Address returns the host:port form of a node's address, suitable for use
// with a transport.
func (n *Node) Address() string {
	return joinHostPort(n.Addr.String(), n.Port)
}

// String returns the node name
func (n *Node) String() string {
	return n.Name
}

// NodeState is used to manage our state view of another node
type nodeState struct {
	Node
	Incarnation uint32        // Last known incarnation number
	State       nodeStateType // Current state
	StateChange time.Time     // Time last state change happened
}

// Address returns the host:port form of a node's address, suitable for use
// with a transport.
func (n *nodeState) Address() string {
	return n.Node.Address()
}

// ackHandler is used to register handlers for incoming acks and nacks.
type ackHandler struct {
	ackFn  func([]byte, time.Time)
	nackFn func()
	timer  *time.Timer
}

// NoPingResponseError is used to indicate a 'ping' packet was
// successfully issued but no response was received
type NoPingResponseError struct {
	node string
}

func (f NoPingResponseError) Error() string {
	return fmt.Sprintf("No response from node %s", f.node)
}

// Schedule is used to ensure the Tick is performed periodically. This
// function is safe to call multiple times. If the gossip is already
// scheduled, then it won't do anything.
func (g *Gossip) schedule() {
	g.tickerLock.Lock()
	defer g.tickerLock.Unlock()

	// If we already have tickers, then don't do anything, since we're
	// scheduled
	if len(g.tickers) > 0 {
		return
	}

	// Create the stop tick channel, a blocking channel. We close this
	// when we should stop the tickers.
	stopCh := make(chan struct{})

	// Create a new probeTicker
	if g.config.ProbeInterval > 0 {
		t := time.NewTicker(g.config.ProbeInterval)
		go g.triggerFunc(g.config.ProbeInterval, t.C, stopCh, g.probe)
		g.tickers = append(g.tickers, t)
	}

	// Create a push pull ticker if needed
	if g.config.PushPullInterval > 0 {
		go g.pushPullTrigger(stopCh)
	}

	// Create a gossip ticker if needed
	if g.config.GossipInterval > 0 && g.config.GossipNodes > 0 {
		t := time.NewTicker(g.config.GossipInterval)
		go g.triggerFunc(g.config.GossipInterval, t.C, stopCh, g.gossip)
		g.tickers = append(g.tickers, t)
	}

	// If we made any tickers, then record the stopTick channel for
	// later.
	if len(g.tickers) > 0 {
		g.stopTick = stopCh
	}
}

// triggerFunc is used to trigger a function call each time a
// message is received until a stop tick arrives.
func (g *Gossip) triggerFunc(stagger time.Duration, C <-chan time.Time, stop <-chan struct{}, f func()) {
	// Use a random stagger to avoid syncronizing
	randStagger := time.Duration(uint64(rand.Int63()) % uint64(stagger))
	select {
	case <-time.After(randStagger):
	case <-stop:
		return
	}
	for {
		select {
		case <-C:
			f()
		case <-stop:
			return
		}
	}
}

// pushPullTrigger is used to periodically trigger a push/pull until
// a stop tick arrives. We don't use triggerFunc since the push/pull
// timer is dynamically scaled based on cluster size to avoid network
// saturation
func (g *Gossip) pushPullTrigger(stop <-chan struct{}) {
	interval := g.config.PushPullInterval

	// Use a random stagger to avoid syncronizing
	randStagger := time.Duration(uint64(rand.Int63()) % uint64(interval))
	select {
	case <-time.After(randStagger):
	case <-stop:
		return
	}

	// Tick using a dynamic timer
	for {
		tickTime := pushPullScale(interval, g.estNumNodes())
		select {
		case <-time.After(tickTime):
			g.pushPull()
		case <-stop:
			return
		}
	}
}

// Deschedule is used to stop the background maintenance. This is safe
// to call multiple times.
func (g *Gossip) deschedule() {
	g.tickerLock.Lock()
	defer g.tickerLock.Unlock()

	// If we have no tickers, then we aren't scheduled.
	if len(g.tickers) == 0 {
		return
	}

	// Close the stop channel so all the ticker listeners stop.
	close(g.stopTick)

	// Explicitly stop all the tickers themselves so they don't take
	// up any more resources, and get rid of the list.
	for _, t := range g.tickers {
		t.Stop()
	}
	g.tickers = nil
}

// Tick is used to perform a single round of failure detection and gossip
func (g *Gossip) probe() {
	// Track the number of indexes we've considered probing
	numCheck := 0
START:
	g.nodeLock.RLock()

	// Make sure we don't wrap around infinitely
	if numCheck >= len(g.nodes) {
		g.nodeLock.RUnlock()
		return
	}

	// Handle the wrap around case
	if g.probeIndex >= len(g.nodes) {
		g.nodeLock.RUnlock()
		g.resetNodes()
		g.probeIndex = 0
		numCheck++
		goto START
	}

	// Determine if we should probe this node
	skip := false
	var node nodeState

	node = *g.nodes[g.probeIndex]
	if node.Name == g.config.Name {
		skip = true
	} else if node.State == stateDead {
		skip = true
	}

	// Potentially skip
	g.nodeLock.RUnlock()
	g.probeIndex++
	if skip {
		numCheck++
		goto START
	}

	// Probe the specific node
	g.probeNode(&node)
}

// probeNodeByAddr just safely calls probeNode given only the address of the node (for tests)
func (g *Gossip) probeNodeByAddr(addr string) {
	g.nodeLock.RLock()
	n := g.nodeMap[addr]
	g.nodeLock.RUnlock()

	g.probeNode(n)
}

// probeNode handles a single round of failure checking on a node.
func (g *Gossip) probeNode(node *nodeState) {
	defer metrics.MeasureSince([]string{"gossip", "probeNode"}, time.Now())

	// We use our health awareness to scale the overall probe interval, so we
	// slow down if we detect problems. The ticker that calls us can handle
	// us running over the base interval, and will skip missed ticks.
	probeInterval := g.awareness.ScaleTimeout(g.config.ProbeInterval)
	if probeInterval > g.config.ProbeInterval {
		metrics.IncrCounter([]string{"gossip", "degraded", "probe"}, 1)
	}

	// Prepare a ping message and setup an ack handler.
	ping := ping{SeqNo: g.nextSeqNo(), Node: node.Name}
	ackCh := make(chan ackMessage, g.config.IndirectChecks+1)
	nackCh := make(chan struct{}, g.config.IndirectChecks+1)
	g.setProbeChannels(ping.SeqNo, ackCh, nackCh, probeInterval)

	// Mark the sent time here, which should be after any pre-processing but
	// before system calls to do the actual send. This probably over-reports
	// a bit, but it's the best we can do. We had originally put this right
	// after the I/O, but that would sometimes give negative RTT measurements
	// which was not desirable.
	sent := time.Now()

	// Send a ping to the node. If this node looks like it's suspect or dead,
	// also tack on a suspect message so that it has a chance to refute as
	// soon as possible.
	deadline := sent.Add(probeInterval)
	addr := node.Address()
	if node.State == stateAlive {
		if err := g.encodeAndSendMsg(addr, pingMsg, &ping); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to send ping: %s", err)
			return
		}
	} else {
		var msgs [][]byte
		if buf, err := encode(pingMsg, &ping); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to encode ping message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}
		s := suspect{Incarnation: node.Incarnation, Node: node.Name, From: g.config.Name}
		if buf, err := encode(suspectMsg, &s); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to encode suspect message: %s", err)
			return
		} else {
			msgs = append(msgs, buf.Bytes())
		}

		compound := makeCompoundMessage(msgs)
		if err := g.rawSendMsgPacket(addr, &node.Node, compound.Bytes()); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to send compound ping and suspect message to %s: %s", addr, err)
			return
		}
	}

	// Arrange for our self-awareness to get updated. At this point we've
	// sent the ping, so any return statement means the probe succeeded
	// which will improve our health until we get to the failure scenarios
	// at the end of this function, which will alter this delta variable
	// accordingly.
	awarenessDelta := -1
	defer func() {
		g.awareness.ApplyDelta(awarenessDelta)
	}()

	// Wait for response or round-trip-time.
	select {
	case v := <-ackCh:
		if v.Complete == true {
			if g.config.Ping != nil {
				rtt := v.Timestamp.Sub(sent)
				g.config.Ping.NotifyPingComplete(&node.Node, rtt, v.Payload)
			}
			return
		}

		// As an edge case, if we get a timeout, we need to re-enqueue it
		// here to break out of the select below.
		if v.Complete == false {
			ackCh <- v
		}
	case <-time.After(g.config.ProbeTimeout):
		// Note that we don't scale this timeout based on awareness and
		// the health score. That's because we don't really expect waiting
		// longer to help get UDP through. Since health does extend the
		// probe interval it will give the TCP fallback more time, which
		// is more active in dealing with lost packets, and it gives more
		// time to wait for indirect acks/nacks.
		g.logger.Printf("[DEBUG] gossip: Failed ping: %v (timeout reached)", node.Name)
	}

	// Get some random live nodes.
	g.nodeLock.RLock()
	kNodes := kRandomNodes(g.config.IndirectChecks, g.nodes, func(n *nodeState) bool {
		return n.Name == g.config.Name ||
			n.Name == node.Name ||
			n.State != stateAlive
	})
	g.nodeLock.RUnlock()

	// Attempt an indirect ping.
	expectedNacks := 0
	ind := indirectPingReq{SeqNo: ping.SeqNo, Target: node.Addr, Port: node.Port, Node: node.Name}
	for _, peer := range kNodes {
		// We only expect nack to be sent from peers who understand
		// version 4 of the protocol.
		if ind.Nack = peer.PMax >= 4; ind.Nack {
			expectedNacks++
		}

		if err := g.encodeAndSendMsg(peer.Address(), indirectPingMsg, &ind); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to send indirect ping: %s", err)
		}
	}

	// Also make an attempt to contact the node directly over TCP. This
	// helps prevent confused clients who get isolated from UDP traffic
	// but can still speak TCP (which also means they can possibly report
	// misinformation to other nodes via anti-entropy), avoiding flapping in
	// the cluster.
	//
	// This is a little unusual because we will attempt a TCP ping to any
	// member who understands version 3 of the protocol, regardless of
	// which protocol version we are speaking. That's why we've included a
	// config option to turn this off if desired.
	fallbackCh := make(chan bool, 1)
	if (!g.config.DisableTcpPings) && (node.PMax >= 3) {
		go func() {
			defer close(fallbackCh)
			didContact, err := g.sendPingAndWaitForAck(node.Address(), ping, deadline)
			if err != nil {
				g.logger.Printf("[ERR] gossip: Failed fallback ping: %s", err)
			} else {
				fallbackCh <- didContact
			}
		}()
	} else {
		close(fallbackCh)
	}

	// Wait for the acks or timeout. Note that we don't check the fallback
	// channel here because we want to issue a warning below if that's the
	// *only* way we hear back from the peer, so we have to let this time
	// out first to allow the normal UDP-based acks to come in.
	select {
	case v := <-ackCh:
		if v.Complete == true {
			return
		}
	}

	// Finally, poll the fallback channel. The timeouts are set such that
	// the channel will have something or be closed without having to wait
	// any additional time here.
	for didContact := range fallbackCh {
		if didContact {
			g.logger.Printf("[WARN] gossip: Was able to connect to %s but other probes failed, network may be misconfigured", node.Name)
			return
		}
	}

	// Update our self-awareness based on the results of this failed probe.
	// If we don't have peers who will send nacks then we penalize for any
	// failed probe as a simple health metric. If we do have peers to nack
	// verify, then we can use that as a more sophisticated measure of self-
	// health because we assume them to be working, and they can help us
	// decide if the probed node was really dead or if it was something wrong
	// with ourselves.
	awarenessDelta = 0
	if expectedNacks > 0 {
		if nackCount := len(nackCh); nackCount < expectedNacks {
			awarenessDelta += (expectedNacks - nackCount)
		}
	} else {
		awarenessDelta++
	}

	// No acks received from target, suspect it as failed.
	g.logger.Printf("[INFO] gossip: Suspect %s has failed, no acks received", node.Name)
	s := suspect{Incarnation: node.Incarnation, Node: node.Name, From: g.config.Name}
	g.suspectNode(&s)
}

// Ping initiates a ping to the node with the specified name.
func (g *Gossip) Ping(node string, addr net.Addr) (time.Duration, error) {
	// Prepare a ping message and setup an ack handler.
	ping := ping{SeqNo: g.nextSeqNo(), Node: node}
	ackCh := make(chan ackMessage, g.config.IndirectChecks+1)
	g.setProbeChannels(ping.SeqNo, ackCh, nil, g.config.ProbeInterval)

	// Send a ping to the node.
	if err := g.encodeAndSendMsg(addr.String(), pingMsg, &ping); err != nil {
		return 0, err
	}

	// Mark the sent time here, which should be after any pre-processing and
	// system calls to do the actual send. This probably under-reports a bit,
	// but it's the best we can do.
	sent := time.Now()

	// Wait for response or timeout.
	select {
	case v := <-ackCh:
		if v.Complete == true {
			return v.Timestamp.Sub(sent), nil
		}
	case <-time.After(g.config.ProbeTimeout):
		// Timeout, return an error below.
	}

	g.logger.Printf("[DEBUG] gossip: Failed UDP ping: %v (timeout reached)", node)
	return 0, NoPingResponseError{ping.Node}
}

// resetNodes is used when the tick wraps around. It will reap the
// dead nodes and shuffle the node list.
func (g *Gossip) resetNodes() {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()

	// Move dead nodes, but respect gossip to the dead interval
	deadIdx := moveDeadNodes(g.nodes, g.config.GossipToTheDeadTime)

	// Deregister the dead nodes
	for i := deadIdx; i < len(g.nodes); i++ {
		delete(g.nodeMap, g.nodes[i].Name)
		g.nodes[i] = nil
	}

	// Trim the nodes to exclude the dead nodes
	g.nodes = g.nodes[0:deadIdx]

	// Update numNodes after we've trimmed the dead nodes
	atomic.StoreUint32(&g.numNodes, uint32(deadIdx))

	// Shuffle live nodes
	shuffleNodes(g.nodes)
}

// gossip is invoked every GossipInterval period to broadcast our gossip
// messages to a few random nodes.
func (g *Gossip) gossip() {
	defer metrics.MeasureSince([]string{"gossip", "gossip"}, time.Now())

	// Get some random live, suspect, or recently dead nodes
	g.nodeLock.RLock()
	kNodes := kRandomNodes(g.config.GossipNodes, g.nodes, func(n *nodeState) bool {
		if n.Name == g.config.Name {
			return true
		}

		switch n.State {
		case stateAlive, stateSuspect:
			return false

		case stateDead:
			return time.Since(n.StateChange) > g.config.GossipToTheDeadTime

		default:
			return true
		}
	})
	g.nodeLock.RUnlock()

	// Compute the bytes available
	bytesAvail := g.config.UDPBufferSize - compoundHeaderOverhead
	if g.config.EncryptionEnabled() {
		bytesAvail -= encryptOverhead(g.encryptionVersion())
	}

	for _, node := range kNodes {
		// Get any pending broadcasts
		msgs := g.getBroadcasts(compoundOverhead, bytesAvail)
		if len(msgs) == 0 {
			return
		}

		addr := node.Address()
		if len(msgs) == 1 {
			// Send single message as is
			if err := g.rawSendMsgPacket(addr, &node.Node, msgs[0]); err != nil {
				g.logger.Printf("[ERR] gossip: Failed to send gossip to %s: %s", addr, err)
			}
		} else {
			// Otherwise create and send a compound message
			compound := makeCompoundMessage(msgs)
			if err := g.rawSendMsgPacket(addr, &node.Node, compound.Bytes()); err != nil {
				g.logger.Printf("[ERR] gossip: Failed to send gossip to %s: %s", addr, err)
			}
		}
	}
}

// pushPull is invoked periodically to randomly perform a complete state
// exchange. Used to ensure a high level of convergence, but is also
// reasonably expensive as the entire state of this node is exchanged
// with the other node.
func (g *Gossip) pushPull() {
	// Get a random live node
	g.nodeLock.RLock()
	nodes := kRandomNodes(1, g.nodes, func(n *nodeState) bool {
		return n.Name == g.config.Name ||
			n.State != stateAlive
	})
	g.nodeLock.RUnlock()

	// If no nodes, bail
	if len(nodes) == 0 {
		return
	}
	node := nodes[0]

	// Attempt a push pull
	if err := g.pushPullNode(node.Address(), false); err != nil {
		g.logger.Printf("[ERR] gossip: Push/Pull with %s failed: %s", node.Name, err)
	}
}

// pushPullNode does a complete state exchange with a specific node.
func (g *Gossip) pushPullNode(addr string, join bool) error {
	defer metrics.MeasureSince([]string{"gossip", "pushPullNode"}, time.Now())

	// Attempt to send and receive with the node
	remote, userState, err := g.sendAndReceiveState(addr, join)
	if err != nil {
		return err
	}

	if err := g.mergeRemoteState(join, remote, userState); err != nil {
		return err
	}
	return nil
}

// verifyProtocol verifies that all the remote nodes can speak with our
// nodes and vice versa on both the core protocol as well as the
// delegate protocol level.
//
// The verification works by finding the maximum minimum and
// minimum maximum understood protocol and delegate versions. In other words,
// it finds the common denominator of protocol and delegate version ranges
// for the entire cluster.
//
// After this, it goes through the entire cluster (local and remote) and
// verifies that everyone's speaking protocol versions satisfy this range.
// If this passes, it means that every node can understand each other.
func (g *Gossip) verifyProtocol(remote []pushNodeState) error {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()

	// Maximum minimum understood and minimum maximum understood for both
	// the protocol and delegate versions. We use this to verify everyone
	// can be understood.
	var maxpmin, minpmax uint8
	var maxdmin, mindmax uint8
	minpmax = math.MaxUint8
	mindmax = math.MaxUint8

	for _, rn := range remote {
		// If the node isn't alive, then skip it
		if rn.State != stateAlive {
			continue
		}

		// Skip nodes that don't have versions set, it just means
		// their version is zero.
		if len(rn.Vsn) == 0 {
			continue
		}

		if rn.Vsn[0] > maxpmin {
			maxpmin = rn.Vsn[0]
		}

		if rn.Vsn[1] < minpmax {
			minpmax = rn.Vsn[1]
		}

		if rn.Vsn[3] > maxdmin {
			maxdmin = rn.Vsn[3]
		}

		if rn.Vsn[4] < mindmax {
			mindmax = rn.Vsn[4]
		}
	}

	for _, n := range g.nodes {
		// Ignore non-alive nodes
		if n.State != stateAlive {
			continue
		}

		if n.PMin > maxpmin {
			maxpmin = n.PMin
		}

		if n.PMax < minpmax {
			minpmax = n.PMax
		}

		if n.DMin > maxdmin {
			maxdmin = n.DMin
		}

		if n.DMax < mindmax {
			mindmax = n.DMax
		}
	}

	// Now that we definitively know the minimum and maximum understood
	// version that satisfies the whole cluster, we verify that every
	// node in the cluster satisifies this.
	for _, n := range remote {
		var nPCur, nDCur uint8
		if len(n.Vsn) > 0 {
			nPCur = n.Vsn[2]
			nDCur = n.Vsn[5]
		}

		if nPCur < maxpmin || nPCur > minpmax {
			return fmt.Errorf(
				"Node '%s' protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nPCur, maxpmin, minpmax)
		}

		if nDCur < maxdmin || nDCur > mindmax {
			return fmt.Errorf(
				"Node '%s' delegate protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nDCur, maxdmin, mindmax)
		}
	}

	for _, n := range g.nodes {
		nPCur := n.PCur
		nDCur := n.DCur

		if nPCur < maxpmin || nPCur > minpmax {
			return fmt.Errorf(
				"Node '%s' protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nPCur, maxpmin, minpmax)
		}

		if nDCur < maxdmin || nDCur > mindmax {
			return fmt.Errorf(
				"Node '%s' delegate protocol version (%d) is incompatible: [%d, %d]",
				n.Name, nDCur, maxdmin, mindmax)
		}
	}

	return nil
}

// nextSeqNo returns a usable sequence number in a thread safe way
func (g *Gossip) nextSeqNo() uint32 {
	return atomic.AddUint32(&g.sequenceNum, 1)
}

// nextIncarnation returns the next incarnation number in a thread safe way
func (g *Gossip) nextIncarnation() uint32 {
	return atomic.AddUint32(&g.incarnation, 1)
}

// skipIncarnation adds the positive offset to the incarnation number.
func (g *Gossip) skipIncarnation(offset uint32) uint32 {
	return atomic.AddUint32(&g.incarnation, offset)
}

// estNumNodes is used to get the current estimate of the number of nodes
func (g *Gossip) estNumNodes() int {
	return int(atomic.LoadUint32(&g.numNodes))
}

type ackMessage struct {
	Complete  bool
	Payload   []byte
	Timestamp time.Time
}

// setProbeChannels is used to attach the ackCh to receive a message when an ack
// with a given sequence number is received. The `complete` field of the message
// will be false on timeout. Any nack messages will cause an empty struct to be
// passed to the nackCh, which can be nil if not needed.
func (g *Gossip) setProbeChannels(seqNo uint32, ackCh chan ackMessage, nackCh chan struct{}, timeout time.Duration) {
	// Create handler functions for acks and nacks
	ackFn := func(payload []byte, timestamp time.Time) {
		select {
		case ackCh <- ackMessage{true, payload, timestamp}:
		default:
		}
	}
	nackFn := func() {
		select {
		case nackCh <- struct{}{}:
		default:
		}
	}

	// Add the handlers
	ah := &ackHandler{ackFn, nackFn, nil}
	g.ackLock.Lock()
	g.ackHandlers[seqNo] = ah
	g.ackLock.Unlock()

	// Setup a reaping routing
	ah.timer = time.AfterFunc(timeout, func() {
		g.ackLock.Lock()
		delete(g.ackHandlers, seqNo)
		g.ackLock.Unlock()
		select {
		case ackCh <- ackMessage{false, nil, time.Now()}:
		default:
		}
	})
}

// setAckHandler is used to attach a handler to be invoked when an ack with a
// given sequence number is received. If a timeout is reached, the handler is
// deleted. This is used for indirect pings so does not configure a function
// for nacks.
func (g *Gossip) setAckHandler(seqNo uint32, ackFn func([]byte, time.Time), timeout time.Duration) {
	// Add the handler
	ah := &ackHandler{ackFn, nil, nil}
	g.ackLock.Lock()
	g.ackHandlers[seqNo] = ah
	g.ackLock.Unlock()

	// Setup a reaping routing
	ah.timer = time.AfterFunc(timeout, func() {
		g.ackLock.Lock()
		delete(g.ackHandlers, seqNo)
		g.ackLock.Unlock()
	})
}

// Invokes an ack handler if any is associated, and reaps the handler immediately
func (g *Gossip) invokeAckHandler(ack ackResp, timestamp time.Time) {
	g.ackLock.Lock()
	ah, ok := g.ackHandlers[ack.SeqNo]
	delete(g.ackHandlers, ack.SeqNo)
	g.ackLock.Unlock()
	if !ok {
		return
	}
	ah.timer.Stop()
	ah.ackFn(ack.Payload, timestamp)
}

// Invokes nack handler if any is associated.
func (g *Gossip) invokeNackHandler(nack nackResp) {
	g.ackLock.Lock()
	ah, ok := g.ackHandlers[nack.SeqNo]
	g.ackLock.Unlock()
	if !ok || ah.nackFn == nil {
		return
	}
	ah.nackFn()
}

// refute gossips an alive message in response to incoming information that we
// are suspect or dead. It will make sure the incarnation number beats the given
// accusedInc value, or you can supply 0 to just get the next incarnation number.
// This alters the node state that's passed in so this MUST be called while the
// nodeLock is held.
func (g *Gossip) refute(me *nodeState, accusedInc uint32) {
	// Make sure the incarnation number beats the accusation.
	inc := g.nextIncarnation()
	if accusedInc >= inc {
		inc = g.skipIncarnation(accusedInc - inc + 1)
	}
	me.Incarnation = inc

	// Decrease our health because we are being asked to refute a probleg.
	g.awareness.ApplyDelta(1)

	// Format and broadcast an alive message.
	a := alive{
		Incarnation: inc,
		Node:        me.Name,
		Addr:        me.Addr,
		Port:        me.Port,
		Meta:        me.Meta,
		Vsn: []uint8{
			me.PMin, me.PMax, me.PCur,
			me.DMin, me.DMax, me.DCur,
		},
	}
	g.encodeAndBroadcast(me.Addr.String(), aliveMsg, a)
}

// aliveNode is invoked by the network layer when we get a message about a
// live node.
func (g *Gossip) aliveNode(a *alive, notify chan struct{}, bootstrap bool) {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()
	state, ok := g.nodeMap[a.Node]

	// It is possible that during a Leave(), there is already an aliveMsg
	// in-queue to be processed but blocked by the locks above. If we let
	// that aliveMsg process, it'll cause us to re-join the cluster. This
	// ensures that we don't.
	if g.hasLeft() && a.Node == g.config.Name {
		return
	}

	if len(a.Vsn) >= 3 {
		pMin := a.Vsn[0]
		pMax := a.Vsn[1]
		pCur := a.Vsn[2]
		if pMin == 0 || pMax == 0 || pMin > pMax {
			g.logger.Printf("[WARN] gossip: Ignoring an alive message for '%s' (%v:%d) because protocol version(s) are wrong: %d <= %d <= %d should be >0", a.Node, net.IP(a.Addr), a.Port, pMin, pCur, pMax)
			return
		}
	}

	// Invoke the Alive delegate if any. This can be used to filter out
	// alive messages based on custom logic. For example, using a cluster name.
	// Using a merge delegate is not enough, as it is possible for passive
	// cluster merging to still occur.
	if g.config.Alive != nil {
		if len(a.Vsn) < 6 {
			g.logger.Printf("[WARN] gossip: ignoring alive message for '%s' (%v:%d) because Vsn is not present",
				a.Node, net.IP(a.Addr), a.Port)
			return
		}
		node := &Node{
			Name: a.Node,
			Addr: a.Addr,
			Port: a.Port,
			Meta: a.Meta,
			PMin: a.Vsn[0],
			PMax: a.Vsn[1],
			PCur: a.Vsn[2],
			DMin: a.Vsn[3],
			DMax: a.Vsn[4],
			DCur: a.Vsn[5],
		}
		if err := g.config.Alive.NotifyAlive(node); err != nil {
			g.logger.Printf("[WARN] gossip: ignoring alive message for '%s': %s",
				a.Node, err)
			return
		}
	}

	// Check if we've never seen this node before, and if not, then
	// store this node in our node map.
	var updatesNode bool
	if !ok {
		state = &nodeState{
			Node: Node{
				Name: a.Node,
				Addr: a.Addr,
				Port: a.Port,
				Meta: a.Meta,
			},
			State: stateDead,
		}
		if len(a.Vsn) > 5 {
			state.PMin = a.Vsn[0]
			state.PMax = a.Vsn[1]
			state.PCur = a.Vsn[2]
			state.DMin = a.Vsn[3]
			state.DMax = a.Vsn[4]
			state.DCur = a.Vsn[5]
		}

		// Add to map
		g.nodeMap[a.Node] = state

		// Get a random offset. This is important to ensure
		// the failure detection bound is low on average. If all
		// nodes did an append, failure detection bound would be
		// very high.
		n := len(g.nodes)
		offset := randomOffset(n)

		// Add at the end and swap with the node at the offset
		g.nodes = append(g.nodes, state)
		g.nodes[offset], g.nodes[n] = g.nodes[n], g.nodes[offset]

		// Update numNodes after we've added a new node
		atomic.AddUint32(&g.numNodes, 1)
	} else {
		// Check if this address is different than the existing node unless the old node is dead.
		if !bytes.Equal([]byte(state.Addr), a.Addr) || state.Port != a.Port {
			// If DeadNodeReclaimTime is configured, check if enough time has elapsed since the node died.
			canReclaim := (g.config.DeadNodeReclaimTime > 0 &&
				time.Since(state.StateChange) > g.config.DeadNodeReclaimTime)

			// Allow the address to be updated if a dead node is being replaced.
			if state.State == stateDead && canReclaim {
				g.logger.Printf("[INFO] gossip: Updating address for failed node %s from %v:%d to %v:%d",
					state.Name, state.Addr, state.Port, net.IP(a.Addr), a.Port)
				updatesNode = true
			} else {
				g.logger.Printf("[ERR] gossip: Conflicting address for %s. Mine: %v:%d Theirs: %v:%d Old state: %v",
					state.Name, state.Addr, state.Port, net.IP(a.Addr), a.Port, state.State)

				// Inform the conflict delegate if provided
				if g.config.Conflict != nil {
					other := Node{
						Name: a.Node,
						Addr: a.Addr,
						Port: a.Port,
						Meta: a.Meta,
					}
					g.config.Conflict.NotifyConflict(&state.Node, &other)
				}
				return
			}
		}
	}

	// Bail if the incarnation number is older, and this is not about us
	isLocalNode := state.Name == g.config.Name
	if a.Incarnation <= state.Incarnation && !isLocalNode && !updatesNode {
		return
	}

	// Bail if strictly less and this is about us
	if a.Incarnation < state.Incarnation && isLocalNode {
		return
	}

	// Clear out any suspicion timer that may be in effect.
	delete(g.nodeTimers, a.Node)

	// Store the old state and meta data
	oldState := state.State
	oldMeta := state.Meta

	// If this is us we need to refute, otherwise re-broadcast
	if !bootstrap && isLocalNode {
		// Compute the version vector
		versions := []uint8{
			state.PMin, state.PMax, state.PCur,
			state.DMin, state.DMax, state.DCur,
		}

		// If the Incarnation is the same, we need special handling, since it
		// possible for the following situation to happen:
		// 1) Start with configuration C, join cluster
		// 2) Hard fail / Kill / Shutdown
		// 3) Restart with configuration C', join cluster
		//
		// In this case, other nodes and the local node see the same incarnation,
		// but the values may not be the same. For this reason, we always
		// need to do an equality check for this Incarnation. In most cases,
		// we just ignore, but we may need to refute.
		//
		if a.Incarnation == state.Incarnation &&
			bytes.Equal(a.Meta, state.Meta) &&
			bytes.Equal(a.Vsn, versions) {
			return
		}
		g.refute(state, a.Incarnation)
		g.logger.Printf("[WARN] gossip: Refuting an alive message for '%s' (%v:%d) meta:(%v VS %v), vsn:(%v VS %v)", a.Node, net.IP(a.Addr), a.Port, a.Meta, state.Meta, a.Vsn, versions)
	} else {
		g.encodeBroadcastNotify(a.Node, aliveMsg, a, notify)

		// Update protocol versions if it arrived
		if len(a.Vsn) > 0 {
			state.PMin = a.Vsn[0]
			state.PMax = a.Vsn[1]
			state.PCur = a.Vsn[2]
			state.DMin = a.Vsn[3]
			state.DMax = a.Vsn[4]
			state.DCur = a.Vsn[5]
		}

		// Update the state and incarnation number
		state.Incarnation = a.Incarnation
		state.Meta = a.Meta
		state.Addr = a.Addr
		state.Port = a.Port
		if state.State != stateAlive {
			state.State = stateAlive
			state.StateChange = time.Now()
		}
	}

	// Update metrics
	metrics.IncrCounter([]string{"gossip", "msg", "alive"}, 1)

	// Notify the delegate of any relevant updates
	if g.config.Events != nil {
		if oldState == stateDead {
			// if Dead -> Alive, notify of join
			g.config.Events.NotifyJoin(&state.Node)

		} else if !bytes.Equal(oldMeta, state.Meta) {
			// if Meta changed, trigger an update notification
			g.config.Events.NotifyUpdate(&state.Node)
		}
	}
}

// suspectNode is invoked by the network layer when we get a message
// about a suspect node
func (g *Gossip) suspectNode(s *suspect) {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()
	state, ok := g.nodeMap[s.Node]

	// If we've never heard about this node before, ignore it
	if !ok {
		return
	}

	// Ignore old incarnation numbers
	if s.Incarnation < state.Incarnation {
		return
	}

	// See if there's a suspicion timer we can confirg. If the info is new
	// to us we will go ahead and re-gossip it. This allows for multiple
	// independent confirmations to flow even when a node probes a node
	// that's already suspect.
	if timer, ok := g.nodeTimers[s.Node]; ok {
		if timer.Confirm(s.From) {
			g.encodeAndBroadcast(s.Node, suspectMsg, s)
		}
		return
	}

	// Ignore non-alive nodes
	if state.State != stateAlive {
		return
	}

	// If this is us we need to refute, otherwise re-broadcast
	if state.Name == g.config.Name {
		g.refute(state, s.Incarnation)
		g.logger.Printf("[WARN] gossip: Refuting a suspect message (from: %s)", s.From)
		return // Do not mark ourself suspect
	} else {
		g.encodeAndBroadcast(s.Node, suspectMsg, s)
	}

	// Update metrics
	metrics.IncrCounter([]string{"gossip", "msg", "suspect"}, 1)

	// Update the state
	state.Incarnation = s.Incarnation
	state.State = stateSuspect
	changeTime := time.Now()
	state.StateChange = changeTime

	// Setup a suspicion timer. Given that we don't have any known phase
	// relationship with our peers, we set up k such that we hit the nominal
	// timeout two probe intervals short of what we expect given the suspicion
	// multiplier.
	k := g.config.SuspicionMult - 2

	// If there aren't enough nodes to give the expected confirmations, just
	// set k to 0 to say that we don't expect any. Note we subtract 2 from n
	// here to take out ourselves and the node being probed.
	n := g.estNumNodes()
	if n-2 < k {
		k = 0
	}

	// Compute the timeouts based on the size of the cluster.
	min := suspicionTimeout(g.config.SuspicionMult, n, g.config.ProbeInterval)
	max := time.Duration(g.config.SuspicionMaxTimeoutMult) * min
	fn := func(numConfirmations int) {
		g.nodeLock.Lock()
		state, ok := g.nodeMap[s.Node]
		timeout := ok && state.State == stateSuspect && state.StateChange == changeTime
		g.nodeLock.Unlock()

		if timeout {
			if k > 0 && numConfirmations < k {
				metrics.IncrCounter([]string{"gossip", "degraded", "timeout"}, 1)
			}

			g.logger.Printf("[INFO] gossip: Marking %s as failed, suspect timeout reached (%d peer confirmations)",
				state.Name, numConfirmations)
			d := dead{Incarnation: state.Incarnation, Node: state.Name, From: g.config.Name}
			g.deadNode(&d)
		}
	}
	g.nodeTimers[s.Node] = newSuspicion(s.From, k, min, max, fn)
}

// deadNode is invoked by the network layer when we get a message
// about a dead node
func (g *Gossip) deadNode(d *dead) {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()
	state, ok := g.nodeMap[d.Node]

	// If we've never heard about this node before, ignore it
	if !ok {
		return
	}

	// Ignore old incarnation numbers
	if d.Incarnation < state.Incarnation {
		return
	}

	// Clear out any suspicion timer that may be in effect.
	delete(g.nodeTimers, d.Node)

	// Ignore if node is already dead
	if state.State == stateDead {
		return
	}

	// Check if this is us
	if state.Name == g.config.Name {
		// If we are not leaving we need to refute
		if !g.hasLeft() {
			g.refute(state, d.Incarnation)
			g.logger.Printf("[WARN] gossip: Refuting a dead message (from: %s)", d.From)
			return // Do not mark ourself dead
		}

		// If we are leaving, we broadcast and wait
		g.encodeBroadcastNotify(d.Node, deadMsg, d, g.leaveBroadcast)
	} else {
		g.encodeAndBroadcast(d.Node, deadMsg, d)
	}

	// Update metrics
	metrics.IncrCounter([]string{"gossip", "msg", "dead"}, 1)

	// Update the state
	state.Incarnation = d.Incarnation
	state.State = stateDead
	state.StateChange = time.Now()

	// Notify of death
	if g.config.Events != nil {
		g.config.Events.NotifyLeave(&state.Node)
	}
}

// mergeState is invoked by the network layer when we get a Push/Pull
// state transfer
func (g *Gossip) mergeState(remote []pushNodeState) {
	for _, r := range remote {
		switch r.State {
		case stateAlive:
			a := alive{
				Incarnation: r.Incarnation,
				Node:        r.Name,
				Addr:        r.Addr,
				Port:        r.Port,
				Meta:        r.Meta,
				Vsn:         r.Vsn,
			}
			g.aliveNode(&a, nil, false)

		case stateDead:
			// If the remote node believes a node is dead, we prefer to
			// suspect that node instead of declaring it dead instantly
			fallthrough
		case stateSuspect:
			s := suspect{Incarnation: r.Incarnation, Node: r.Name, From: g.config.Name}
			g.suspectNode(&s)
		}
	}
}
