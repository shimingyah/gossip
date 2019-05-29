package gossip

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-sockaddr"
	"github.com/miekg/dns"
)

// Gossip basic structure of gossip
type Gossip struct {
	sequenceNum uint32 // Local sequence number
	incarnation uint32 // Local incarnation number
	numNodes    uint32 // Number of known nodes (estimate)
	pushPullReq uint32 // Number of push/pull requests

	config         *Config
	shutdown       int32 // Used as an atomic boolean value
	shutdownCh     chan struct{}
	leave          int32 // Used as an atomic boolean value
	leaveBroadcast chan struct{}

	shutdownLock sync.Mutex // Serializes calls to Shutdown
	leaveLock    sync.Mutex // Serializes calls to Leave

	transport Transport

	handoffCh            chan struct{}
	highPriorityMsgQueue *list.List
	lowPriorityMsgQueue  *list.List
	msgQueueLock         sync.Mutex

	nodeLock   sync.RWMutex
	nodes      []*nodeState          // Known nodes
	nodeMap    map[string]*nodeState // Maps Addr.String() -> NodeState
	nodeTimers map[string]*suspicion // Maps Addr.String() -> suspicion timer
	awareness  *awareness

	tickerLock sync.Mutex
	tickers    []*time.Ticker
	stopTick   chan struct{}
	probeIndex int

	ackLock     sync.Mutex
	ackHandlers map[uint32]*ackHandler

	// The broadcast message is a list of members and does not contain user information.
	// User info requires the user to define a TransmitLimitedQueue to broadcast user info.
	broadcasts *TransmitLimitedQueue

	logger *log.Logger
}

// newGossip creates the network listeners.
// Does not schedule execution of background maintenance.
func newGossip(conf *Config) (*Gossip, error) {
	if conf.ProtocolVersion < ProtocolVersionMin {
		return nil, fmt.Errorf("Protocol version '%d' too low. Must be in range: [%d, %d]",
			conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	} else if conf.ProtocolVersion > ProtocolVersionMax {
		return nil, fmt.Errorf("Protocol version '%d' too high. Must be in range: [%d, %d]",
			conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	}

	if len(conf.SecretKey) > 0 {
		if conf.Keyring == nil {
			keyring, err := NewKeyring(nil, conf.SecretKey)
			if err != nil {
				return nil, err
			}
			conf.Keyring = keyring
		} else {
			if err := conf.Keyring.AddKey(conf.SecretKey); err != nil {
				return nil, err
			}
			if err := conf.Keyring.UseKey(conf.SecretKey); err != nil {
				return nil, err
			}
		}
	}

	if conf.LogOutput != nil && conf.Logger != nil {
		return nil, fmt.Errorf("Cannot specify both LogOutput and Logger. Please choose a single log configuration setting.")
	}

	logDest := conf.LogOutput
	if logDest == nil {
		logDest = os.Stderr
	}

	logger := conf.Logger
	if logger == nil {
		logger = log.New(logDest, "", log.LstdFlags)
	}

	// Set up a network transport by default if a custom one wasn't given
	// by the config.
	transport := conf.Transport
	if transport == nil {
		nc := &NetTransportConfig{
			BindAddrs: []string{conf.BindAddr},
			BindPort:  conf.BindPort,
			Logger:    logger,
		}

		// See comment below for details about the retry in here.
		makeNetRetry := func(limit int) (*NetTransport, error) {
			var err error
			for try := 0; try < limit; try++ {
				var nt *NetTransport
				if nt, err = NewNetTransport(nc); err == nil {
					return nt, nil
				}
				if strings.Contains(err.Error(), "address already in use") {
					logger.Printf("[DEBUG] Gossip: Got bind error: %v", err)
					continue
				}
			}

			return nil, fmt.Errorf("failed to obtain an address: %v", err)
		}

		// The dynamic bind port operation is inherently racy because
		// even though we are using the kernel to find a port for us, we
		// are attempting to bind multiple protocols (and potentially
		// multiple addresses) with the same port number. We build in a
		// few retries here since this often gets transient errors in
		// busy unit tests.
		limit := 1
		if conf.BindPort == 0 {
			limit = 10
		}

		nt, err := makeNetRetry(limit)
		if err != nil {
			return nil, fmt.Errorf("Could not set up network transport: %v", err)
		}
		if conf.BindPort == 0 {
			port := nt.GetAutoBindPort()
			conf.BindPort = port
			conf.AdvertisePort = port
			logger.Printf("[DEBUG] Gossip: Using dynamic bind port %d", port)
		}
		transport = nt
	}

	g := &Gossip{
		config:               conf,
		shutdownCh:           make(chan struct{}),
		leaveBroadcast:       make(chan struct{}, 1),
		transport:            transport,
		handoffCh:            make(chan struct{}, 1),
		highPriorityMsgQueue: list.New(),
		lowPriorityMsgQueue:  list.New(),
		nodeMap:              make(map[string]*nodeState),
		nodeTimers:           make(map[string]*suspicion),
		awareness:            newAwareness(conf.AwarenessMaxMultiplier),
		ackHandlers:          make(map[uint32]*ackHandler),
		broadcasts:           &TransmitLimitedQueue{RetransmitMult: conf.RetransmitMult},
		logger:               logger,
	}
	g.broadcasts.NumNodes = func() int {
		return g.estNumNodes()
	}
	go g.streamListen()
	go g.packetListen()
	go g.packetHandler()
	return g, nil
}

// Create will create a new Gossip using the given configuration.
// This will not connect to any other node (see Join) yet, but will start
// all the listeners to allow other nodes to join this Gossip.
// After creating a Gossip, the configuration given should not be
// modified by the user anymore.
func Create(conf *Config) (*Gossip, error) {
	g, err := newGossip(conf)
	if err != nil {
		return nil, err
	}
	if err := g.setAlive(); err != nil {
		g.Shutdown()
		return nil, err
	}
	g.schedule()
	return g, nil
}

// Join is used to take an existing Gossip and attempt to join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Gossip only contains our own state, so doing this will cause
// remote nodes to become aware of the existence of this node, effectively
// joining the cluster.
//
// This returns the number of hosts successfully contacted and an error if
// none could be reached. If an error is returned, the node did not successfully
// join the cluster.
func (g *Gossip) Join(existing []string) (int, error) {
	numSuccess := 0
	var errs error
	for _, exist := range existing {
		addrs, err := g.resolveAddr(exist)
		if err != nil {
			err = fmt.Errorf("Failed to resolve %s: %v", exist, err)
			errs = multierror.Append(errs, err)
			g.logger.Printf("[WARN] Gossip: %v", err)
			continue
		}

		for _, addr := range addrs {
			hp := joinHostPort(addr.ip.String(), addr.port)
			if err := g.pushPullNode(hp, true); err != nil {
				err = fmt.Errorf("Failed to join %s: %v", addr.ip, err)
				errs = multierror.Append(errs, err)
				g.logger.Printf("[DEBUG] Gossip: %v", err)
				continue
			}
			numSuccess++
		}

	}
	if numSuccess > 0 {
		errs = nil
	}
	return numSuccess, errs
}

// ipPort holds information about a node we want to try to join.
type ipPort struct {
	ip   net.IP
	port uint16
}

// tcpLookupIP is a helper to initiate a TCP-based DNS lookup for the given host.
// The built-in Go resolver will do a UDP lookup first, and will only use TCP if
// the response has the truncate bit set, which isn't common on DNS servers like
// Consul's. By doing the TCP lookup directly, we get the best chance for the
// largest list of hosts to join. Since joins are relatively rare events, it's ok
// to do this rather expensive operation.
func (g *Gossip) tcpLookupIP(host string, defaultPort uint16) ([]ipPort, error) {
	// Don't attempt any TCP lookups against non-fully qualified domain
	// names, since those will likely come from the resolv.conf file.
	if !strings.Contains(host, ".") {
		return nil, nil
	}

	// Make sure the domain name is terminated with a dot (we know there's
	// at least one character at this point).
	dn := host
	if dn[len(dn)-1] != '.' {
		dn = dn + "."
	}

	// See if we can find a server to try.
	cc, err := dns.ClientConfigFromFile(g.config.DNSConfigPath)
	if err != nil {
		return nil, err
	}
	if len(cc.Servers) > 0 {
		// We support host:port in the DNS config, but need to add the
		// default port if one is not supplied.
		server := cc.Servers[0]
		if !hasPort(server) {
			server = net.JoinHostPort(server, cc.Port)
		}

		// Do the lookup.
		c := new(dns.Client)
		c.Net = "tcp"
		msg := new(dns.Msg)
		msg.SetQuestion(dn, dns.TypeANY)
		in, _, err := c.Exchange(msg, server)
		if err != nil {
			return nil, err
		}

		// Handle any IPs we get back that we can attempt to join.
		var ips []ipPort
		for _, r := range in.Answer {
			switch rr := r.(type) {
			case (*dns.A):
				ips = append(ips, ipPort{rr.A, defaultPort})
			case (*dns.AAAA):
				ips = append(ips, ipPort{rr.AAAA, defaultPort})
			case (*dns.CNAME):
				g.logger.Printf("[DEBUG] Gossip: Ignoring CNAME RR in TCP-first answer for '%s'", host)
			}
		}
		return ips, nil
	}

	return nil, nil
}

// resolveAddr is used to resolve the address into an address,
// port, and error. If no port is given, use the default
func (g *Gossip) resolveAddr(hostStr string) ([]ipPort, error) {
	// This captures the supplied port, or the default one.
	hostStr = ensurePort(hostStr, g.config.BindPort)
	host, sport, err := net.SplitHostPort(hostStr)
	if err != nil {
		return nil, err
	}
	lport, err := strconv.ParseUint(sport, 10, 16)
	if err != nil {
		return nil, err
	}
	port := uint16(lport)

	// If it looks like an IP address we are done. The SplitHostPort() above
	// will make sure the host part is in good shape for parsing, even for
	// IPv6 addresses.
	if ip := net.ParseIP(host); ip != nil {
		return []ipPort{ipPort{ip, port}}, nil
	}

	// First try TCP so we have the best chance for the largest list of
	// hosts to join. If this fails it's not fatal since this isn't a standard
	// way to query DNS, and we have a fallback below.
	ips, err := g.tcpLookupIP(host, port)
	if err != nil {
		g.logger.Printf("[DEBUG] Gossip: TCP-first lookup failed for '%s', falling back to UDP: %s", hostStr, err)
	}
	if len(ips) > 0 {
		return ips, nil
	}

	// If TCP didn't yield anything then use the normal Go resolver which
	// will try UDP, then might possibly try TCP again if the UDP response
	// indicates it was truncated.
	ans, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	ips = make([]ipPort, 0, len(ans))
	for _, ip := range ans {
		ips = append(ips, ipPort{ip, port})
	}
	return ips, nil
}

// setAlive is used to mark this node as being alive. This is the same
// as if we received an alive notification our own network channel for
// ourself.
func (g *Gossip) setAlive() error {
	// Get the final advertise address from the transport, which may need
	// to see which address we bound to.
	addr, port, err := g.transport.FinalAdvertiseAddr(
		g.config.AdvertiseAddr, g.config.AdvertisePort)
	if err != nil {
		return fmt.Errorf("Failed to get final advertise address: %v", err)
	}

	// Check if this is a public address without encryption
	ipAddr, err := sockaddr.NewIPAddr(addr.String())
	if err != nil {
		return fmt.Errorf("Failed to parse interface addresses: %v", err)
	}
	ifAddrs := []sockaddr.IfAddr{
		sockaddr.IfAddr{
			SockAddr: ipAddr,
		},
	}
	_, publicIfs, err := sockaddr.IfByRFC("6890", ifAddrs)
	if len(publicIfs) > 0 && !g.config.EncryptionEnabled() {
		g.logger.Printf("[WARN] Gossip: Binding to public address without encryption!")
	}

	// Set any metadata from the delegate.
	var meta []byte
	if g.config.Delegate != nil {
		meta = g.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("Node meta data provided is longer than the limit")
		}
	}

	a := alive{
		Incarnation: g.nextIncarnation(),
		Node:        g.config.Name,
		Addr:        addr,
		Port:        uint16(port),
		Meta:        meta,
		Vsn:         g.config.BuildVsnArray(),
	}
	g.aliveNode(&a, nil, true)
	return nil
}

// LocalNode is used to return the local Node
func (g *Gossip) LocalNode() *Node {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	state := g.nodeMap[g.config.Name]
	return &state.Node
}

// UpdateNode is used to trigger re-advertising the local node. This is
// primarily used with a Delegate to support dynamic updates to the local
// meta data.  This will block until the update message is successfully
// broadcasted to a member of the cluster, if any exist or until a specified
// timeout is reached.
func (g *Gossip) UpdateNode(timeout time.Duration) error {
	// Get the node meta data
	var meta []byte
	if g.config.Delegate != nil {
		meta = g.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("Node meta data provided is longer than the limit")
		}
	}

	// Get the existing node
	g.nodeLock.RLock()
	state := g.nodeMap[g.config.Name]
	g.nodeLock.RUnlock()

	// Format a new alive message
	a := alive{
		Incarnation: g.nextIncarnation(),
		Node:        g.config.Name,
		Addr:        state.Addr,
		Port:        state.Port,
		Meta:        meta,
		Vsn:         g.config.BuildVsnArray(),
	}
	notifyCh := make(chan struct{})
	g.aliveNode(&a, notifyCh, true)

	// Wait for the broadcast or a timeout
	if g.anyAlive() {
		var timeoutCh <-chan time.Time
		if timeout > 0 {
			timeoutCh = time.After(timeout)
		}
		select {
		case <-notifyCh:
		case <-timeoutCh:
			return fmt.Errorf("timeout waiting for update broadcast")
		}
	}
	return nil
}

// SendTo is deprecated in favor of SendBestEffort, which requires a node to
// target.
func (g *Gossip) SendTo(to net.Addr, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	// Send the message
	return g.rawSendMsgPacket(to.String(), nil, buf)
}

// SendToUDP is deprecated in favor of SendBestEffort.
func (g *Gossip) SendToUDP(to *Node, msg []byte) error {
	return g.SendBestEffort(to, msg)
}

// SendToTCP is deprecated in favor of SendReliable.
func (g *Gossip) SendToTCP(to *Node, msg []byte) error {
	return g.SendReliable(to, msg)
}

// SendBestEffort uses the unreliable packet-oriented interface of the transport
// to target a user message at the given node (this does not use the gossip
// mechanism). The maximum size of the message depends on the configured
// UDPBufferSize for this Gossip instance.
func (g *Gossip) SendBestEffort(to *Node, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	// Send the message
	return g.rawSendMsgPacket(to.Address(), to, buf)
}

// SendReliable uses the reliable stream-oriented interface of the transport to
// target a user message at the given node (this does not use the gossip
// mechanism). Delivery is guaranteed if no error is returned, and there is no
// limit on the size of the message.
func (g *Gossip) SendReliable(to *Node, msg []byte) error {
	return g.sendUserMsg(to.Address(), msg)
}

// Members returns a list of all known live nodes. The node structures
// returned must not be modified. If you wish to modify a Node, make a
// copy first.
func (g *Gossip) Members() []*Node {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()

	nodes := make([]*Node, 0, len(g.nodes))
	for _, n := range g.nodes {
		if n.State != stateDead {
			nodes = append(nodes, &n.Node)
		}
	}

	return nodes
}

// NumMembers returns the number of alive nodes currently known. Between
// the time of calling this and calling Members, the number of alive nodes
// may have changed, so this shouldn't be used to determine how many
// members will be returned by Members.
func (g *Gossip) NumMembers() (alive int) {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()

	for _, n := range g.nodes {
		if n.State != stateDead {
			alive++
		}
	}

	return
}

// Leave will broadcast a leave message but will not shutdown the background
// listeners, meaning the node will continue participating in gossip and state
// updates.
//
// This will block until the leave message is successfully broadcasted to
// a member of the cluster, if any exist or until a specified timeout
// is reached.
//
// This method is safe to call multiple times, but must not be called
// after the cluster is already shut down.
func (g *Gossip) Leave(timeout time.Duration) error {
	g.leaveLock.Lock()
	defer g.leaveLock.Unlock()

	if g.hasShutdown() {
		panic("leave after shutdown")
	}

	if !g.hasLeft() {
		atomic.StoreInt32(&g.leave, 1)

		g.nodeLock.Lock()
		state, ok := g.nodeMap[g.config.Name]
		g.nodeLock.Unlock()
		if !ok {
			g.logger.Printf("[WARN] Gossip: Leave but we're not in the node map.")
			return nil
		}

		d := dead{
			Incarnation: state.Incarnation,
			Node:        state.Name,
		}
		g.deadNode(&d)

		// Block until the broadcast goes out
		if g.anyAlive() {
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}
			select {
			case <-g.leaveBroadcast:
			case <-timeoutCh:
				return fmt.Errorf("timeout waiting for leave broadcast")
			}
		}
	}

	return nil
}

// Check for any other alive node.
func (g *Gossip) anyAlive() bool {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	for _, n := range g.nodes {
		if n.State != stateDead && n.Name != g.config.Name {
			return true
		}
	}
	return false
}

// GetHealthScore gives this instance's idea of how well it is meeting the soft
// real-time requirements of the protocol. Lower numbers are better, and zero
// means "totally healthy".
func (g *Gossip) GetHealthScore() int {
	return g.awareness.GetHealthScore()
}

// ProtocolVersion returns the protocol version currently in use by
// this Gossip.
func (g *Gossip) ProtocolVersion() uint8 {
	// NOTE: This method exists so that in the future we can control
	// any locking if necessary, if we change the protocol version at
	// runtime, etc.
	return g.config.ProtocolVersion
}

// encodeAndBroadcast encodes a message and enqueues it for broadcast. Fails
// silently if there is an encoding error.
func (g *Gossip) encodeAndBroadcast(node string, msgType messageType, msg interface{}) {
	g.encodeBroadcastNotify(node, msgType, msg, nil)
}

// encodeBroadcastNotify encodes a message and enqueues it for broadcast
// and notifies the given channel when transmission is finished. Fails
// silently if there is an encoding error.
func (g *Gossip) encodeBroadcastNotify(node string, msgType messageType, msg interface{}, notify chan struct{}) {
	buf, err := encode(msgType, msg)
	if err != nil {
		g.logger.Printf("[ERR] gossip: Failed to encode message for broadcast: %s", err)
	} else {
		g.queueBroadcast(node, buf.Bytes(), notify)
	}
}

// queueBroadcast is used to start dissemination of a message. It will be
// sent up to a configured number of times. The message could potentially
// be invalidated by a future message about the same node
func (g *Gossip) queueBroadcast(node string, msg []byte, notify chan struct{}) {
	b := &gossipBroadcast{node, msg, notify}
	g.broadcasts.QueueBroadcast(b)
}

// getBroadcasts is used to return a slice of broadcasts to send up to
// a maximum byte size, while imposing a per-broadcast overhead. This is used
// to fill a UDP packet with piggybacked data
func (g *Gossip) getBroadcasts(overhead, limit int) [][]byte {
	// Get gossip messages first
	toSend := g.broadcasts.GetBroadcasts(overhead, limit)

	// Check if the user has anything to broadcast
	d := g.config.Delegate
	if d != nil {
		// Determine the bytes used already
		bytesUsed := 0
		for _, msg := range toSend {
			bytesUsed += len(msg) + overhead
		}

		// Check space remaining for user messages
		avail := limit - bytesUsed
		if avail > overhead+userMsgOverhead {
			userMsgs := d.GetBroadcasts(overhead+userMsgOverhead, avail)

			// Frame each user message
			for _, msg := range userMsgs {
				buf := make([]byte, 1, len(msg)+1)
				buf[0] = byte(userMsg)
				buf = append(buf, msg...)
				toSend = append(toSend, buf)
			}
		}
	}
	return toSend
}

// Shutdown will stop any background maintanence of network activity
// for this Gossip, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// This method is safe to call multiple times.
func (g *Gossip) Shutdown() error {
	g.shutdownLock.Lock()
	defer g.shutdownLock.Unlock()

	if g.hasShutdown() {
		return nil
	}

	// Shut down the transport first, which should block until it's
	// completely torn down. If we kill the Gossip-side handlers
	// those I/O handlers might get stuck.
	if err := g.transport.Shutdown(); err != nil {
		g.logger.Printf("[ERR] Failed to shutdown transport: %v", err)
	}

	// Now tear down everything else.
	atomic.StoreInt32(&g.shutdown, 1)
	close(g.shutdownCh)
	g.deschedule()
	return nil
}

func (g *Gossip) hasShutdown() bool {
	return atomic.LoadInt32(&g.shutdown) == 1
}

func (g *Gossip) hasLeft() bool {
	return atomic.LoadInt32(&g.leave) == 1
}

func (g *Gossip) getNodeState(addr string) nodeStateType {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()

	n := g.nodeMap[addr]
	return n.State
}

func (g *Gossip) getNodeStateChange(addr string) time.Time {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()

	n := g.nodeMap[addr]
	return n.StateChange
}

func (g *Gossip) changeNode(addr string, f func(*nodeState)) {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()

	n := g.nodeMap[addr]
	f(n)
}
