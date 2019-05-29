package gossip

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/go-msgpack/codec"
)

// This is the minimum and maximum protocol version that we can
// _understand_. We're allowed to speak at any version within this
// range. This range is inclusive.
const (
	ProtocolVersionMin uint8 = 1

	// Version 3 added support for TCP pings but we kept the default
	// protocol version at 2 to ease transition to this new feature.
	// A gossip speaking version 2 of the protocol will attempt
	// to TCP ping another gossip who understands version 3 or
	// greater.
	//
	// Version 4 added support for nacks as part of indirect probes.
	// A gossip speaking version 2 of the protocol will expect
	// nacks from another gossip who understands version 4 or
	// greater, and likewise nacks will be sent to gossips who
	// understand version 4 or greater.
	ProtocolVersion2Compatible = 2

	ProtocolVersionMax = 5
)

// messageType is an integer ID of a type of message that can be received
// on network channels from other members.
type messageType uint8

// The list of available message types.
const (
	pingMsg messageType = iota
	indirectPingMsg
	ackRespMsg
	suspectMsg
	aliveMsg
	deadMsg
	pushPullMsg
	compoundMsg
	userMsg // User mesg, not handled by us
	compressMsg
	encryptMsg
	nackRespMsg
	hasCrcMsg
	errMsg
)

// compressionType is used to specify the compression algorithm
type compressionType uint8

const (
	lzwAlgo compressionType = iota
)

const (
	// MetaMaxSize user defined node meta info max size
	MetaMaxSize            = 512 // Maximum size for node meta data
	compoundHeaderOverhead = 2   // Assumed header overhead
	compoundOverhead       = 2   // Assumed overhead per entry in compoundHeader
	userMsgOverhead        = 1
	blockingWarning        = 10 * time.Millisecond // Warn if a UDP packet takes this long to process
	maxPushStateBytes      = 20 * 1024 * 1024
	maxPushPullRequests    = 128 // Maximum number of concurrent push/pull requests
)

// ping request sent directly to node
type ping struct {
	SeqNo uint32

	// Node is sent so the target can verify they are
	// the intended recipient. This is to protect again an agent
	// restart with a new name.
	Node string
}

// indirect ping sent to an indirect ndoe
type indirectPingReq struct {
	SeqNo  uint32
	Target []byte
	Port   uint16
	Node   string
	Nack   bool // true if we'd like a nack back
}

// ack response is sent for a ping
type ackResp struct {
	SeqNo   uint32
	Payload []byte
}

// nack response is sent for an indirect ping when the pinger doesn't hear from
// the ping-ee within the configured timeout. This lets the original node know
// that the indirect ping attempt happened but didn't succeed.
type nackResp struct {
	SeqNo uint32
}

// err response is sent to relay the error from the remote end
type errResp struct {
	Error string
}

// suspect is broadcast when we suspect a node is dead
type suspect struct {
	Incarnation uint32
	Node        string
	From        string // Include who is suspecting
}

// alive is broadcast when we know a node is alive.
// Overloaded for nodes joining
type alive struct {
	Incarnation uint32
	Node        string
	Addr        []byte
	Port        uint16
	Meta        []byte

	// The versions of the protocol/delegate that are being spoken, order:
	// pmin, pmax, pcur, dmin, dmax, dcur
	Vsn []uint8
}

// dead is broadcast when we confirm a node is dead
// Overloaded for nodes leaving
type dead struct {
	Incarnation uint32
	Node        string
	From        string // Include who is suspecting
}

// pushPullHeader is used to inform the
// otherside how many states we are transferring
type pushPullHeader struct {
	Nodes        int
	UserStateLen int  // Encodes the byte lengh of user state
	Join         bool // Is this a join request or a anti-entropy run
}

// userMsgHeader is used to encapsulate a userMsg
type userMsgHeader struct {
	UserMsgLen int // Encodes the byte lengh of user state
}

// pushNodeState is used for pushPullReq when we are
// transferring out node states
type pushNodeState struct {
	Name        string
	Addr        []byte
	Port        uint16
	Meta        []byte
	Incarnation uint32
	State       nodeStateType
	Vsn         []uint8 // Protocol versions
}

// compress is used to wrap an underlying payload
// using a specified compression algorithm
type compress struct {
	Algo compressionType
	Buf  []byte
}

// msgHandoff is used to transfer a message between goroutines
type msgHandoff struct {
	msgType messageType
	buf     []byte
	from    net.Addr
}

// encryptionVersion returns the encryption version to use
func (g *Gossip) encryptionVersion() encryptionVersion {
	switch g.ProtocolVersion() {
	case 1:
		return 0
	default:
		return 1
	}
}

// streamListen is a long running goroutine that pulls incoming streams from the
// transport and hands them off for processing.
func (g *Gossip) streamListen() {
	for {
		select {
		case conn := <-g.transport.StreamCh():
			go g.handleConn(conn)

		case <-g.shutdownCh:
			return
		}
	}
}

// handleConn handles a single incoming stream connection from the transport.
func (g *Gossip) handleConn(conn net.Conn) {
	g.logger.Printf("[DEBUG] gossip: Stream connection %s", LogConn(conn))

	defer conn.Close()
	metrics.IncrCounter([]string{"gossip", "tcp", "accept"}, 1)

	conn.SetDeadline(time.Now().Add(g.config.TCPTimeout))
	msgType, bufConn, dec, err := g.readStream(conn)
	if err != nil {
		if err != io.EOF {
			g.logger.Printf("[ERR] gossip: failed to receive: %s %s", err, LogConn(conn))

			resp := errResp{err.Error()}
			out, err := encode(errMsg, &resp)
			if err != nil {
				g.logger.Printf("[ERR] gossip: Failed to encode error response: %s", err)
				return
			}

			err = g.rawSendMsgStream(conn, out.Bytes())
			if err != nil {
				g.logger.Printf("[ERR] gossip: Failed to send error: %s %s", err, LogConn(conn))
				return
			}
		}
		return
	}

	switch msgType {
	case userMsg:
		if err := g.readUserMsg(bufConn, dec); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to receive user message: %s %s", err, LogConn(conn))
		}
	case pushPullMsg:
		// Increment counter of pending push/pulls
		numConcurrent := atomic.AddUint32(&g.pushPullReq, 1)
		defer atomic.AddUint32(&g.pushPullReq, ^uint32(0))

		// Check if we have too many open push/pull requests
		if numConcurrent >= maxPushPullRequests {
			g.logger.Printf("[ERR] gossip: Too many pending push/pull requests")
			return
		}

		join, remoteNodes, userState, err := g.readRemoteState(bufConn, dec)
		if err != nil {
			g.logger.Printf("[ERR] gossip: Failed to read remote state: %s %s", err, LogConn(conn))
			return
		}

		if err := g.sendLocalState(conn, join); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to push local state: %s %s", err, LogConn(conn))
			return
		}

		if err := g.mergeRemoteState(join, remoteNodes, userState); err != nil {
			g.logger.Printf("[ERR] gossip: Failed push/pull merge: %s %s", err, LogConn(conn))
			return
		}
	case pingMsg:
		var p ping
		if err := dec.Decode(&p); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to decode ping: %s %s", err, LogConn(conn))
			return
		}

		if p.Node != "" && p.Node != g.config.Name {
			g.logger.Printf("[WARN] gossip: Got ping for unexpected node %s %s", p.Node, LogConn(conn))
			return
		}

		ack := ackResp{p.SeqNo, nil}
		out, err := encode(ackRespMsg, &ack)
		if err != nil {
			g.logger.Printf("[ERR] gossip: Failed to encode ack: %s", err)
			return
		}

		err = g.rawSendMsgStream(conn, out.Bytes())
		if err != nil {
			g.logger.Printf("[ERR] gossip: Failed to send ack: %s %s", err, LogConn(conn))
			return
		}
	default:
		g.logger.Printf("[ERR] gossip: Received invalid msgType (%d) %s", msgType, LogConn(conn))
	}
}

// packetListen is a long running goroutine that pulls packets out of the
// transport and hands them off for processing.
func (g *Gossip) packetListen() {
	for {
		select {
		case packet := <-g.transport.PacketCh():
			g.ingestPacket(packet.Buf, packet.From, packet.Timestamp)

		case <-g.shutdownCh:
			return
		}
	}
}

func (g *Gossip) ingestPacket(buf []byte, from net.Addr, timestamp time.Time) {
	// Check if encryption is enabled
	if g.config.EncryptionEnabled() {
		// Decrypt the payload
		plain, err := decryptPayload(g.config.Keyring.GetKeys(), buf, nil)
		if err != nil {
			if !g.config.GossipVerifyIncoming {
				// Treat the message as plaintext
				plain = buf
			} else {
				g.logger.Printf("[ERR] gossip: Decrypt packet failed: %v %s", err, LogAddress(from))
				return
			}
		}

		// Continue processing the plaintext buffer
		buf = plain
	}

	// See if there's a checksum included to verify the contents of the message
	if len(buf) >= 5 && messageType(buf[0]) == hasCrcMsg {
		crc := crc32.ChecksumIEEE(buf[5:])
		expected := binary.BigEndian.Uint32(buf[1:5])
		if crc != expected {
			g.logger.Printf("[WARN] gossip: Got invalid checksum for UDP packet: %x, %x", crc, expected)
			return
		}
		g.handleCommand(buf[5:], from, timestamp)
	} else {
		g.handleCommand(buf, from, timestamp)
	}
}

func (g *Gossip) handleCommand(buf []byte, from net.Addr, timestamp time.Time) {
	// Decode the message type
	msgType := messageType(buf[0])
	buf = buf[1:]

	// Switch on the msgType
	switch msgType {
	case compoundMsg:
		g.handleCompound(buf, from, timestamp)
	case compressMsg:
		g.handleCompressed(buf, from, timestamp)

	case pingMsg:
		g.handlePing(buf, from)
	case indirectPingMsg:
		g.handleIndirectPing(buf, from)
	case ackRespMsg:
		g.handleAck(buf, from, timestamp)
	case nackRespMsg:
		g.handleNack(buf, from)

	case suspectMsg:
		fallthrough
	case aliveMsg:
		fallthrough
	case deadMsg:
		fallthrough
	case userMsg:
		// Determine the message queue, prioritize alive
		queue := g.lowPriorityMsgQueue
		if msgType == aliveMsg {
			queue = g.highPriorityMsgQueue
		}

		// Check for overflow and append if not full
		g.msgQueueLock.Lock()
		if queue.Len() >= g.config.HandoffQueueDepth {
			g.logger.Printf("[WARN] gossip: handler queue full, dropping message (%d) %s", msgType, LogAddress(from))
		} else {
			queue.PushBack(msgHandoff{msgType, buf, from})
		}
		g.msgQueueLock.Unlock()

		// Notify of pending message
		select {
		case g.handoffCh <- struct{}{}:
		default:
		}

	default:
		g.logger.Printf("[ERR] gossip: msg type (%d) not supported %s", msgType, LogAddress(from))
	}
}

// getNextMessage returns the next message to process in priority order, using LIFO
func (g *Gossip) getNextMessage() (msgHandoff, bool) {
	g.msgQueueLock.Lock()
	defer g.msgQueueLock.Unlock()

	if el := g.highPriorityMsgQueue.Back(); el != nil {
		g.highPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	} else if el := g.lowPriorityMsgQueue.Back(); el != nil {
		g.lowPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	}
	return msgHandoff{}, false
}

// packetHandler is a long running goroutine that processes messages received
// over the packet interface, but is decoupled from the listener to avoid
// blocking the listener which may cause ping/ack messages to be delayed.
func (g *Gossip) packetHandler() {
	for {
		select {
		case <-g.handoffCh:
			for {
				msg, ok := g.getNextMessage()
				if !ok {
					break
				}
				msgType := msg.msgType
				buf := msg.buf
				from := msg.from

				switch msgType {
				case suspectMsg:
					g.handleSuspect(buf, from)
				case aliveMsg:
					g.handleAlive(buf, from)
				case deadMsg:
					g.handleDead(buf, from)
				case userMsg:
					g.handleUser(buf, from)
				default:
					g.logger.Printf("[ERR] gossip: Message type (%d) not supported %s (packet handler)", msgType, LogAddress(from))
				}
			}

		case <-g.shutdownCh:
			return
		}
	}
}

func (g *Gossip) handleCompound(buf []byte, from net.Addr, timestamp time.Time) {
	// Decode the parts
	trunc, parts, err := decodeCompoundMessage(buf)
	if err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode compound request: %s %s", err, LogAddress(from))
		return
	}

	// Log any truncation
	if trunc > 0 {
		g.logger.Printf("[WARN] gossip: Compound request had %d truncated messages %s", trunc, LogAddress(from))
	}

	// Handle each message
	for _, part := range parts {
		g.handleCommand(part, from, timestamp)
	}
}

func (g *Gossip) handlePing(buf []byte, from net.Addr) {
	var p ping
	if err := decode(buf, &p); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode ping request: %s %s", err, LogAddress(from))
		return
	}
	// If node is provided, verify that it is for us
	if p.Node != "" && p.Node != g.config.Name {
		g.logger.Printf("[WARN] gossip: Got ping for unexpected node '%s' %s", p.Node, LogAddress(from))
		return
	}
	var ack ackResp
	ack.SeqNo = p.SeqNo
	if g.config.Ping != nil {
		ack.Payload = g.config.Ping.AckPayload()
	}
	if err := g.encodeAndSendMsg(from.String(), ackRespMsg, &ack); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to send ack: %s %s", err, LogAddress(from))
	}
}

func (g *Gossip) handleIndirectPing(buf []byte, from net.Addr) {
	var ind indirectPingReq
	if err := decode(buf, &ind); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode indirect ping request: %s %s", err, LogAddress(from))
		return
	}

	// For proto versions < 2, there is no port provided. Mask old
	// behavior by using the configured port.
	if g.ProtocolVersion() < 2 || ind.Port == 0 {
		ind.Port = uint16(g.config.BindPort)
	}

	// Send a ping to the correct host.
	localSeqNo := g.nextSeqNo()
	ping := ping{SeqNo: localSeqNo, Node: ind.Node}

	// Setup a response handler to relay the ack
	cancelCh := make(chan struct{})
	respHandler := func(payload []byte, timestamp time.Time) {
		// Try to prevent the nack if we've caught it in time.
		close(cancelCh)

		// Forward the ack back to the requestor.
		ack := ackResp{ind.SeqNo, nil}
		if err := g.encodeAndSendMsg(from.String(), ackRespMsg, &ack); err != nil {
			g.logger.Printf("[ERR] gossip: Failed to forward ack: %s %s", err, LogAddress(from))
		}
	}
	g.setAckHandler(localSeqNo, respHandler, g.config.ProbeTimeout)

	// Send the ping.
	addr := joinHostPort(net.IP(ind.Target).String(), ind.Port)
	if err := g.encodeAndSendMsg(addr, pingMsg, &ping); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to send ping: %s %s", err, LogAddress(from))
	}

	// Setup a timer to fire off a nack if no ack is seen in time.
	if ind.Nack {
		go func() {
			select {
			case <-cancelCh:
				return
			case <-time.After(g.config.ProbeTimeout):
				nack := nackResp{ind.SeqNo}
				if err := g.encodeAndSendMsg(from.String(), nackRespMsg, &nack); err != nil {
					g.logger.Printf("[ERR] gossip: Failed to send nack: %s %s", err, LogAddress(from))
				}
			}
		}()
	}
}

func (g *Gossip) handleAck(buf []byte, from net.Addr, timestamp time.Time) {
	var ack ackResp
	if err := decode(buf, &ack); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode ack response: %s %s", err, LogAddress(from))
		return
	}
	g.invokeAckHandler(ack, timestamp)
}

func (g *Gossip) handleNack(buf []byte, from net.Addr) {
	var nack nackResp
	if err := decode(buf, &nack); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode nack response: %s %s", err, LogAddress(from))
		return
	}
	g.invokeNackHandler(nack)
}

func (g *Gossip) handleSuspect(buf []byte, from net.Addr) {
	var sus suspect
	if err := decode(buf, &sus); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode suspect message: %s %s", err, LogAddress(from))
		return
	}
	g.suspectNode(&sus)
}

func (g *Gossip) handleAlive(buf []byte, from net.Addr) {
	var live alive
	if err := decode(buf, &live); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode alive message: %s %s", err, LogAddress(from))
		return
	}

	// For proto versions < 2, there is no port provided. Mask old
	// behavior by using the configured port
	if g.ProtocolVersion() < 2 || live.Port == 0 {
		live.Port = uint16(g.config.BindPort)
	}

	g.aliveNode(&live, nil, false)
}

func (g *Gossip) handleDead(buf []byte, from net.Addr) {
	var d dead
	if err := decode(buf, &d); err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decode dead message: %s %s", err, LogAddress(from))
		return
	}
	g.deadNode(&d)
}

// handleUser is used to notify channels of incoming user data
func (g *Gossip) handleUser(buf []byte, from net.Addr) {
	d := g.config.Delegate
	if d != nil {
		d.NotifyMsg(buf)
	}
}

// handleCompressed is used to unpack a compressed message
func (g *Gossip) handleCompressed(buf []byte, from net.Addr, timestamp time.Time) {
	// Try to decode the payload
	payload, err := decompressPayload(buf)
	if err != nil {
		g.logger.Printf("[ERR] gossip: Failed to decompress payload: %v %s", err, LogAddress(from))
		return
	}

	// Recursively handle the payload
	g.handleCommand(payload, from, timestamp)
}

// encodeAndSendMsg is used to combine the encoding and sending steps
func (g *Gossip) encodeAndSendMsg(addr string, msgType messageType, msg interface{}) error {
	out, err := encode(msgType, msg)
	if err != nil {
		return err
	}
	if err := g.sendMsg(addr, out.Bytes()); err != nil {
		return err
	}
	return nil
}

// sendMsg is used to send a message via packet to another host. It will
// opportunistically create a compoundMsg and piggy back other broadcasts.
func (g *Gossip) sendMsg(addr string, msg []byte) error {
	// Check if we can piggy back any messages
	bytesAvail := g.config.UDPBufferSize - len(msg) - compoundHeaderOverhead
	if g.config.EncryptionEnabled() && g.config.GossipVerifyOutgoing {
		bytesAvail -= encryptOverhead(g.encryptionVersion())
	}
	extra := g.getBroadcasts(compoundOverhead, bytesAvail)

	// Fast path if nothing to piggypack
	if len(extra) == 0 {
		return g.rawSendMsgPacket(addr, nil, msg)
	}

	// Join all the messages
	msgs := make([][]byte, 0, 1+len(extra))
	msgs = append(msgs, msg)
	msgs = append(msgs, extra...)

	// Create a compound message
	compound := makeCompoundMessage(msgs)

	// Send the message
	return g.rawSendMsgPacket(addr, nil, compound.Bytes())
}

// rawSendMsgPacket is used to send message via packet to another host without
// modification, other than compression or encryption if enabled.
func (g *Gossip) rawSendMsgPacket(addr string, node *Node, msg []byte) error {
	// Check if we have compression enabled
	if g.config.EnableCompression {
		buf, err := compressPayload(msg)
		if err != nil {
			g.logger.Printf("[WARN] gossip: Failed to compress payload: %v", err)
		} else {
			// Only use compression if it reduced the size
			if buf.Len() < len(msg) {
				msg = buf.Bytes()
			}
		}
	}

	// Try to look up the destination node
	if node == nil {
		toAddr, _, err := net.SplitHostPort(addr)
		if err != nil {
			g.logger.Printf("[ERR] gossip: Failed to parse address %q: %v", addr, err)
			return err
		}
		g.nodeLock.RLock()
		nodeState, ok := g.nodeMap[toAddr]
		g.nodeLock.RUnlock()
		if ok {
			node = &nodeState.Node
		}
	}

	// Add a CRC to the end of the payload if the recipient understands
	// ProtocolVersion >= 5
	if node != nil && node.PMax >= 5 {
		crc := crc32.ChecksumIEEE(msg)
		header := make([]byte, 5, 5+len(msg))
		header[0] = byte(hasCrcMsg)
		binary.BigEndian.PutUint32(header[1:], crc)
		msg = append(header, msg...)
	}

	// Check if we have encryption enabled
	if g.config.EncryptionEnabled() && g.config.GossipVerifyOutgoing {
		// Encrypt the payload
		var buf bytes.Buffer
		primaryKey := g.config.Keyring.GetPrimaryKey()
		err := encryptPayload(g.encryptionVersion(), primaryKey, msg, nil, &buf)
		if err != nil {
			g.logger.Printf("[ERR] gossip: Encryption of message failed: %v", err)
			return err
		}
		msg = buf.Bytes()
	}

	metrics.IncrCounter([]string{"gossip", "udp", "sent"}, float32(len(msg)))
	_, err := g.transport.WriteTo(msg, addr)
	return err
}

// rawSendMsgStream is used to stream a message to another host without
// modification, other than applying compression and encryption if enabled.
func (g *Gossip) rawSendMsgStream(conn net.Conn, sendBuf []byte) error {
	// Check if compresion is enabled
	if g.config.EnableCompression {
		compBuf, err := compressPayload(sendBuf)
		if err != nil {
			g.logger.Printf("[ERROR] gossip: Failed to compress payload: %v", err)
		} else {
			sendBuf = compBuf.Bytes()
		}
	}

	// Check if encryption is enabled
	if g.config.EncryptionEnabled() && g.config.GossipVerifyOutgoing {
		crypt, err := g.encryptLocalState(sendBuf)
		if err != nil {
			g.logger.Printf("[ERROR] gossip: Failed to encrypt local state: %v", err)
			return err
		}
		sendBuf = crypt
	}

	// Write out the entire send buffer
	metrics.IncrCounter([]string{"gossip", "tcp", "sent"}, float32(len(sendBuf)))

	if n, err := conn.Write(sendBuf); err != nil {
		return err
	} else if n != len(sendBuf) {
		return fmt.Errorf("only %d of %d bytes written", n, len(sendBuf))
	}

	return nil
}

// sendUserMsg is used to stream a user message to another host.
func (g *Gossip) sendUserMsg(addr string, sendBuf []byte) error {
	conn, err := g.transport.DialTimeout(addr, g.config.TCPTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	bufConn := bytes.NewBuffer(nil)
	if err := bufConn.WriteByte(byte(userMsg)); err != nil {
		return err
	}

	header := userMsgHeader{UserMsgLen: len(sendBuf)}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(bufConn, &hd)
	if err := enc.Encode(&header); err != nil {
		return err
	}
	if _, err := bufConn.Write(sendBuf); err != nil {
		return err
	}
	return g.rawSendMsgStream(conn, bufConn.Bytes())
}

// sendAndReceiveState is used to initiate a push/pull over a stream with a
// remote host.
func (g *Gossip) sendAndReceiveState(addr string, join bool) ([]pushNodeState, []byte, error) {
	// Attempt to connect
	conn, err := g.transport.DialTimeout(addr, g.config.TCPTimeout)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	g.logger.Printf("[DEBUG] gossip: Initiating push/pull sync with: %s", conn.RemoteAddr())
	metrics.IncrCounter([]string{"gossip", "tcp", "connect"}, 1)

	// Send our state
	if err := g.sendLocalState(conn, join); err != nil {
		return nil, nil, err
	}

	conn.SetDeadline(time.Now().Add(g.config.TCPTimeout))
	msgType, bufConn, dec, err := g.readStream(conn)
	if err != nil {
		return nil, nil, err
	}

	if msgType == errMsg {
		var resp errResp
		if err := dec.Decode(&resp); err != nil {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("remote error: %v", resp.Error)
	}

	// Quit if not push/pull
	if msgType != pushPullMsg {
		err := fmt.Errorf("received invalid msgType (%d), expected pushPullMsg (%d) %s", msgType, pushPullMsg, LogConn(conn))
		return nil, nil, err
	}

	// Read remote state
	_, remoteNodes, userState, err := g.readRemoteState(bufConn, dec)
	return remoteNodes, userState, err
}

// sendLocalState is invoked to send our local state over a stream connection.
func (g *Gossip) sendLocalState(conn net.Conn, join bool) error {
	// Setup a deadline
	conn.SetDeadline(time.Now().Add(g.config.TCPTimeout))

	// Prepare the local node state
	g.nodeLock.RLock()
	localNodes := make([]pushNodeState, len(g.nodes))
	for idx, n := range g.nodes {
		localNodes[idx].Name = n.Name
		localNodes[idx].Addr = n.Addr
		localNodes[idx].Port = n.Port
		localNodes[idx].Incarnation = n.Incarnation
		localNodes[idx].State = n.State
		localNodes[idx].Meta = n.Meta
		localNodes[idx].Vsn = []uint8{
			n.PMin, n.PMax, n.PCur,
			n.DMin, n.DMax, n.DCur,
		}
	}
	g.nodeLock.RUnlock()

	// Get the delegate state
	var userData []byte
	if g.config.Delegate != nil {
		userData = g.config.Delegate.LocalState(join)
	}

	// Create a bytes buffer writer
	bufConn := bytes.NewBuffer(nil)

	// Send our node state
	header := pushPullHeader{Nodes: len(localNodes), UserStateLen: len(userData), Join: join}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(bufConn, &hd)

	// Begin state push
	if _, err := bufConn.Write([]byte{byte(pushPullMsg)}); err != nil {
		return err
	}

	if err := enc.Encode(&header); err != nil {
		return err
	}
	for i := 0; i < header.Nodes; i++ {
		if err := enc.Encode(&localNodes[i]); err != nil {
			return err
		}
	}

	// Write the user state as well
	if userData != nil {
		if _, err := bufConn.Write(userData); err != nil {
			return err
		}
	}

	// Get the send buffer
	return g.rawSendMsgStream(conn, bufConn.Bytes())
}

// encryptLocalState is used to help encrypt local state before sending
func (g *Gossip) encryptLocalState(sendBuf []byte) ([]byte, error) {
	var buf bytes.Buffer

	// Write the encryptMsg byte
	buf.WriteByte(byte(encryptMsg))

	// Write the size of the message
	sizeBuf := make([]byte, 4)
	encVsn := g.encryptionVersion()
	encLen := encryptedLength(encVsn, len(sendBuf))
	binary.BigEndian.PutUint32(sizeBuf, uint32(encLen))
	buf.Write(sizeBuf)

	// Write the encrypted cipher text to the buffer
	key := g.config.Keyring.GetPrimaryKey()
	err := encryptPayload(encVsn, key, sendBuf, buf.Bytes()[:5], &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decryptRemoteState is used to help decrypt the remote state
func (g *Gossip) decryptRemoteState(bufConn io.Reader) ([]byte, error) {
	// Read in enough to determine message length
	cipherText := bytes.NewBuffer(nil)
	cipherText.WriteByte(byte(encryptMsg))
	_, err := io.CopyN(cipherText, bufConn, 4)
	if err != nil {
		return nil, err
	}

	// Ensure we aren't asked to download too much. This is to guard against
	// an attack vector where a huge amount of state is sent
	moreBytes := binary.BigEndian.Uint32(cipherText.Bytes()[1:5])
	if moreBytes > maxPushStateBytes {
		return nil, fmt.Errorf("Remote node state is larger than limit (%d)", moreBytes)
	}

	// Read in the rest of the payload
	_, err = io.CopyN(cipherText, bufConn, int64(moreBytes))
	if err != nil {
		return nil, err
	}

	// Decrypt the cipherText
	dataBytes := cipherText.Bytes()[:5]
	cipherBytes := cipherText.Bytes()[5:]

	// Decrypt the payload
	keys := g.config.Keyring.GetKeys()
	return decryptPayload(keys, cipherBytes, dataBytes)
}

// readStream is used to read from a stream connection, decrypting and
// decompressing the stream if necessary.
func (g *Gossip) readStream(conn net.Conn) (messageType, io.Reader, *codec.Decoder, error) {
	// Created a buffered reader
	var bufConn io.Reader = bufio.NewReader(conn)

	// Read the message type
	buf := [1]byte{0}
	if _, err := bufConn.Read(buf[:]); err != nil {
		return 0, nil, nil, err
	}
	msgType := messageType(buf[0])

	// Check if the message is encrypted
	if msgType == encryptMsg {
		if !g.config.EncryptionEnabled() {
			return 0, nil, nil,
				fmt.Errorf("Remote state is encrypted and encryption is not configured")
		}

		plain, err := g.decryptRemoteState(bufConn)
		if err != nil {
			return 0, nil, nil, err
		}

		// Reset message type and bufConn
		msgType = messageType(plain[0])
		bufConn = bytes.NewReader(plain[1:])
	} else if g.config.EncryptionEnabled() && g.config.GossipVerifyIncoming {
		return 0, nil, nil,
			fmt.Errorf("Encryption is configured but remote state is not encrypted")
	}

	// Get the msgPack decoders
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(bufConn, &hd)

	// Check if we have a compressed message
	if msgType == compressMsg {
		var c compress
		if err := dec.Decode(&c); err != nil {
			return 0, nil, nil, err
		}
		decomp, err := decompressBuffer(&c)
		if err != nil {
			return 0, nil, nil, err
		}

		// Reset the message type
		msgType = messageType(decomp[0])

		// Create a new bufConn
		bufConn = bytes.NewReader(decomp[1:])

		// Create a new decoder
		dec = codec.NewDecoder(bufConn, &hd)
	}

	return msgType, bufConn, dec, nil
}

// readRemoteState is used to read the remote state from a connection
func (g *Gossip) readRemoteState(bufConn io.Reader, dec *codec.Decoder) (bool, []pushNodeState, []byte, error) {
	// Read the push/pull header
	var header pushPullHeader
	if err := dec.Decode(&header); err != nil {
		return false, nil, nil, err
	}

	// Allocate space for the transfer
	remoteNodes := make([]pushNodeState, header.Nodes)

	// Try to decode all the states
	for i := 0; i < header.Nodes; i++ {
		if err := dec.Decode(&remoteNodes[i]); err != nil {
			return false, nil, nil, err
		}
	}

	// Read the remote user state into a buffer
	var userBuf []byte
	if header.UserStateLen > 0 {
		userBuf = make([]byte, header.UserStateLen)
		bytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserStateLen)
		if err == nil && bytes != header.UserStateLen {
			err = fmt.Errorf(
				"Failed to read full user state (%d / %d)",
				bytes, header.UserStateLen)
		}
		if err != nil {
			return false, nil, nil, err
		}
	}

	// For proto versions < 2, there is no port provided. Mask old
	// behavior by using the configured port
	for idx := range remoteNodes {
		if g.ProtocolVersion() < 2 || remoteNodes[idx].Port == 0 {
			remoteNodes[idx].Port = uint16(g.config.BindPort)
		}
	}

	return header.Join, remoteNodes, userBuf, nil
}

// mergeRemoteState is used to merge the remote state with our local state
func (g *Gossip) mergeRemoteState(join bool, remoteNodes []pushNodeState, userBuf []byte) error {
	if err := g.verifyProtocol(remoteNodes); err != nil {
		return err
	}

	// Invoke the merge delegate if any
	if join && g.config.Merge != nil {
		nodes := make([]*Node, len(remoteNodes))
		for idx, n := range remoteNodes {
			nodes[idx] = &Node{
				Name: n.Name,
				Addr: n.Addr,
				Port: n.Port,
				Meta: n.Meta,
				PMin: n.Vsn[0],
				PMax: n.Vsn[1],
				PCur: n.Vsn[2],
				DMin: n.Vsn[3],
				DMax: n.Vsn[4],
				DCur: n.Vsn[5],
			}
		}
		if err := g.config.Merge.NotifyMerge(nodes); err != nil {
			return err
		}
	}

	// Merge the membership state
	g.mergeState(remoteNodes)

	// Invoke the delegate for user state
	if userBuf != nil && g.config.Delegate != nil {
		g.config.Delegate.MergeRemoteState(userBuf, join)
	}
	return nil
}

// readUserMsg is used to decode a userMsg from a streag.
func (g *Gossip) readUserMsg(bufConn io.Reader, dec *codec.Decoder) error {
	// Read the user message header
	var header userMsgHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	// Read the user message into a buffer
	var userBuf []byte
	if header.UserMsgLen > 0 {
		userBuf = make([]byte, header.UserMsgLen)
		bytes, err := io.ReadAtLeast(bufConn, userBuf, header.UserMsgLen)
		if err == nil && bytes != header.UserMsgLen {
			err = fmt.Errorf(
				"Failed to read full user message (%d / %d)",
				bytes, header.UserMsgLen)
		}
		if err != nil {
			return err
		}

		d := g.config.Delegate
		if d != nil {
			d.NotifyMsg(userBuf)
		}
	}

	return nil
}

// sendPingAndWaitForAck makes a stream connection to the given address, sends
// a ping, and waits for an ack. All of this is done as a series of blocking
// operations, given the deadline. The bool return parameter is true if we
// we able to round trip a ping to the other node.
func (g *Gossip) sendPingAndWaitForAck(addr string, ping ping, deadline time.Time) (bool, error) {
	conn, err := g.transport.DialTimeout(addr, deadline.Sub(time.Now()))
	if err != nil {
		// If the node is actually dead we expect this to fail, so we
		// shouldn't spam the logs with it. After this point, errors
		// with the connection are real, unexpected errors and should
		// get propagated up.
		return false, nil
	}
	defer conn.Close()
	conn.SetDeadline(deadline)

	out, err := encode(pingMsg, &ping)
	if err != nil {
		return false, err
	}

	if err = g.rawSendMsgStream(conn, out.Bytes()); err != nil {
		return false, err
	}

	msgType, _, dec, err := g.readStream(conn)
	if err != nil {
		return false, err
	}

	if msgType != ackRespMsg {
		return false, fmt.Errorf("Unexpected msgType (%d) from ping %s", msgType, LogConn(conn))
	}

	var ack ackResp
	if err = dec.Decode(&ack); err != nil {
		return false, err
	}

	if ack.SeqNo != ping.SeqNo {
		return false, fmt.Errorf("Sequence number from ack (%d) doesn't match ping (%d)", ack.SeqNo, ping.SeqNo)
	}

	return true, nil
}
