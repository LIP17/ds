// Contains the implementation of a LSP server.

package lsp

import (
	"bytes"
	"container/list"
	"crypto/md5"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type server struct {
	conn                *lspnet.UDPConn
	clientConnIDCounter int
	clients             map[int]*connectedClient
	isClosed            bool

	needReadChan        chan bool
	readedMsgChan       chan *ReadResult
	unfinishedReadCount int

	needWriteChan     chan *WritePacket
	needCloseConnChan chan int
	receivedMsgChan   chan *Packet

	epochTick      *time.Ticker
	epochFiredChan chan bool

	needCloseChan                chan bool
	closeFinishedChan            chan bool
	closeListenMsgFromClientChan chan bool
	closeHandleConnectionChan    chan bool
	closeEpochFireChan           chan bool

	closePendingNum  int
	nonclosedClients map[int]*connectedClient

	params *Params
}

type connectedClient struct {
	uaddr     *lspnet.UDPAddr
	connectID int

	isClosed bool
	isLost   bool

	expectedReadSeqNum int
	receivedMsgMap     map[int]*Message

	sendMsgSeqNum int
	sendMsgList   *list.List

	numEpochNoMsg     int
	msgArrivedInEpoch bool
	lastReceivedId    int
}

type Packet struct {
	uaddr *lspnet.UDPAddr
	msg   *Message
}

type ReadResult struct {
	connID  int
	payload []byte
	err     error
}

type WritePacket struct {
	connID  int
	payload []byte
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	//initialize a new server
	newServer := new(server)

	newServer.clientConnIDCounter = 1
	newServer.clients = make(map[int]*connectedClient)
	newServer.isClosed = false

	newServer.needReadChan = make(chan bool)
	newServer.readedMsgChan = make(chan *ReadResult)
	newServer.unfinishedReadCount = 0

	newServer.needWriteChan = make(chan *WritePacket)
	newServer.needCloseConnChan = make(chan int)
	newServer.receivedMsgChan = make(chan *Packet)

	newServer.epochTick = time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis))
	newServer.epochFiredChan = make(chan bool)

	newServer.needCloseChan = make(chan bool)
	newServer.closeFinishedChan = make(chan bool)
	newServer.closeListenMsgFromClientChan = make(chan bool, 1)
	newServer.closeHandleConnectionChan = make(chan bool)
	newServer.closeEpochFireChan = make(chan bool)

	newServer.closePendingNum = 0
	newServer.nonclosedClients = make(map[int]*connectedClient)
	newServer.params = params

	//prepare server to listen on client request
	err := newServer.createConnection(port)
	if err != nil {
		// fmt.Println("///Error in buildConnection()")
		return nil, err
	}

	go listenMsgFromClient(newServer)
	go handleConnectionServer(newServer)
	go epochFiredServer(newServer)

	// fmt.Println("++Server build ok.")
	return newServer, nil
}

func (s *server) Read() (int, []byte, error) {
	if s.isClosed {
		return 0, nil, errors.New("server is closed when Read")
	}

	s.needReadChan <- true

	select {
	case result := <-s.readedMsgChan:
		return result.connID, result.payload, result.err
	}
}

func (s *server) Write(connID int, payload []byte) error {
	if _, ok := s.clients[connID]; !ok {
		return errors.New("connID doesn't exist when write")
	}

	wrtPack := &WritePacket{connID, payload}
	s.needWriteChan <- wrtPack

	return nil
}

func (s *server) CloseConn(connID int) error {
	if _, ok := s.clients[connID]; !ok {
		return errors.New("connID doesn't exist when close")
	}
	s.needCloseConnChan <- connID

	return nil
}

func (s *server) createConnection(port int) error {
	hostport := lspnet.JoinHostPort("localhost", strconv.Itoa(port))
	uaddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		// fmt.Println("///Error when resolve udp addr.")
		return err
	}

	uconn, e := lspnet.ListenUDP("udp", uaddr)
	if e != nil {
		// fmt.Println("///Error when listen udp.")
		return e
	}

	s.conn = uconn
	return nil
}

//handle all kinds of cases
func handleConnectionServer(server *server) {
	for {
		select {
		case pck := <-server.receivedMsgChan:
			serverHandleReceivedMsg(server, pck)

		case <-server.needReadChan:
			serverHandleReadMsg(server)

		case wrtPck := <-server.needWriteChan:
			serverHandleSendMsg(server, wrtPck)

		case id := <-server.needCloseConnChan:
			handleCloseConn(server, id)

		case <-server.epochFiredChan:
			serverHandleEpoch(server)

		case <-server.needCloseChan:
			serverHandleClose(server)

		case <-server.closeHandleConnectionChan:
			// fmt.Println("~~Close hanldeConnection.")
			return
		}
	}
}

//listening client request
func listenMsgFromClient(server *server) {
	for {
		select {
		case <-server.closeListenMsgFromClientChan:
			// fmt.Println("~~close listenMsgFromClient.")
			return

		default:
			var buf [1000]byte
			n, uaddr, err := server.conn.ReadFromUDP(buf[0:])
			if err != nil {
				// fmt.Println("///Error when ReadFromUDP.")
				return
			}

			msg := new(Message)
			err = json.Unmarshal(buf[0:n], msg)
			if err != nil {
				// fmt.Println("///Error when unmarshal msg from client")
			}

			pck := &Packet{uaddr, msg}
			server.receivedMsgChan <- pck
		}
	}
}

func serverHandleReceivedMsg(server *server, pck *Packet) {
	uaddr := pck.uaddr
	msg := pck.msg
	switch msg.Type {
	case MsgConnect:
		exist := false
		for _, c := range server.clients {
			if c.uaddr.String() == uaddr.String() {
				exist = true
				break
			}
		}
		//ack to connect request when no duplicate
		if !exist {
			id := server.clientConnIDCounter
			server.clients[id] = NewConnClient(pck, id)
			server.clientConnIDCounter++

			ackMsg := NewAck(id, 0)
			server.serverSendMarshalMsg(ackMsg, uaddr)
		}

	case MsgData:
		c := server.clients[msg.ConnID]
		//re-hash check
		hash := sgetHashCode(msg.ConnID, msg.SeqNum, msg.Payload)
		if bytes.Equal(hash, msg.Hash) == true {
			// fmt.Println("++MsgData hash is same.")
			if !c.isLost {
				c.msgArrivedInEpoch = true
				//check if msg doesn't exist: store in map and send back ack
				if _, ok := c.receivedMsgMap[msg.SeqNum]; !ok {
					c.receivedMsgMap[msg.SeqNum] = msg

					ack := NewAck(c.connectID, msg.SeqNum)
					server.serverSendMarshalMsg(ack, uaddr)

					c.lastReceivedId = msg.SeqNum

					//if there is Read unfinished, now can Read
					if server.unfinishedReadCount != 0 && c.expectedReadSeqNum == msg.SeqNum {
						readRes := &ReadResult{c.connectID, msg.Payload, nil}
						server.readedMsgChan <- readRes

						server.unfinishedReadCount--
						c.expectedReadSeqNum++
						delete(c.receivedMsgMap, msg.SeqNum)
					}
				}
			}
		}

	case MsgAck:
		c := server.clients[msg.ConnID]
		var deleteId int

		if !c.isLost {
			c.msgArrivedInEpoch = true

			//delete corresponding msg in sendMsgList
			for e := c.sendMsgList.Front(); e != nil; e = e.Next() {
				if e.Value.(*Message).SeqNum == msg.SeqNum {
					deleteId = msg.SeqNum
					c.sendMsgList.Remove(e)
					break
				}
			}
			//keep sending
			keepSendingId := deleteId + server.params.WindowSize
			for e := c.sendMsgList.Front(); e != nil; e = e.Next() {
				keepSendMsg := e.Value.(*Message)
				if keepSendMsg.SeqNum <= keepSendingId {
					server.serverSendMarshalMsg(keepSendMsg, uaddr)
				} else {
					break
				}
			}
			//check if close is called
			if server.closePendingNum != 0 {
				_, ok := server.nonclosedClients[c.connectID]
				if ok {
					if c.sendMsgList.Len() == 0 {
						server.closePendingNum--
						// fmt.Println("s: pendingNum-- :", server.closePendingNum)

						delete(server.nonclosedClients, c.connectID)
						// fmt.Println("s: nonCloseClients: ", len(server.nonclosedClients))
						if len(server.nonclosedClients) == 0 {
							// fmt.Println("s: nonCloseClients is empty, finish close..")
							server.closeFinishedChan <- true
						}
					}
				}
			}
		} else { //if lost, close directly
			_, ok := server.nonclosedClients[c.connectID]
			if ok {
				server.closePendingNum--
				delete(server.nonclosedClients, c.connectID)
				if len(server.nonclosedClients) == 0 {
					server.closeFinishedChan <- true
				}
			}
		}

	}
}

func NewConnClient(pck *Packet, id int) *connectedClient {
	return &connectedClient{
		uaddr:              pck.uaddr,
		connectID:          id,
		isClosed:           false,
		isLost:             false,
		expectedReadSeqNum: 1,
		receivedMsgMap:     make(map[int]*Message),
		// readFailedOnce:     false,
		sendMsgSeqNum:     1,
		sendMsgList:       list.New(),
		numEpochNoMsg:     0,
		msgArrivedInEpoch: false,
		lastReceivedId:    -1,
	}
}

//send marshal message
func (s *server) serverSendMarshalMsg(msg *Message, uaddr *lspnet.UDPAddr) error {
	// fmt.Println("++Server send MarshalMsg to client.")
	pck, _ := json.Marshal(msg)
	_, e := s.conn.WriteToUDP(pck, uaddr)
	if e != nil {
		// fmt.Println("///Error when send marshal msg to client.")
		return e
	}
	return nil
}

func serverHandleReadMsg(server *server) {
	result := new(ReadResult)
	success := false

	for _, client := range server.clients {

		if !client.isClosed && !client.isLost {
			readMsgId := client.expectedReadSeqNum
			msg, ok := client.receivedMsgMap[readMsgId]
			if ok {
				result.connID = client.connectID
				result.payload = msg.Payload
				result.err = nil
				server.readedMsgChan <- result
				client.expectedReadSeqNum++
				delete(client.receivedMsgMap, readMsgId)
				success = true
				break
			}
		} else {

			result.connID = client.connectID
			result.payload = nil
			result.err = errors.New("Client closed or lost")
			server.readedMsgChan <- result
			success = true
			break

		}
	}

	if !success {
		server.unfinishedReadCount++
	}
}

func serverHandleSendMsg(server *server, wrtPck *WritePacket) {
	connID := wrtPck.connID
	payload := wrtPck.payload
	c, ok := server.clients[connID]
	if ok && !c.isClosed && !c.isLost {
		hash := sgetHashCode(connID, c.sendMsgSeqNum, payload)
		sendMsg := NewData(connID, c.sendMsgSeqNum, payload, hash)
		c.sendMsgList.PushBack(sendMsg)
		c.sendMsgSeqNum++

		if c.sendMsgList.Front().Value.(*Message).SeqNum+server.params.WindowSize > sendMsg.SeqNum {
			server.serverSendMarshalMsg(sendMsg, c.uaddr)
		}
	}
}

func sgetHashCode(id, sn int, payload []byte) []byte {
	idd := strconv.Itoa(id)
	snn := strconv.Itoa(sn)

	hasher := md5.New()
	hasher.Write([]byte(idd))
	hasher.Write([]byte(snn))
	hasher.Write(payload)

	return hasher.Sum(nil)
}

func handleCloseConn(server *server, connID int) {
	c, ok := server.clients[connID]
	if ok {
		c.isClosed = true
	}
}

//time trigger
func epochFiredServer(server *server) {
	for {
		select {
		case <-server.epochTick.C:
			server.epochFiredChan <- true

		case <-server.closeEpochFireChan:
			// fmt.Println("++close epochFired.")
			return
		}
	}
}

func serverHandleEpoch(server *server) {
	// fmt.Println("s: epoch triggered..")
	for id, c := range server.clients {
		if !c.isLost {
			if !c.msgArrivedInEpoch {
				c.numEpochNoMsg++
			} else {
				c.numEpochNoMsg = 0
			}
			c.msgArrivedInEpoch = false

			//if timeout, lost
			if c.numEpochNoMsg >= server.params.EpochLimit {
				c.isLost = true
				// fmt.Println("s: c is lost")

				//ensure no more read request left
				if server.unfinishedReadCount != 0 {
					result := new(ReadResult)
					result.connID = c.connectID
					result.payload = nil
					result.err = errors.New("Client closed or lost")
					server.readedMsgChan <- result
				}

				delete(server.clients, id)
				return
			}

			if !c.isClosed {
				//reack to connection request
				if len(c.receivedMsgMap) == 0 && c.expectedReadSeqNum == 1 {
					reack := NewAck(c.connectID, 0)
					server.serverSendMarshalMsg(reack, c.uaddr)
					// fmt.Println("!sEpoch, reack to conn: ", c.connectID)
				} else {
					resendStartId := c.lastReceivedId
					resendEndId := resendStartId - server.params.WindowSize
					for resendStartId > resendEndId {
						if msg, ok := c.receivedMsgMap[resendStartId]; ok {
							reack := NewAck(c.connectID, msg.SeqNum)
							server.serverSendMarshalMsg(reack, c.uaddr)
						}
						resendStartId--
					}
				}

				//resend data msg that has been sent but not ack received.
				if c.sendMsgList.Front() != nil {
					upperId := c.sendMsgList.Front().Value.(*Message).SeqNum + server.params.WindowSize
					for e := c.sendMsgList.Front(); e != nil; e = e.Next() {
						if msg, ok := e.Value.(*Message); ok {
							if msg.SeqNum >= upperId {
								break
							} else {
								// 发送窗内信息
								server.serverSendMarshalMsg(msg, c.uaddr)
							}
						}
					}
				}
			}

		}
	}
}

func (s *server) Close() error {

	s.needCloseChan <- true

	<-s.closeFinishedChan

	s.closeEpochFireChan <- true

	s.closeHandleConnectionChan <- true

	err := s.conn.Close()
	if err != nil {
		return nil
	}
	s.isClosed = true

	s.closeListenMsgFromClientChan <- true

	return nil
}

func serverHandleClose(server *server) {
	// fmt.Println("s:handle close")
	for id, c := range server.clients {
		if !c.isClosed {
			if c.isLost {
				c.isClosed = true
				// fmt.Println("s: this c is already closed.")
				continue
			}

			if c.sendMsgList.Len() == 0 && len(c.receivedMsgMap) == 0 {
				// fmt.Println("s: c closed")
				c.isClosed = true
			} else {
				server.nonclosedClients[id] = c
			}
		}
	}

	if len(server.nonclosedClients) == 0 {
		// fmt.Println("s: uncloseNum: 0")
		server.closeFinishedChan <- true
	} else {
		server.closePendingNum = len(server.nonclosedClients)
		// fmt.Println("s: uncloseNum: ", server.closePendingNum)
	}

}
