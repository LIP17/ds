// Contains the implementation of a LSP client.

package lsp

import (
	"bytes"
	"container/list"
	"crypto/md5"
	"encoding/json"
	"errors"
	// "fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type client struct {
	conn      *lspnet.UDPConn
	connectID int

	hostport    string
	isConnected bool
	isLost      bool
	isClosed    bool

	acceptAck      bool
	connectAckChan chan *Message

	epochTick         *time.Ticker
	epochFiredChan    chan bool
	numEpochNoMsg     int
	msgArrivedInEpoch bool

	sendSeqNum   int
	needSendChan chan *Message
	sendMsgList  *list.List

	needReadChan  chan bool
	readedMsgChan chan *Message

	expectedReceivedSeqNum int
	receivedMsgChan        chan *Message
	receivedMsgMap         map[int]*Message
	waitToReadMsgID        int
	lastReceivedId         int

	needCloseChan chan bool

	closeHandleConnectChan       chan bool
	closeEpochfireChan           chan bool
	closeListenMsgFromServerChan chan bool
	closeFinishedChan            chan bool

	noncloseNum           int
	epochTimeoutCloseChan chan bool
	unfinishedRead        int

	params *Params
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	//initialize new client
	newClient := new(client)

	newClient.hostport = hostport
	newClient.isConnected = false
	newClient.isLost = true
	newClient.isClosed = true
	newClient.acceptAck = false
	newClient.connectAckChan = make(chan *Message)

	newClient.epochTick = time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis))
	newClient.epochFiredChan = make(chan bool)
	newClient.numEpochNoMsg = 0
	newClient.msgArrivedInEpoch = false

	newClient.sendSeqNum = 0
	newClient.needSendChan = make(chan *Message)
	newClient.sendMsgList = list.New()

	newClient.needReadChan = make(chan bool)
	newClient.readedMsgChan = make(chan *Message)

	newClient.expectedReceivedSeqNum = 0
	newClient.receivedMsgChan = make(chan *Message)
	newClient.receivedMsgMap = make(map[int]*Message)
	newClient.waitToReadMsgID = -1
	newClient.lastReceivedId = -1

	newClient.needCloseChan = make(chan bool)

	newClient.closeHandleConnectChan = make(chan bool)
	newClient.closeEpochfireChan = make(chan bool)
	newClient.closeListenMsgFromServerChan = make(chan bool, 1)
	newClient.closeFinishedChan = make(chan bool)

	newClient.noncloseNum = 0 // 只有当外部应用发出了close指令之后才会将这个数置为1
	newClient.epochTimeoutCloseChan = make(chan bool)
	newClient.unfinishedRead = 0
	newClient.params = params

	//build a connection
	err := buildConnection(newClient, hostport)
	if err != nil {
		// fmt.Println("///Error in buildConnection()")
		return nil, err
	}

	//send connection request
	connMsg := NewConnect()
	err = sendMarshalMessage(newClient, connMsg)
	if err != nil {
		// fmt.Println("///Error in sendMarshalMessage()")
		return nil, err
	}
	// fmt.Println("+++Client has sent conn request, waiting for ack...")

	go handleConnectAck(newClient)
	go epochFired(newClient)
	go epochTimeoutClose(newClient)

	//check whether connection is build
	for {
		select {
		case ackMsg := <-newClient.connectAckChan:
			if newClient.isConnected == false {
				newClient.connectID = ackMsg.ConnID
				newClient.sendSeqNum = 1
				newClient.expectedReceivedSeqNum = 1

				newClient.isConnected = true
				newClient.isLost = false
				newClient.isClosed = false

				go handleConnection(newClient)
				go listenMsgFromServer(newClient)

				return newClient, nil
			}
		case <-newClient.epochFiredChan:
			if newClient.numEpochNoMsg < newClient.params.EpochLimit {
				connMsg := NewConnect()
				err := sendMarshalMessage(newClient, connMsg)
				if err != nil {
					// fmt.Println("///Error in sendMarshalMessage() again")
				}
				newClient.numEpochNoMsg++
			} else {
				return nil, errors.New("Client is lost...")
			}
		}
	}
}

//establish a connection
func buildConnection(client *client, hostport string) error {
	uaddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {

		return err
	}

	uconn, e := lspnet.DialUDP("udp", nil, uaddr)
	if e != nil {

		return e
	}
	client.conn = uconn

	return nil
}

//send marshal message
func sendMarshalMessage(client *client, msg *Message) error {
	pck, _ := json.Marshal(msg)
	_, e := client.conn.Write(pck)
	if e != nil {
		// write to client and failed
		return e
	}
	return nil
}

//receive connect ack message
func handleConnectAck(client *client) {
	for {
		var buf [1000]byte
		n, err := client.conn.Read(buf[0:])
		if err != nil {
			// fmt.Println("///Error when read ack")
		}

		ackMsg := new(Message)
		e := json.Unmarshal(buf[0:n], ackMsg)
		if e != nil {
			// fmt.Println("///Error when unmarshal ackMsg")
		}

		if ackMsg.Type == MsgAck && ackMsg.SeqNum == 0 {
			client.acceptAck = true
			client.msgArrivedInEpoch = true
			client.connectAckChan <- ackMsg

			return
		}
	}
}

//handle all kind of cases

// 因为epochFiredChan跟其他的方法
// 共用一个select,所以读写方法使用之前要
// 判断是否是lost的

func handleConnection(client *client) {
	for {
		select {
		//epoch handler
		case <-client.epochFiredChan:
			handleEpoch(client)

			//send handler
		case msg := <-client.needSendChan:
			handleSendMsg(client, msg)

			//read handler
		case <-client.needReadChan:
			handleReadMsg(client)

			//receive msg from server
		case msg := <-client.receivedMsgChan:
			handleReceivedMsg(client, msg)

			//close handler
		case <-client.needCloseChan:
			handleClose(client)

			//exit
		case <-client.closeHandleConnectChan:
			return
		}
	}
}

func epochTimeoutClose(client *client) {
	select {
	case <-client.epochTimeoutCloseChan:
		client.closeHandleConnectChan <- true
	}
	return
}

//send message to server
func handleSendMsg(client *client, msg *Message) {
	if !client.isLost {
		client.sendMsgList.PushBack(msg)
		if msg.SeqNum < client.sendMsgList.Front().Value.(*Message).SeqNum+client.params.WindowSize {
			sendMarshalMessage(client, msg)
		}
	}
}

//read msg from received msg map
func handleReadMsg(client *client) {
	if !client.isLost {
		//expectedReceivedSeqNum 表示外部的应用期望收到的msg的number，因为外部应用要求收到的消息是有顺序的
		readMsgId := client.expectedReceivedSeqNum
		// 如果这个消息已经收到了，就返回这个消息，并把他从map里面删除
		msg, ok := client.receivedMsgMap[readMsgId]
		if ok {
			client.readedMsgChan <- msg
			client.expectedReceivedSeqNum++
			delete(client.receivedMsgMap, readMsgId)
		} else {
			client.waitToReadMsgID = readMsgId
			client.unfinishedRead++
		}
	} else {
		client.readedMsgChan <- nil
	}
}

//listen and receive msg from server side
func listenMsgFromServer(client *client) {
	for {
		select {
		case <-client.closeListenMsgFromServerChan:
			return
		default:
			var buf [1000]byte
			n, err := client.conn.Read(buf[0:])
			if err != nil {
				return
			}

			serverMsg := new(Message)
			e := json.Unmarshal(buf[0:n], serverMsg)
			if e != nil {
			}

			client.receivedMsgChan <- serverMsg
		}
	}
}

//handle received msg from server

/*
1. 收到ACK
		1.1 删除发送list里面对应的元素
		1.2 更新窗口位置，并把窗口内所有的元素重新发送
	2. 收到MSG
		2.1 hash对比判断是否corrupted
		2.2 从map中判断是否曾经收到过
		2.3 如果没收到过，发送ACK，判断读到的信息是不是外部应用正在等待的seqnum
		2.4 如果外部正在等待这个seqnum，则写到readmsgchannel
*/
func handleReceivedMsg(client *client, msg *Message) {
	switch msg.Type {
	case MsgAck:
		var deleteId int
		if !client.isLost {
			client.msgArrivedInEpoch = true

			//delete 1 msg in sendMsgList according to ack
			for e := client.sendMsgList.Front(); e != nil; e = e.Next() {
				if msg.SeqNum == e.Value.(*Message).SeqNum {
					deleteId = msg.SeqNum
					client.sendMsgList.Remove(e)
					break
				}
			}
			// 更新window的位置，并且发送window内的所有信息到server
			keepSendingId := deleteId + client.params.WindowSize
			for e := client.sendMsgList.Front(); e != nil; e = e.Next() {
				keepSendMsg := e.Value.(*Message)
				if keepSendMsg.SeqNum <= keepSendingId {
					sendMarshalMessage(client, keepSendMsg)
				} else {
					break
				}
			}
			//
			if client.noncloseNum != 0 {
				if client.sendMsgList.Len() == 0 && len(client.receivedMsgMap) == 0 {
					client.noncloseNum--
					client.closeFinishedChan <- true
				}
			}
		} else {
			// noncloseNum 是mutex型的lock，不过0,1对应与PV相反
			// 如果lost了直接--然后说明链接任务完成
			client.noncloseNum--
			client.closeFinishedChan <- true
		}

	case MsgData:
		//re-hash check
		hash := getHashCode(msg.ConnID, msg.SeqNum, msg.Payload)
		if bytes.Equal(hash, msg.Hash) == true {
			if !client.isLost {
				client.msgArrivedInEpoch = true

				if _, ok := client.receivedMsgMap[msg.SeqNum]; !ok {
					client.receivedMsgMap[msg.SeqNum] = msg

					ack := NewAck(client.connectID, msg.SeqNum)
					sendMarshalMessage(client, ack)
					client.lastReceivedId = msg.SeqNum

					if client.waitToReadMsgID != -1 && client.waitToReadMsgID == msg.SeqNum {
						keepReadMsg := client.receivedMsgMap[client.waitToReadMsgID]
						client.readedMsgChan <- keepReadMsg
						client.waitToReadMsgID = -1
						client.expectedReceivedSeqNum++
						client.unfinishedRead--
						delete(client.receivedMsgMap, msg.SeqNum)
					}
				}
			}
		}
	}
}

func (c *client) ConnID() int {
	if c.isConnected {
		return c.connectID
	}
	return 0
}

func (c *client) Read() ([]byte, error) {
	if c.isClosed || c.isLost {
		return nil, errors.New("Can't read...")
	}
	c.needReadChan <- true

	msg := new(Message)
	select {
	case msg = <-c.readedMsgChan:
		break
	}
	if msg == nil {
		return nil, errors.New("Can't read...")
	}
	payload := msg.Payload
	return payload, nil
}

func (c *client) Write(payload []byte) error {
	if c.isClosed || c.isLost {
		return errors.New("Can't write...")
	}

	hashCode := getHashCode(c.connectID, c.sendSeqNum, payload)
	msg := NewData(c.connectID, c.sendSeqNum, payload, hashCode)
	c.sendSeqNum++
	c.needSendChan <- msg
	return nil
}

func getHashCode(id, sn int, payload []byte) []byte {
	idd := strconv.Itoa(id)
	snn := strconv.Itoa(sn)

	hasher := md5.New()
	hasher.Write([]byte(idd))
	hasher.Write([]byte(snn))
	hasher.Write(payload)

	return hasher.Sum(nil)
}

func (c *client) Close() error {

	c.needCloseChan <- true

	<-c.closeFinishedChan

	c.closeEpochfireChan <- true

	c.closeHandleConnectChan <- true

	err := c.conn.Close()
	if err != nil {

		return err
	}
	c.isClosed = true

	c.closeListenMsgFromServerChan <- true

	return nil
}

func handleClose(client *client) {
	if !client.isLost {
		if client.sendMsgList.Len() == 0 && len(client.receivedMsgMap) == 0 {
			client.closeFinishedChan <- true
		} else {
			client.noncloseNum++
		}
	} else {
		client.closeFinishedChan <- true
	}
}

func epochFired(client *client) {
	for {
		select {
		case <-client.epochTick.C:
			client.epochFiredChan <- true
		case <-client.closeEpochfireChan:
			return
		}
	}
}

func handleEpoch(client *client) {
	// fmt.Println("^cEpoch triggered...")
	if !client.msgArrivedInEpoch {
		client.numEpochNoMsg++
	} else {
		client.numEpochNoMsg = 0
	}
	client.msgArrivedInEpoch = false

	//client is lost
	if client.numEpochNoMsg >= client.params.EpochLimit {
		client.isLost = true
		// fmt.Println("c: ^^Client is lost...")
		if !client.isConnected {
			err := client.conn.Close()
			if err != nil {
				// fmt.Println("///Error when close client...")
				return
			}
			client.isClosed = true
			return
		}

		//before lost happens, ensure there are no read request unfinished
		// 只有Read() 方法是被block住的，所以需要发送一个nil解开block
		if client.unfinishedRead != 0 {
			client.readedMsgChan <- nil
		}

		client.closeEpochfireChan <- true

		err := client.conn.Close()
		if err != nil {
			// fmt.Println("///Error when close client...")
			return
		}
		client.isClosed = true

		client.closeListenMsgFromServerChan <- true

		if client.noncloseNum != 0 {
			client.noncloseNum--
			client.isClosed = true
			client.closeFinishedChan <- true
		}

		client.epochTimeoutCloseChan <- true

		return
	}

	//if no ack for connection request, resend connection request
	if !client.acceptAck {
		connMsg := NewConnect()
		err := sendMarshalMessage(client, connMsg)
		if err != nil {
		}
	}

	//if no data msg received after connect-ack
	if len(client.receivedMsgMap) == 0 && client.expectedReceivedSeqNum == 1 {
		resendAck := NewAck(client.connectID, 0)
		sendMarshalMessage(client, resendAck)
	} else {
		// resend ACK's in the map

		// 因为没有收到ACK，所以Server会重发窗口内的信息，这样肯定又会
		// 收到这些seqnum的信息(虽然Read()方法可能已经读入过这些信息了)
		resendStartId := client.lastReceivedId
		resendEndId := client.lastReceivedId - client.params.WindowSize
		for resendStartId > resendEndId {
			if msg, ok := client.receivedMsgMap[resendStartId]; ok {
				resendAck := NewAck(client.connectID, msg.SeqNum)
				sendMarshalMessage(client, resendAck)
			}
			resendStartId--
		}
	}

	//resend data msg that has been sent but not ack received.
	if client.sendMsgList.Front() != nil {
		upperId := client.sendMsgList.Front().Value.(*Message).SeqNum + client.params.WindowSize
		for e := client.sendMsgList.Front(); e != nil; e = e.Next() {
			if msg, ok := e.Value.(*Message); ok {
				if msg.SeqNum >= upperId {
					break
				} else {
					sendMarshalMessage(client, msg)
					// fmt.Printf("!sEpoch, resend MsgData, msgId: %d\r\n", msg.SeqNum)
				}
			}
		}
	}
}
