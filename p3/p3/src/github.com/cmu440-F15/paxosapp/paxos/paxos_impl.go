package paxos

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cmu440-F15/paxosapp/rpc/paxosrpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// type deadLine int64
const DL int64 = 3000 * int64(time.Millisecond)
const DLPropose int64 = 15 * int64(time.Second)

type paxosNode struct {
	ss        map[int]*rpc.Client
	numNodes  int
	id        int
	instances map[string]*paxosKeyData
	ptimes    int
	majorNum  int
}

type paxosKeyData struct {
	Na           int           // highest proposal #
	Va           interface{}   // its corresponding accepted value
	Nh           int           // highest proposal # seen
	Myn          int           // my proposal # in current Paxos
	mu           *sync.RWMutex //Common Lock for GetValue RecvCommit RecvPrepare RecvAccept
	CommittedVal interface{}   //the final committed value for the key
	storeLock    *sync.RWMutex //lock for committedVal
	proposeLock  *sync.RWMutex //lock for propose
}

// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if the node
// could not be started in spite of dialing the other nodes numRetries times.
// hostMap is a map from node IDs to their hostports, numNodes is the number
// of nodes in the ring, replace is a flag which indicates whether this node
// is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	pn := &paxosNode{
		ss:        make(map[int]*rpc.Client),
		numNodes:  numNodes,
		id:        srvId,
		ptimes:    0,
		majorNum:  numNodes/2 + 1,
		instances: make(map[string]*paxosKeyData),
	}
	// Create the server socket that will listen for incoming RPCs.
	_, port, _ := net.SplitHostPort(myHostPort)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("PaxosNode", paxosrpc.Wrap(pn))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	receivedData := false
	// make connnection to all other nodes
	for servid, hport := range hostMap {
		conn, err := rpc.DialHTTP("tcp", hport)
		ri := 0
		for ; err != nil && ri < numRetries; ri++ {
			time.Sleep(1000 * time.Millisecond)
			conn, err = rpc.DialHTTP("tcp", hport)
		}
		if ri >= numRetries {
			return nil, err
		}
		pn.ss[servid] = conn
		// if replace flag is set
		if replace && servid != srvId {
			var myreply paxosrpc.ReplaceServerReply
			myargs := &paxosrpc.ReplaceServerArgs{
				Hostport: myHostPort,
				SrvID:    srvId,
			}
			//replace the old server entry in other server
			conn.Call("PaxosNode.RecvReplaceServer", myargs, &myreply)
			// flag to check if we have already received data from other nodes
			if !receivedData {
				var serverState paxosrpc.ReplaceCatchupReply
				var catchupArgs paxosrpc.ReplaceCatchupArgs
				// call other server for data
				conn.Call("PaxosNode.RecvReplaceCatchup", catchupArgs, &serverState)
				// the map is decoded from gob
				network := bytes.NewBuffer(serverState.Data)
				dec := gob.NewDecoder(network)
				var instances map[string]*paxosKeyData
				dec.Decode(&instances)

				receivedData = true
				pn.instances = instances
			}
		}
	}

	return pn, nil
}

//get paxosKeyData data structure for the key. Create one and return if none exists (singleton pattern)
func (pn *paxosNode) getInstance(key string) *paxosKeyData {
	pxi, ok := pn.instances[key]
	if !ok {
		pxi = &paxosKeyData{
			Myn:          0,
			Na:           -1,
			Nh:           0,
			Va:           nil,
			mu:           &sync.RWMutex{},
			CommittedVal: nil,
			storeLock:    &sync.RWMutex{},
			proposeLock:  &sync.RWMutex{},
		}
		pn.instances[key] = pxi
	}
	return pxi
}

// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat, and for a particular node
// they should be strictly increasing.
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	// Will just give the Max([Nh/k]*k + id , )
	key := args.Key
	pxi := pn.getInstance(key)

	pxi.mu.RLock()
	defer pxi.mu.RUnlock()

	replyN := (pxi.Nh/pn.numNodes+1)*pn.numNodes + pn.id
	reply.N = replyN

	return nil
}

// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or 15 seconds have passed. Returns an error if 15 seconds have passed
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	key := args.Key
	n_proposal := args.N
	v := args.V
	pkd := pn.getInstance(key)
	//we lock on this key on this node because we only have one instance of paxos per key at one time
	pkd.proposeLock.Lock()
	defer pkd.proposeLock.Unlock()
	//PROPOSE PHASE
	// A node decides to be leader (and propose)
	// Leader chooses Myn > Nh
	// Leader sends <prepare, Myn> to all nodes
	// ask for majority here
	// should make it parallel, but here we use a loop
	// suppose there are not so much paxosNode in the test program
	vote := 0
	chanRet := make(chan bool)
	timeoutTime := time.Now().UnixNano() + DLPropose

	go func() {
		for {
			if timeoutTime < time.Now().UnixNano() {
				//fmt.println("BYE BYE")
				close(chanRet)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	var wg sync.WaitGroup
	v_prime := v
	highest_n_a := -1
	for _, conn := range pn.ss {
		wg.Add(1)
		myargs := &paxosrpc.PrepareArgs{
			Key: key,
			N:   n_proposal,
		}
		//we call RecvPrepare on each server asynchronously
		go func(conn *rpc.Client) {
			defer wg.Done()

			chanTime := make(chan bool)
			chanRPC := make(chan bool)
			var myreply paxosrpc.PrepareReply

			// actual query
			go func() {
				conn.Call("PaxosNode.RecvPrepare", myargs, &myreply)
				if myreply.Status == paxosrpc.OK {
					vote++
					if myreply.N_a > highest_n_a {
						highest_n_a = myreply.N_a
						v_prime = myreply.V_a
					}

				}
				chanRPC <- true
			}()
			expiry := time.Now().UnixNano() + DL
			// deadline of DL seconds for RPC to return
			go func() {
				for {
					if expiry < time.Now().UnixNano() {
						chanTime <- true
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}()
			//revoking using channels
			select {
			case <-chanRPC:
				// rpc returned
				return
			case <-chanTime:
				// RecvPrepare timeout
				return
			case <-chanRet:
				// Revoking because of 15 second timeout of propose

				return

			}
		}(conn)

	}
	// use of wait to ensure all the RPC calls to recvPrepare have either timed out or returned
	wg.Wait()
	pxi := pn.getInstance(key)

	// we hsould now update the highest_n_a for this paxos instance
	pxi.mu.Lock()
	if highest_n_a > pxi.Nh {
		pxi.Nh = highest_n_a

	}
	pxi.mu.Unlock()

	// If leader fails to get prepare-ok from a majority
	if vote < pn.majorNum {
		return errors.New("Unable to get Majority")
	}

	// ACCEPT PHASE
	// If leader gets prepare-ok from a majority
	// V = non-empty value corresponding to the highest Na received
	// If V= null, then leader can pick any V
	// Send <accept, Myn, V> to all nodes
	newVote := 0
	for _, conn := range pn.ss {
		wg.Add(1)
		myargs := &paxosrpc.AcceptArgs{
			Key: key,
			N:   n_proposal,
			V:   v_prime,
		}
		//we call RecvAccept on each server asynchronously
		go func(conn *rpc.Client) {
			defer wg.Done()

			chanTime := make(chan bool)
			chanRPC := make(chan bool)
			var myreply paxosrpc.AcceptReply

			// actual query
			go func() {

				conn.Call("PaxosNode.RecvAccept", myargs, &myreply)

				if myreply.Status == paxosrpc.OK {
					newVote++
				}
				chanRPC <- true
			}()
			expiry := time.Now().UnixNano() + DL
			// deadline of DL seconds for RPC to return
			go func() {
				for {
					if expiry < time.Now().UnixNano() {
						chanTime <- true
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}()

			//revoking using channels
			select {
			case <-chanRPC:
				// rpc returned
				return
			case <-chanTime:
				// RecvAccept timeout
				return
			case <-chanRet:
				// Revoking because of 15 second timeout of propose
				return
			}
		}(conn)

	}
	wg.Wait()

	// If leader fails to get prepare-ok from a majority
	if newVote < pn.majorNum {
		return errors.New("Unable to get Majority")
	}

	//COMMIT PHASE
	// If leader gets accept-ok from a majority
	// Send <commit, Va> to all nodes
	for _, conn := range pn.ss {
		wg.Add(1)
		myargs := &paxosrpc.CommitArgs{
			Key: key,
			V:   v_prime,
		}
		//we call RecvCommit on each server asynchronously
		go func(conn *rpc.Client) {
			defer wg.Done()
			var myreply paxosrpc.CommitReply
			chanRPC := make(chan bool)
			chanTime := make(chan bool)

			go func() {
				conn.Call("PaxosNode.RecvCommit", myargs, &myreply)
				chanRPC <- true
			}()
			expiry := time.Now().UnixNano() + DL
			// deadline of DL seconds for RPC to return
			go func() {
				for {
					if expiry < time.Now().UnixNano() {
						chanTime <- true
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}()
			//revoking using channels
			select {
			case <-chanRPC:
				// rpc returned
				return
			case <-chanRet:
				// Revoking because of 15 second timeout of propose
				return
			case <-chanTime:
				//RPC call timedout
				return
			}
		}(conn)

	}
	wg.Wait()
	select {
	case <-chanRet:
		return errors.New("Timeout")
	default:
		reply.V = v_prime
		return nil
	}
	return nil
}

// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	key := args.Key
	pxi := pn.getInstance(key)
	pxi.storeLock.RLock()
	defer pxi.storeLock.RUnlock()
	value := pxi.CommittedVal
	if value == nil {
		reply.Status = paxosrpc.KeyNotFound
		reply.V = nil
	} else {
		reply.Status = paxosrpc.KeyFound
		reply.V = value
	}

	return nil
}

// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	// Upon receiving <Prepare, n, V>
	// If n < Nh
	//     reply with <accept-reject, Na, Va>
	// else
	//    Na = n; Va = V; Nh = n
	//     reply with <accept-ok>
	key := args.Key
	n := args.N
	pxi := pn.getInstance(key)
	pxi.mu.Lock()
	defer pxi.mu.Unlock()
	//reject if request is with a proposal number less than the highest seen by this node for this key
	if n < pxi.Nh {
		reply.Status = paxosrpc.Reject
		reply.N_a = pxi.Na
		reply.V_a = pxi.Va
	} else {
		pxi.Nh = n
		reply.Status = paxosrpc.OK
		reply.N_a = pxi.Na
		reply.V_a = pxi.Va
		if reply.V_a == nil {
			reply.N_a = -1
		}
	}

	return nil
}

// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	// Upon receiving <accept, n, V>
	// If n < Nh
	//     reply with <accept-reject>
	// else
	//    Na = n; Va = V; Nh = n
	//     reply with <accept-ok>
	key := args.Key
	n := args.N
	v := args.V
	pxi := pn.getInstance(key)
	pxi.mu.Lock()
	defer pxi.mu.Unlock()
	if n < pxi.Nh {
		reply.Status = paxosrpc.Reject
	} else {
		pxi.Nh = n
		pxi.Na = n
		pxi.Va = v
		reply.Status = paxosrpc.OK
	}

	return nil
}

// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	// Upon receiving <Commit, Key, V>
	// committedVal = V
	key := args.Key
	v := args.V
	pxi := pn.getInstance(key)
	pxi.mu.Lock()
	defer pxi.mu.Unlock()
	pxi.storeLock.Lock()
	defer pxi.storeLock.Unlock()
	pxi.Va = nil
	pxi.Na = -1

	pxi.CommittedVal = v

	return nil
}

// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	// we know that replacement server should already have started
	conn, err := rpc.DialHTTP("tcp", args.Hostport)
	// replace the old server connection with the new server
	pn.ss[args.SrvID] = conn
	return err
}

// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	var network bytes.Buffer // Stand-in for the network.

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	err := enc.Encode(pn.instances)
	if err != nil {
	}
	reply.Data = network.Bytes()
	return nil
}
