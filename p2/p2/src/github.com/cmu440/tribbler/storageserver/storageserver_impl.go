package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type storageServer struct {
	// TODO: implement this!
	hostport string
	nodeId   uint32
	sMap     map[string]*sValue
	sMutex   *sync.Mutex // Mutex for single value map
	lMap     map[string]*lValue
	lMutex   *sync.Mutex // Mutex for list value map

	libStoreMap map[string]*rpc.Client
	libMutex    *sync.Mutex

	// [contentkey] -> [libstore key] -> time
	leaseMap map[string](map[string]time.Time) // select to change

	keyLockMap map[string]*sync.Mutex

	nodeIdMap map[uint32]storagerpc.Node // Map from node id to the node info (port, id)
	nodesList []storagerpc.Node

	storageServerReady bool
	serverFull         chan int
	nodeSize           int

	cMutex        *sync.Mutex
	informedCount int
}

// save only 1 string
type sValue struct {
	value string
}

// save list of string
type lValue struct {
	value []string
}

// sort for consistent hash
type Nodes []storagerpc.Node

func (nodeList Nodes) Len() int {
	return len(nodeList)
}

func (nodeList Nodes) Less(i, j int) bool {
	return nodeList[i].NodeID < nodeList[j].NodeID
}

func (nodeList Nodes) Swap(i, j int) {
	nodeList[i], nodeList[j] = nodeList[j], nodeList[i]
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	newStorageServer := &storageServer{
		hostport:           fmt.Sprintf("localhost:%d", port),
		nodeId:             nodeID,
		sMap:               make(map[string]*sValue),
		sMutex:             &sync.Mutex{},
		lMap:               make(map[string]*lValue),
		lMutex:             &sync.Mutex{},
		libStoreMap:        make(map[string]*rpc.Client),
		libMutex:           &sync.Mutex{},
		leaseMap:           make(map[string](map[string]time.Time)),
		keyLockMap:         make(map[string]*sync.Mutex),
		storageServerReady: false,
		nodeSize:           numNodes,
		cMutex:             &sync.Mutex{},
	}

	// if masterServerHostPort is empty (len == 0), it is the master server
	if len(masterServerHostPort) == 0 {
		// for master Server, first join itself
		newStorageServer.nodeIdMap = make(map[uint32]storagerpc.Node)
		newStorageServer.nodesList = make([]storagerpc.Node, 0)
		thisNode := storagerpc.Node{newStorageServer.hostport, nodeID}
		newStorageServer.nodeIdMap[nodeID] = thisNode // save this master into the server hashmap
		newStorageServer.nodesList = append(newStorageServer.nodesList, thisNode)
		newStorageServer.serverFull = make(chan int, 1)
		newStorageServer.informedCount = 0

		// RPC Registration
		rpc.RegisterName("StorageServer", storagerpc.Wrap(newStorageServer))
		rpc.HandleHTTP()

		listener, err := net.Listen("tcp", newStorageServer.hostport)
		if err != nil {
			return nil, errors.New("Error : Master Storage Listen")
		}

		// listening to connection request
		go http.Serve(listener, nil)

		// 如果ring里面只有一个server， 那就是master server本身，直接解锁就好
		if numNodes == 1 {
			newStorageServer.serverFull <- 1
		}
		<-newStorageServer.serverFull
		newStorageServer.storageServerReady = true

	} else {

		// if this is the Slave server

		masterStorageServer, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, errors.New("")
		}

		// register this slave
		registerArgs := &storagerpc.RegisterArgs{ServerInfo: storagerpc.Node{HostPort: fmt.Sprintf("localhost:%d", port), NodeID: nodeID}}
		var registerReply storagerpc.RegisterReply
		masterStorageServer.Call("StorageServer.RegisterServer", registerArgs, &registerReply)
		// if register fails, wait for 1 sec and try another time
		for registerReply.Status != storagerpc.OK {
			time.Sleep(time.Second)
			masterStorageServer.Call("StorageServer.RegisterServer", registerArgs, &registerReply)
		}

		// get the list of all member storage server in the ring
		newStorageServer.nodesList = registerReply.Servers

		rpc.RegisterName("StorageServer", storagerpc.Wrap(newStorageServer))
		rpc.HandleHTTP()

		listener, listen_err := net.Listen("tcp", newStorageServer.hostport)
		if listen_err != nil {
			return nil, errors.New("Error : Slave Storage Listen")
		}

		go http.Serve(listener, nil)

		newStorageServer.storageServerReady = true
	}

	tmp := Nodes(newStorageServer.nodesList)
	sort.Sort(tmp)
	newStorageServer.nodesList = ([]storagerpc.Node)(tmp)
	go newStorageServer.CheckLease()
	return newStorageServer, nil

}

// 只有master server会使用这个方法
// slave server则是从外部call master server的这个方法
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	serverInfo := args.ServerInfo
	_, ok := ss.nodeIdMap[serverInfo.NodeID]

	// 如果这个server node没有被register过， 放到node map里面
	if !ok {
		ss.nodeIdMap[serverInfo.NodeID] = args.ServerInfo
		ss.nodesList = append(ss.nodesList, args.ServerInfo)
	}

	// 如果当前node总数量少于hash ring里面规定的总数量，则没有ready
	if len(ss.nodeIdMap) < ss.nodeSize {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodesList

		ss.cMutex.Lock()
		ss.informedCount += 1
		ss.cMutex.Unlock()
		if ss.informedCount == ss.nodeSize-1 {
			ss.serverFull <- 1
		}
	}

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if ss.storageServerReady {
		reply.Status = storagerpc.OK
		tmp := make([]storagerpc.Node, len(ss.nodesList), cap(ss.nodesList))
		copy(tmp, ss.nodesList)
		reply.Servers = tmp
	} else {
		reply.Status = storagerpc.NotReady
	}

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if args == nil {
		return errors.New("Storage Server Get: No Args Input")
	}

	if reply == nil {
		return errors.New("Storage Server Get: No Reply Input")
	}

	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// 1. Lock the keyLockMap access so as to find the lock for this specific key.
	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]

	if !exist {
		// create new lock for this key
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	// Step 2: Release the sMutex lock so that the ss can serve other GET requests.
	// Meanwhile, since we are dealing with lease related to args.Key, we must lock
	// it using its own lock, keyLock.

	granted := false
	keyLock.Lock()
	defer keyLock.Unlock()

	if args.WantLease {
		leasedLibStores, ok := ss.leaseMap[args.Key]
		if !ok {
			leasedLibStores = make(map[string]time.Time)
			ss.leaseMap[args.Key] = leasedLibStores
		}
		ss.leaseMap[args.Key][args.HostPort] = time.Now()
		granted = true
	}

	reply.Lease = storagerpc.Lease{granted, storagerpc.LeaseSeconds}
	v, ok := ss.sMap[args.Key]

	if ok {
		reply.Value = v.value
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if args == nil {
		return errors.New("Storage Server Delete: No Args Input")
	}

	if reply == nil {
		return errors.New("Storage Server Delete: No Reply Input")
	}

	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// create the lock for this key
	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]

	if !exist {
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}

	ss.sMutex.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	_, ok := ss.sMap[args.Key]

	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	hpTimeMap, leaseExists := ss.leaseMap[args.Key]

	// 如果存在lease, 则要从所有的libstore里面删除这个key
	if leaseExists {

		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(hpTimeMap)

		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)
		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		<-finishChan

		delete(ss.leaseMap, args.Key)
	}

	delete(ss.sMap, args.Key)
	reply.Status = storagerpc.OK
	return nil

}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if args == nil {
		return errors.New("Storage Server GetList: No Args Input")
	}

	if reply == nil {
		return errors.New("Storage Server GetList: No Reply Input")
	}

	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]

	if !exist {
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	granted := false
	keyLock.Lock()
	defer keyLock.Unlock()

	if args.WantLease {
		leasedLibStores, ok := ss.leaseMap[args.Key]
		if !ok {
			leasedLibStores = make(map[string]time.Time)
			ss.leaseMap[args.Key] = leasedLibStores
		}
		ss.leaseMap[args.Key][args.HostPort] = time.Now()
		granted = true
	}

	reply.Lease = storagerpc.Lease{granted, storagerpc.LeaseSeconds}

	retList, ok := ss.lMap[args.Key]
	if ok {
		ret := make([]string, len(retList.value), cap(retList.value))
		copy(ret, retList.value)
		reply.Value = ret
		reply.Status = storagerpc.OK
	} else {
		reply.Value = make([]string, 0, 0)
		reply.Status = storagerpc.KeyNotFound
	}

	return nil

}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("Storage Server Put: No Args Input")
	}

	if reply == nil {
		return errors.New("Storage Server Put: No Reply Input")
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	hpTimeMap, exist := ss.leaseMap[args.Key]

	if exist {
		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(ss.leaseMap[args.Key])
		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)

		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		<-finishChan
		delete(ss.leaseMap, args.Key)
	}

	newValue := sValue{
		value: args.Value,
	}
	ss.sMap[args.Key] = &newValue
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("Storage Server AppendToList: No Args Input")
	}

	if reply == nil {
		return errors.New("Storage Server AppendToList: No Reply Input")
	}

	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]
	if !exist {
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	hpTimeMap, exist := ss.leaseMap[args.Key]
	if exist {
		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(ss.leaseMap[args.Key])

		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)

		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		<-finishChan

		delete(ss.leaseMap, args.Key)
	}

	retList, ok := ss.lMap[args.Key]
	if ok {
		for _, v := range retList.value {
			if v == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
		retList.value = append(retList.value, args.Value)
	} else {
		newValue := lValue{
			value: make([]string, 1),
		}
		newValue.value[0] = args.Value
		ss.lMap[args.Key] = &newValue
	}

	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if args == nil {
		return errors.New("Storage Server AppendToList: No Args Input")
	}

	if reply == nil {
		return errors.New("Storage Server AppendToList: No Reply Input")
	}

	if !(ss.CheckKeyInRange(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.sMutex.Lock()
	keyLock, exist := ss.keyLockMap[args.Key]

	if !exist {
		keyLock = &sync.Mutex{}
		ss.keyLockMap[args.Key] = keyLock
	}
	ss.sMutex.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	hpTimeMap, exist := ss.leaseMap[args.Key]

	if exist {
		successChan := make(chan int, 1)
		finishChan := make(chan int, 1)
		expected := len(ss.leaseMap[args.Key])

		go ss.CheckRevokeStatus(args.Key, successChan, finishChan, expected)

		for hp, _ := range hpTimeMap {
			go ss.RevokeLeaseAt(hp, args.Key, successChan)
		}

		delete(ss.leaseMap, args.Key)
	}

	retList, ok := ss.lMap[args.Key]
	if ok {
		for i, v := range retList.value {
			if v == args.Value {
				retList.value = append(retList.value[:i], retList.value[i+1:]...)
				reply.Status = storagerpc.OK
				return nil
			}
		}
	}
	reply.Status = storagerpc.ItemNotFound
	return nil

}

/* 判断一个key是不是在这个server内 */
func (ss *storageServer) CheckKeyInRange(key string) bool {
	hashValue := libstore.StoreHash(key)
	serverId := 0

	for i := 0; i < len(ss.nodesList); i++ {
		if i == len(ss.nodesList)-1 {
			serverId = 0
			break
		}

		if hashValue > ss.nodesList[i].NodeID && hashValue <= ss.nodesList[i+1].NodeID {
			serverId = i + 1
			break
		}
	}

	return ss.nodesList[serverId].NodeID == ss.nodeId
}

/*意义不明 */
func (ss *storageServer) CheckLease() {
	expireTime := time.Duration(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) * time.Second
	for {
		time.Sleep(100 * time.Millisecond)
		for k, hostTimeMap := range ss.leaseMap {
			for hp, tt := range hostTimeMap {
				if time.Since(tt) > expireTime {
					delete(ss.leaseMap[k], hp)
				}
			}
		}

	}
}

func (ss *storageServer) CheckRevokeStatus(key string, successChan, finishChan chan int, expected int) {
	count := 0
	for {
		select {
		case <-successChan:
			count += 1
			mapLen := len(ss.leaseMap[key])
			if count == expected || mapLen == 0 {
				finishChan <- 1
				return
			}
		case <-time.After(500 * time.Millisecond):
			if len(ss.leaseMap[key]) == 0 {
				finishChan <- 1
				return
			}
		}
	}
}

func (ss *storageServer) RevokeLeaseAt(hostport, key string, successChan chan int) {
	ss.libMutex.Lock()
	targetLib, ok := ss.libStoreMap[hostport]
	if !ok {
		var err error
		targetLib, err = rpc.DialHTTP("tcp", hostport)
		if err != nil {
			ss.libMutex.Unlock()
			return
		}
		ss.libStoreMap[hostport] = targetLib
	}

	ss.libMutex.Unlock()

	args := &storagerpc.RevokeLeaseArgs{key}
	var reply storagerpc.RevokeLeaseReply

	revokeErr := targetLib.Call("LeaseCallbacks.RevokeLease", args, &reply)
	if revokeErr != nil {
		return
	}

	successChan <- 1

}
