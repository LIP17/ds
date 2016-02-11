package libstore

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type sCacheNode struct {
	value   string
	seconds int
	isValid bool
}

type lCacheNode struct {
	value   []string
	seconds int
	isValid bool
}

type libstore struct {

	// 保存master server和所有client server
	masterServer   *rpc.Client
	serverMap      map[uint32]*rpc.Client
	serverMapMutex *sync.Mutex
	serverList     []storagerpc.Node

	// 保存lease的信息
	myHostPort string
	mode       LeaseMode

	// Cache
	counter      map[string]int // 用来记录每个key被access的次数
	counterMutex *sync.Mutex
	sCache       map[string]*sCacheNode
	sMutex       *sync.Mutex
	lCache       map[string]*lCacheNode
	lMutex       *sync.Mutex
}

// 对所有cache node排序，用于一致性哈希
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

func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	newMasterServer, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply

	// 要去 ： 尝试5次链接，每次中间等待1秒
	for i := 0; i < 5; i++ {
		newMasterServer.Call("StorageServer.GetServers", args, &reply)
		if reply.Status == storagerpc.OK {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	// 如果等待了5秒之后链接还没有成功，返回
	if reply.Status != storagerpc.OK {
		return nil, nil
	}

	nodeList := Nodes(reply.Servers)
	sort.Sort(nodeList)
	nodeListSorted := ([]storagerpc.Node)(nodeList)

	newLibstore := &libstore{
		// 保存master server和所有client server
		masterServer:   newMasterServer,
		serverMap:      make(map[uint32]*rpc.Client),
		serverMapMutex: &sync.Mutex{},
		serverList:     nodeListSorted,

		// 保存lease的信息
		myHostPort: myHostPort,
		mode:       mode,

		// Cache
		counter:      make(map[string]int),
		counterMutex: &sync.Mutex{},
		sCache:       make(map[string]*sCacheNode),
		sMutex:       &sync.Mutex{},
		lCache:       make(map[string]*lCacheNode),
		lMutex:       &sync.Mutex{},
	}

	if mode != Never {
		rpc.RegisterName("LeaseCallbacks", librpc.Wrap(newLibstore))
	}

	go newLibstore.selfRevoke()
	go newLibstore.selfCleanCounter()

	return newLibstore, nil
}

// 定时清零所有的counter
func (ls *libstore) selfCleanCounter() {
	for {
		time.Sleep(storagerpc.QueryCacheSeconds * time.Second)
		ls.counterMutex.Lock()
		for k, _ := range ls.counter {
			ls.counter[k] = 0
		}
		ls.counterMutex.Unlock()
	}
}

func (ls *libstore) selfRevoke() {
	for {
		time.Sleep(time.Second)
		ls.sMutex.Lock()
		for k, v := range ls.sCache {
			if v.isValid {
				v.seconds = v.seconds - 1
				if v.seconds <= 0 {
					delete(ls.sCache, k)
				}
			}
		}
		ls.sMutex.Unlock()

		ls.lMutex.Lock()
		for k, v := range ls.lCache {
			if v.isValid {
				v.seconds = v.seconds - 1
				delete(ls.lCache, k)
			}
		}
		ls.lMutex.Unlock()

	}
}

func (ls *libstore) Get(key string) (string, error) {

	// 1. check cache

	ls.sMutex.Lock()
	node, ok := ls.sCache[key]

	if ok {
		if node.isValid {
			ret := node.value
			ls.sMutex.Unlock()
			return ret, nil
		}
	}
	ls.sMutex.Unlock()

	// 2. increment the counter of this key
	ls.counterMutex.Lock()
	num, ok := ls.counter[key]
	if ok {
		ls.counter[key] = num + 1
	} else {
		ls.counter[key] = 1
	}
	ls.counterMutex.Unlock()

	var args *storagerpc.GetArgs

	// ok 表示这个key存在在counter里
	if ok && num >= storagerpc.QueryCacheThresh {
		// true表示需要lease
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	} else {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	// 如果模式是Always, 则永远需要lease
	// 如果是Never， 则永远不要lease

	if ls.mode == Always {
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	}
	if ls.mode == Never {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	var reply storagerpc.GetReply

	storageServer, getServerErr := ls.GetStorageServer(key)
	if getServerErr != nil {
		return "", getServerErr
	}

	getErr := storageServer.Call("StorageServer.Get", args, &reply)
	if getErr != nil {
		return "", getErr
	}

	if reply.Status != storagerpc.OK {
		return "", errors.New("Libstore Get : Status not OK from StorageServer")
	}

	if args.WantLease && reply.Lease.Granted {
		ls.sMutex.Lock()
		ls.sCache[key] = &sCacheNode{
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds,
			isValid: true,
		}
		ls.sMutex.Unlock()
	}

	return reply.Value, nil

}

// put 总是following get， 所以不需要更新他的lease状况
func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{key, value}
	var reply *storagerpc.PutReply

	storageServer, getServerErr := ls.GetStorageServer(key)
	if getServerErr != nil {
		return getServerErr
	}

	putErr := storageServer.Call("StorageServer.Put", args, &reply)
	if putErr != nil {
		return putErr
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Libstore Put : Status not OK.")
	}

	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{key}
	var reply *storagerpc.DeleteReply

	storageServer, getServerErr := ls.GetStorageServer(key)
	if getServerErr != nil {
		return getServerErr
	}

	err := storageServer.Call("StorageServer.Delete", args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Libstore Delete : Status not OK.")
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// 1. check cache
	ls.lMutex.Lock()
	node, ok := ls.lCache[key]
	if ok {
		if node.isValid {
			ret := node.value
			ls.lMutex.Unlock()
			return ret, nil
		}
	}

	ls.lMutex.Unlock()

	// 2. cache not found, increment the counter
	ls.counterMutex.Lock()
	num, ok := ls.counter[key]
	if ok {
		ls.counter[key] = num + 1
	} else {
		ls.counter[key] = 1
	}
	ls.counterMutex.Unlock()

	// 3. get list from server
	var args *storagerpc.GetArgs

	if ok && num >= storagerpc.QueryCacheThresh {
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	} else {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	// 4. check Always or Never
	if ls.mode == Always {
		args = &storagerpc.GetArgs{key, true, ls.myHostPort}
	}

	if ls.mode == Never {
		args = &storagerpc.GetArgs{key, false, ls.myHostPort}
	}

	var reply storagerpc.GetListReply

	storageServer, getServerError := ls.GetStorageServer(key)
	if getServerError != nil {
		return nil, getServerError
	}

	getListError := storageServer.Call("StorageServer.GetList", args, &reply)
	if getListError != nil {
		return nil, getListError
	}

	if reply.Status != storagerpc.OK {
		return nil, errors.New("Libstore GetList : Status Not Ok.")
	}

	if args.WantLease && reply.Lease.Granted {
		ls.lMutex.Lock()
		ls.lCache[key] = &lCacheNode{
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds,
			isValid: true,
		}
		ls.lMutex.Unlock()
	}
	return reply.Value, nil

}

// 每个用户的信息对应一个服务器（用户名对应一个hash对应一个服务器）
func (ls *libstore) GetStorageServer(key string) (*rpc.Client, error) {

	hashValue := StoreHash(key)
	serverNode := ls.serverList[ls.GetStorageServerId(hashValue)]

	storageServer, ok := ls.serverMap[serverNode.NodeID]

	if !ok {
		ls.serverMapMutex.Lock()
		defer ls.serverMapMutex.Unlock()

		newStorageServer, getStorageServerErr := rpc.DialHTTP("tcp", serverNode.HostPort)
		if getStorageServerErr != nil {
			return nil, getStorageServerErr
		}

		ls.serverMap[serverNode.NodeID] = newStorageServer
		return newStorageServer, nil
	}

	return storageServer, nil
}

func (ls *libstore) GetStorageServerId(hashValue uint32) uint32 {
	serverId := 0

	// Note: ls.allServerNodes is SORTED.
	for i := 0; i < len(ls.serverList); i++ {
		if i == len(ls.serverList)-1 {
			return 0
		}

		if hashValue > ls.serverList[i].NodeID && hashValue <= ls.serverList[i+1].NodeID {
			serverId = i + 1
			break
		}

	}
	return uint32(serverId)
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{key, removeItem}
	var reply *storagerpc.PutReply

	storageServer, getServerErr := ls.GetStorageServer(key)
	if getServerErr != nil {
		return getServerErr
	}

	RemoveErr := storageServer.Call("StorageServer.RemoveFromList", args, &reply)
	if RemoveErr != nil {
		return RemoveErr
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Libstore RemoveFromList : Status not OK.")
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{key, newItem}
	var reply *storagerpc.PutReply

	storageServer, getServerErr := ls.GetStorageServer(key)
	if getServerErr != nil {
		return getServerErr
	}

	AppendErr := storageServer.Call("StorageServer.AppendToList", args, &reply)
	if AppendErr != nil {
		return AppendErr
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Append To List : Status not OK.")
	}

	return nil
}

// called from StorageServer to revoke the
// valid status of lease
func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.sMutex.Lock()
	_, ok := ls.sCache[args.Key]

	if ok {
		delete(ls.sCache, args.Key)
		ls.sMutex.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}

	ls.sMutex.Unlock()

	ls.lMutex.Lock()
	_, ok = ls.lCache[args.Key]
	if ok {
		delete(ls.lCache, args.Key)
		ls.lMutex.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}

	ls.lMutex.Unlock()

	// if the key is not in the list cache and singlevalue cache
	reply.Status = storagerpc.KeyNotFound
	return nil

}
