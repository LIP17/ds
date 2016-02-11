package tribserver

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strings"
	"time"
)

type tribServer struct {
	// TODO: implement this!
	ls libstore.Libstore
}

// define this type and Len() Less() and Swap() function for sort
// according to time
type tribIDs []string

func (tribIDList tribIDs) Len() int {
	return len(tribIDList)
}

func (tribIDList tribIDs) Less(i, j int) bool {

	// the original trib_key look like
	// userid:post_UnixNanoTime_randomnumber
	// 不管是针对一个人的tribble排序，还是对它订阅的所有人排序，排序只与UnixNanoTime有关
	// 但是针对多人的排序，userid可能会影响排序，所以直接取从UnixNanoTime开始的substring

	// For string i
	si := tribIDList[i]
	underlineIndex := strings.Index(si, "_")
	subSi := si[underlineIndex+1:]

	// For string j
	sj := tribIDList[j]
	underlineIndex = strings.Index(sj, "_")
	subSj := sj[underlineIndex+1:]

	comp := strings.Compare(subSi, subSj)

	if comp > 0 {
		return true
	} else {
		return false
	}
}

func (tribIDList tribIDs) Swap(i, j int) {
	tribIDList[i], tribIDList[j] = tribIDList[j], tribIDList[i]
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)

	if err != nil {
		return nil, errors.New("Creating new libstore failed")
	}

	newTribServer := &tribServer{
		ls: libstore,
	}

	// listener to TribClient
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	err = rpc.RegisterName("TribServer", tribrpc.Wrap(newTribServer))
	if err != nil {
		return nil, err
	}

	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return newTribServer, nil

}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	// userNotExistError : if user exist, the error will be nil
	_, userNotExistError := ts.ls.Get(useridkey)
	if userNotExistError == nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	ts.ls.Put(useridkey, userid)

	reply.Status = tribrpc.OK
	return nil

}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// get this userid and target user id
	userid := args.UserID
	targetid := args.TargetUserID
	useridkey := util.FormatUserKey(userid)
	targetidkey := util.FormatUserKey(targetid)

	// test if user exist
	_, userExist := ts.ls.Get(useridkey)

	if userExist != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// test if target user exist
	_, targetUserExist := ts.ls.Get(targetidkey)

	if targetUserExist != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	userSubListKey := util.FormatSubListKey(userid)

	// if target user exist in the sublist, there will be error
	targetUserExistErr := ts.ls.AppendToList(userSubListKey, targetid)
	if targetUserExistErr != nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// get this userid and target user id
	userid := args.UserID
	targetid := args.TargetUserID
	useridkey := util.FormatUserKey(userid)
	targetidkey := util.FormatUserKey(targetid)

	// test if user exist
	_, userExists := ts.ls.Get(useridkey)

	if userExists != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// test if target user exist
	_, targetExists := ts.ls.Get(targetidkey)

	if targetExists != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	userSubListKey := util.FormatSubListKey(userid)

	// if the target user is not in the sublist
	targetUserNotExistErr := ts.ls.RemoveFromList(userSubListKey, targetid)
	if targetUserNotExistErr != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	// test if this user exist
	_, userExists := ts.ls.Get(useridkey)

	if userExists != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	nodes, err := ts.ls.GetList(util.FormatSubListKey(userid))

	// if this user subscribe to no one
	if err != nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = nil
		return nil
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = nodes
	return nil

}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if args == nil {
		return errors.New("TribServer : cannot post nil")
	}

	if reply == nil {
		return errors.New("TribServer : cannot reply with nil in a post")
	}

	// test if this user exist
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)
	_, err := ts.ls.Get(useridkey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		reply.PostKey = ""
		return nil
	}

	// FormatPostkey会随机生成一个数放到TribListKey里面，但是不排除随机数也有重复的可能
	// 所以要尝试直到这个key在storageserver里面不存在为止作为新的key
	curTime := time.Now()
	newTribID := util.FormatPostKey(userid, curTime.UnixNano())
	for ts.ls.AppendToList(util.FormatTribListKey(userid), newTribID) != nil {
		newTribID = util.FormatPostKey(userid, curTime.UnixNano())
	}

	//
	trib := &tribrpc.Tribble{
		UserID:   userid,
		Posted:   curTime,       // The exact time the tribble was posted.
		Contents: args.Contents, // The text/contents of the tribble message.
	}

	tribMarshalled, _ := json.Marshal(trib)

	/*
		这一块功能未知，做后端的时候再看看
	*/
	if ts.ls.Put(newTribID, string(tribMarshalled[:])) != nil {
		ts.ls.RemoveFromList(newTribID, newTribID)
		reply.Status = tribrpc.NoSuchUser
		reply.PostKey = ""
		return errors.New("Fatal, failed to add new post")
	}

	reply.Status = tribrpc.OK
	reply.PostKey = newTribID
	return nil

}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	if args == nil {
		return errors.New("TribServer : cannot post nil")
	}

	if reply == nil {
		return errors.New("TribServer : cannot reply with nil in a post")
	}

	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	// test if user exist
	_, err := ts.ls.Get(useridkey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// remove the trib
	if ts.ls.RemoveFromList(util.FormatTribListKey(userid), args.PostKey) != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	if ts.ls.Delete(args.PostKey) != nil {
		reply.Status = tribrpc.NoSuchPost
		return errors.New("fail to delete a key")
	}

	reply.Status = tribrpc.OK
	return nil

}

// 获得某个用户的最近的100个tribble
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	// test if user exist
	_, userExistError := ts.ls.Get(useridkey)
	if userExistError != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get all tribbles of the user
	tribIDList, err := ts.ls.GetList(util.FormatTribListKey(userid))
	if err != nil {
		// when there is no tribble of this user
		reply.Status = tribrpc.OK
		reply.Tribbles = nil
		return nil
	}

	// sort the trib according to UnixNano time
	tmp := tribIDs(tribIDList)
	sort.Sort(tmp)
	tribIDListTimed := []string(tmp)

	// get the most recent 100 tribbles
	MAX_LEN := 100

	if len(tribIDListTimed) < MAX_LEN {
		MAX_LEN = len(tribIDListTimed)
	}

	allTribs := make([]tribrpc.Tribble, MAX_LEN)
	var currTrib tribrpc.Tribble

	for i := 0; i < MAX_LEN; {
		trib, err := ts.ls.Get(tribIDListTimed[i])
		if err == nil {
			json.Unmarshal([]byte(trib), &currTrib)
			allTribs[i] = currTrib
			i += 1
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = allTribs
	return nil

}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if args == nil {
		return errors.New("ts: Can't getSubscription nil")
	}
	if reply == nil {
		return errors.New("ts: Can't reply with nil in getSubscription")
	}

	userid := args.UserID
	useridkey := util.FormatUserKey(userid)

	// test if user exist
	_, err := ts.ls.Get(useridkey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		reply.Tribbles = make([]tribrpc.Tribble, 0, 0)
		return nil
	}

	// 获得这个用户所订阅的所有用户对象的key
	users, err := ts.ls.GetList(util.FormatSubListKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = make([]tribrpc.Tribble, 0, 0)
		return nil
	}

	// 获得所有的tribble
	tribIDList := make([]string, 0)
	for _, userID := range users {
		lst, err := ts.ls.GetList(util.FormatTribListKey(userID))
		if err == nil {
			tribIDList = append(tribIDList, lst...)
		}
	}

	// 排序
	// sort the trib according to UnixNano time
	tmp := tribIDs(tribIDList)
	sort.Sort(tmp)
	tribIDListTimed := []string(tmp)

	// get the most recent 100 tribbles
	MAX_LEN := 100

	if len(tribIDListTimed) < MAX_LEN {
		MAX_LEN = len(tribIDListTimed)
	}

	allTribs := make([]tribrpc.Tribble, MAX_LEN)
	var currTrib tribrpc.Tribble

	for i := 0; i < MAX_LEN; {
		trib, err := ts.ls.Get(tribIDListTimed[i])
		if err == nil {
			json.Unmarshal([]byte(trib), &currTrib)
			allTribs[i] = currTrib
			i += 1
		}
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = allTribs
	return nil
}
