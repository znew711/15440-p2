package tribserver

import (
	"errors"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"time"
	"log"
	"os"
)

type tribServer struct {
	// TODO: implement this!
	//tribserver *rpc.Server
	listener   net.Listener
	storagecli *rpc.Client
	ls         libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	l := log.New(os.Stderr, "", 0)
	tribServer := new(tribServer)

    // Create the server socket that will listen for incoming RPCs.
    listener, err := net.Listen("tcp", myHostPort)
    if err != nil {
    	fmt.Println("listen fails")
        return nil, err
    }

    // Wrap the tribServer before registering it for RPC.
    err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
    if err != nil {
    	fmt.Println("could not register tribserver")
        return nil, err
    }

    // create new libstore for communication with storage server
   	//var ls libstore.Libstore
    connected := false
    var ls libstore.Libstore
	for numTries := 0; numTries < 5; numTries++ {
		ls, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
		if err != nil {
			time.Sleep(1 * time.Second)
		} else {
			tribServer.ls = ls
			connected = true
			break
		}
	}
	if !connected {
		fmt.Println("could not find storage server")
		return nil, errors.New("Libstore could not connect to storage server.")
	}

    rpc.HandleHTTP()
    go http.Serve(listener, nil)

    l.Println("Help")
    tribServer.listener = listener

	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userKey := util.FormatUserKey(args.UserID)
	if ts.ls == nil {
		fmt.Println("FATAL ERROR ABORT ABORT")
	}
	if _, err := ts.ls.Get(userKey); err == nil {
		// user already exists
		reply.Status = tribrpc.Exists
		return nil
	}

	err := ts.ls.Put(userKey, args.UserID) // just the user string??? what is the value of a user
	if err != nil {
		// put shouldn't return any errors that are visible to the tribserver
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	sublistKey := util.FormatSubListKey(args.UserID)
	sublistRKey := util.FormatSubListRKey(args.TargetUserID)
	userKey := util.FormatUserKey(args.UserID)
	targetKey := util.FormatUserKey(args.TargetUserID)

	if _, err := ts.ls.Get(userKey); err != nil {
		// user not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if _, err := ts.ls.Get(targetKey); err != nil {
		// user not found
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// add terget to user's sublist
	err := ts.ls.AppendToList(sublistKey, args.TargetUserID)
	if err != nil && err.Error() == "Item exists." {
		reply.Status = tribrpc.Exists
		return nil
	} else if err != nil {
		return err
	}

	// mark user as subscribed to target
	err = ts.ls.AppendToList(sublistRKey, args.UserID)
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	sublistKey := util.FormatSubListKey(args.UserID)
	sublistRKey := util.FormatSubListRKey(args.TargetUserID)
	userKey := util.FormatUserKey(args.UserID)
	targetKey := util.FormatUserKey(args.TargetUserID)

	if _, err := ts.ls.Get(userKey); err != nil {
		// user not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if _, err := ts.ls.Get(targetKey); err != nil {
		// user not found
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// remove target from user's sublist
	err := ts.ls.RemoveFromList(sublistKey, args.TargetUserID)
	if err != nil && err.Error() == "Item not found." {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	} else if err != nil {
		return err
	}

	// remove user from target's subscribee list
	err = ts.ls.RemoveFromList(sublistRKey, args.UserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	userKey := util.FormatUserKey(args.UserID)
	sublistKey := util.FormatSubListKey(args.UserID)
	sublistRKey := util.FormatSubListRKey(args.UserID)
	if _, err := ts.ls.Get(userKey); err != nil {
		if err.Error() == "Key not found." {
			reply.Status = tribrpc.NoSuchUser
			return nil
		} else {
			return err
		}
	}

	subscribedList, err := ts.ls.GetList(sublistKey)
	if err != nil {
		if err.Error() == "Key not found." {
			reply.Status = tribrpc.OK
			return nil
		} else {
			return err
		}
	}
	subscribeeList, err := ts.ls.GetList(sublistRKey)
	if err != nil {
		return err
	}
	var friends []string
	var subscribedMap = make(map[string]bool)
	for _, elem := range subscribedList {
		subscribedMap[elem] = true
	}
	for _, elem := range subscribeeList {
		if _, ok := subscribedMap[elem]; ok == true {
			friends = append(friends, elem)
		}
	}
	reply.UserIDs = friends
	reply.Status = tribrpc.OK
	return nil
}

// as of now: triblistkey -> list of postkeys?? or do we need to hash?
//   then, postkey -> json-encoded tribble
func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.ls.Get(userKey); err != nil {
		// key not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	time := time.Now()
	newTribble, err := json.Marshal(&tribrpc.Tribble{UserID: args.UserID, Posted: time, Contents: args.Contents})
	if err != nil {
		return err
	}
	tribListKey := util.FormatTribListKey(args.UserID)
	postKey := util.FormatPostKey(args.UserID, time.UnixNano())

	err = ts.ls.AppendToList(tribListKey, postKey)
	if err != nil && err.Error() == "Item exists." {
		reply.Status = tribrpc.Exists
		return nil
	} else if err != nil {
		return err
	}

	err = ts.ls.Put(postKey, string(newTribble))
	if err != nil && err.Error() == "Item exists." {
		reply.Status = tribrpc.Exists
		return nil
	} else if err != nil {
		return err
	}

	reply.PostKey = postKey
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.ls.Get(userKey); err != nil {
		// key not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	tribListKey := util.FormatTribListKey(args.UserID)
	err := ts.ls.RemoveFromList(tribListKey, args.PostKey)
	if err != nil && err.Error() == "Item not found." {
		reply.Status = tribrpc.NoSuchPost
		return nil
	} else if err != nil {
		return err
	}

	err = ts.ls.Delete(args.PostKey)
	if err != nil && err.Error() == "Key not found." {
		reply.Status = tribrpc.NoSuchPost
		return nil
	} else if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.ls.Get(userKey); err != nil {
		// key not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	tribListKey := util.FormatTribListKey(args.UserID)
	postKeys, err := ts.ls.GetList(tribListKey)
	if err != nil && err.Error() == "Key not found." {
		// this is ok, user just hasn't posted
		reply.Status = tribrpc.OK
		return nil
	} else if err != nil {
		return err
	}
	// reverse postkeys to get them in reverse chronological order
	for i := len(postKeys)/2 - 1; i >= 0; i-- {
		opp := len(postKeys) - 1 - i
		postKeys[i], postKeys[opp] = postKeys[opp], postKeys[i]
	}

	tribbles := []tribrpc.Tribble{}
	// get actual tribbles using found postkeys
	for i := 0; i < 100 && i < len(postKeys); i++ {
		postKey := postKeys[i]
		jsonTribble, err := ts.ls.Get(postKey)
		if err != nil && err.Error() == "Key not found." {
			continue
		}
		if err != nil {
			// something has gone terribly wrong??
			return err
		}

		tribble := tribrpc.Tribble{}
		err = json.Unmarshal([]byte(jsonTribble), &tribble)
		if err != nil {
			return err
		}
		tribbles = append(tribbles, tribble)
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.ls.Get(userKey); err != nil {
		// user not=t found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subListKey := util.FormatSubListKey(args.UserID)
	subscribed, err := ts.ls.GetList(subListKey)
	if err != nil && err.Error() == "Key not found." {
		// this is ok, user just hasn't subscribed to anyone
		reply.Status = tribrpc.OK
		return nil
	} else if err != nil {
		return err
	}

	// get all post keys from all subscribed users
	subscribedPostKeys := []string{}
	for _, sub := range subscribed {
		postKeys, err := ts.ls.GetList(util.FormatTribListKey(sub))
		if err != nil && err.Error() == "Key not found." {
			// this is ok, subscribed user just hasn't posted
			continue
		} else if err != nil {
			return err
		}
		subscribedPostKeys = append(subscribedPostKeys, postKeys...)
	}
	// sort in reverse chronological order
	sort.Sort(ByReverseTime(subscribedPostKeys))
	// get a maximum of 100 keys
	if len(subscribedPostKeys) > 100 {
		subscribedPostKeys = subscribedPostKeys[0:100]
	}

	// get corresponding tribbles
	subscribedTribbles := []tribrpc.Tribble{}
	for _, postKey := range subscribedPostKeys {
		jsonTribble, err := ts.ls.Get(postKey)
		if err != nil && err.Error() == "Key not found." {
			// BUG: need to grab another possible tribble from subscribedpostkeys??
			continue
		}
		if err != nil {
			return err
		}

		tribble := tribrpc.Tribble{}
		err = json.Unmarshal([]byte(jsonTribble), &tribble)
		if err != nil {
			return err
		}
		subscribedTribbles = append(subscribedTribbles, tribble)
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = subscribedTribbles
	return nil
}

type ByReverseTime []string

func (postKeys ByReverseTime) Len() int      { return len(postKeys) }
func (postKeys ByReverseTime) Swap(i, j int) { postKeys[i], postKeys[j] = postKeys[j], postKeys[i] }
func (postKeys ByReverseTime) Less(i, j int) bool {
	postKey1 := strings.Split(postKeys[i], ":")[1]
	postKey2 := strings.Split(postKeys[j], ":")[1]
	time1, _ := strconv.ParseInt(strings.Split(postKey1, "_")[1], 16, 64)
	time2, _ := strconv.ParseInt(strings.Split(postKey2, "_")[1], 16, 64)

	// sort so that list is in REVERSE chronological order
	return time1 > time2
}
