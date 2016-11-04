package tribserver

import (
	"errors"
	"encoding/json"
	"net"
	"net/http"
	"net/rpc"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"time"
)

type tribServer struct {
	// TODO: implement this!
	//tribserver *rpc.Server
	listener net.Listener
	storagecli *rpc.Client
	ls libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	tribServer := new(tribServer)

    // Create the server socket that will listen for incoming RPCs.
    listener, err := net.Listen("tcp", myHostPort)
    if err != nil {
        return nil, err
    }

    // Wrap the tribServer before registering it for RPC.
    err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
    if err != nil {
        return nil, err
    }
/*
    cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
    if err != nil {
		return nil, err
	}
    // Setup the HTTP handler that will serve incoming RPCs and
    // serve requests in a background goroutine.
*/

	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	tribServer.ls = ls
    rpc.HandleHTTP()
    go http.Serve(listener, nil)

    //tribServer.storagecli = cli
    tribServer.listener = listener


	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.ls.Get(userKey); err == nil {
		// key exists
		reply.Status = tribrpc.Exists
		return nil // TODO: maybe incorrect??? i'm not sure what errors to return
	} 

	err := ts.ls.Put(userKey, args.UserID) // just the user string??? what is the value of a user
	if err != nil {
		// put shouldn't return any errors that are visible to the tribserver?
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	sublistKey := util.FormatSubListKey(args.UserID)
	userKey := util.FormatUserKey(args.UserID)
	targetKey := util.FormatUserKey(args.TargetUserID)

	if _, err := ts.ls.Get(userKey); err != nil {
		// key not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if _, err := ts.ls.Get(targetKey); err != nil {
		// key not found
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	err := ts.ls.AppendToList(sublistKey, targetKey)
	if err != nil && err.Error() == "Item exists." {
		reply.Status = tribrpc.Exists
		return nil
	} else if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	sublistKey := util.FormatSubListKey(args.UserID)
	userKey := util.FormatUserKey(args.UserID)
	targetKey := util.FormatUserKey(args.TargetUserID)

	if _, err := ts.ls.Get(userKey); err != nil {
		// key not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if _, err := ts.ls.Get(targetKey); err != nil {
		// key not found
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	err := ts.ls.RemoveFromList(sublistKey, targetKey)
	if err != nil && err.Error() == "Item not found." {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	} else if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	return errors.New("getfriends not implemented")
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
		// should this case even be here???
		reply.Status = tribrpc.Exists
		return nil
	} else if err != nil {
		return err
	}

	err = ts.ls.Put(postKey, string(newTribble))
	if err != nil && err.Error() == "Item exists." {
		// should this case even be here???
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
		// should this case even be here???
		reply.Status = tribrpc.NoSuchPost
		return nil
	} else if err != nil {
		return err
	}

	err = ts.ls.Delete(args.PostKey)
	if err != nil && err.Error() == "Key not found." {
		// should this case even be here???
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
	for i := len(postKeys)/2-1; i >= 0; i-- {
	    opp := len(postKeys)-1-i
	    postKeys[i], postKeys[opp] = postKeys[opp], postKeys[i]
	}

	tribbles := []tribrpc.Tribble{}

	for i := 0; i < 100 && i < len(postKeys); i++ {
		postKey := postKeys[i]
		jsonTribble, err := ts.ls.Get(postKey)
		if err != nil && err.Error() == "Key not found." {
			continue
		}
		if err != nil {
			// soemthing has gone terribly wrong??
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
		// key not found
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subListKey := util.FormatSubListKey(args.UserID)
	_, err := ts.ls.GetList(subListKey) // replace with subscribed once i write this
	if err != nil && err.Error() == "Key not found." {
		// this is ok, user just hasn't posted
		reply.Status = tribrpc.OK
		return nil
	} else if err != nil {
		return err
	}

	return nil
}
