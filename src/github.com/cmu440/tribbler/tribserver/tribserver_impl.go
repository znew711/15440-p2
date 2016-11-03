package tribserver

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
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
	//userid := args.UserID
	return errors.New("not implemented")
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	//userid := args.UserID

	return errors.New("not implemented")
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// two gets: first get 
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
