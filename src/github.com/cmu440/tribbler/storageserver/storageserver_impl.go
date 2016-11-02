package storageserver

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"
)

type storageServer struct {
	Data             map[string][]byte
	NodeId           uint32
	NodesJoined      int
	ExpectedNumNodes int
	Nodes            []storagerpc.Node
	Listener         *net.Listener
	SlaveJoined      chan bool
}

//hello world

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := storageServer{
		Data:             make(map[string] []byte),
		NodesJoined:      1,
		ExpectedNumNodes: numNodes,
		Nodes:            make([]storagerpc.Node, numNodes),
		NodeId:           nodeID,
		Listener:         nil,
		SlaveJoined:      make(chan bool, 5),
	}
	rpc.RegisterName("StorageServer", storagerpc.Wrap(&ss))
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	ss.Listener = &l
	go http.Serve(l, nil)

	if masterServerHostPort == "" {
		//master
		ss.Nodes[0] = storagerpc.Node{"localhost:" + strconv.Itoa(port), nodeID}
		needMoreSlaves := true
		for needMoreSlaves {
			select {
			case <-ss.SlaveJoined:
				ss.NodesJoined += 1
				if ss.NodesJoined == numNodes {
					needMoreSlaves = false
				}
			}
		}
	} else {
		//we are a slave
		masterConn, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{"localhost:" + strconv.Itoa(port), nodeID},
		}
		var reply storagerpc.RegisterReply
		notReady := true
		for notReady {
			if err := masterConn.Call("StorageServer.RegisterServer", args, &reply); err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				ss.Nodes = reply.Servers
				notReady = false
				ss.ExpectedNumNodes = len(reply.Servers)
			} else {
				time.Sleep(time.Second)
			}
		}
	}
	return &ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	found := false
	for i := 0; i < ss.ExpectedNumNodes; i++ {
		if ss.Nodes[i].HostPort == args.ServerInfo.HostPort {
			found = true
		}
	}
	if !found {
		ss.Nodes[ss.NodesJoined] = args.ServerInfo
		ss.NodesJoined += 1
	}
	if ss.NodesJoined < ss.ExpectedNumNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
		reply.Servers = ss.Nodes
	}
	ss.SlaveJoined <- true
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
