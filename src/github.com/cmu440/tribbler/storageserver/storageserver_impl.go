package storageserver

import (
	//"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"
)

type storageServer struct {
	Data             map[string]string
	ListData         map[string][]string
	NodeId           uint32
	NodesJoined      int
	ExpectedNumNodes int
	Nodes            []storagerpc.Node
	Listener         *net.Listener
	SlaveJoined      chan bool
	Ready            bool
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
	ss := storageServer{
		Data:             make(map[string] string),
		ListData:         make(map[string] []string),
		NodesJoined:      1,
		ExpectedNumNodes: numNodes,
		Nodes:            make([]storagerpc.Node, numNodes),
		NodeId:           nodeID,
		Listener:         nil,
		SlaveJoined:      make(chan bool, 5),
		Ready:            false,
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
				ss.Ready = true
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
	if ss.Ready {
		reply.Status = storagerpc.OK
		reply.Servers = ss.Nodes
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	i, ok := ss.Data[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = i
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	_, ok := ss.Data[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		delete(ss.Data, args.Key)
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	i, ok := ss.ListData[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = i
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.Data[args.Key] = args.Value
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	list, ok := ss.ListData[args.Key]
	if !ok {
		ss.ListData[args.Key] = []string{args.Value}
		reply.Status = storagerpc.OK
	} else {
		found := false
		for i := 0; i < len(list); i++ {
			if list[i] == args.Value {
				found = true
			}
		}
		if found {
			reply.Status = storagerpc.ItemExists
		} else {
			ss.ListData[args.Key] = append(list, args.Value)
			reply.Status = storagerpc.OK
		}
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	_, ok := ss.ListData[args.Key]
	if !ok {
		reply.Status = storagerpc.ItemNotFound
	} else {
		found := false
		for index, element := range ss.ListData[args.Key] {
			if element == args.Value {
				found = true
			}
			if found && index < len(ss.ListData[args.Key]) -1 {
				ss.ListData[args.Key][index] = ss.ListData[args.Key][index + 1]
			}
		}
		if found {
			length := len(ss.ListData[args.Key])
			ss.ListData[args.Key] = ss.ListData[args.Key][:length - 1]
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
	}
	return nil
}
