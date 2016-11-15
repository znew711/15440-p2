package storageserver

import (
	"errors"
	"fmt"
	"container/list"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type subLeaseStruct struct {
	HostPort  string
	GrantTime time.Time
}

type leaseStruct struct {
	revokeInProgress bool
	serverList       *list.List
}

type storageServer struct {
	Data             map[string]string
	DataMutex        *sync.Mutex
	ListData         map[string][]string
	ListDataMutex    *sync.Mutex
	NodeID           uint32
	NodesJoined      int
	ExpectedNumNodes int
	minHash          uint32
	Nodes            []storagerpc.Node
	Listener         *net.Listener
	SlaveJoined      chan bool
	Ready            bool
	Timer            *time.Ticker
	Leases           map[string]*leaseStruct
	LeasesMutex      *sync.Mutex
	RevokeQueue      chan string
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
		Data:             make(map[string]string),
		DataMutex:        &sync.Mutex{},
		ListData:         make(map[string][]string),
		ListDataMutex:    &sync.Mutex{},
		NodesJoined:      1,
		ExpectedNumNodes: numNodes,
		Nodes:            make([]storagerpc.Node, numNodes),
		NodeID:           nodeID,
		Listener:         nil,
		SlaveJoined:      make(chan bool, 5),
		Ready:            false,
		Timer:            time.NewTicker(time.Duration(storagerpc.LeaseGuardSeconds) * time.Second),
		Leases:           make(map[string]*leaseStruct),
		LeasesMutex:      &sync.Mutex{},
		RevokeQueue:      make(chan string),
	}
	rpc.RegisterName("StorageServer", storagerpc.Wrap(&ss))
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
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
				//fmt.Println("nodes needed: " + strconv.Itoa(numNodes) + " nodes now: " + strconv.Itoa(ss.NodesJoined))
				if ss.NodesJoined == numNodes {
					needMoreSlaves = false
				}
			}
		}
		ss.Ready = true
		ss.minHash = getMinHash(&ss)
	} else {
		//we are a slave
		var masterConn *rpc.Client = nil
		for tryCount := 0; tryCount < 5; tryCount++ {
			masterConn, err = rpc.DialHTTP("tcp", masterServerHostPort)
			if err != nil {
				//fmt.Println("SLAVE/MASTER ERROR 2 " + strconv.Itoa(int(nodeID)))
				fmt.Println(err)
				time.Sleep(time.Second)
			} else {
				break
			}
		}
		if masterConn == nil {
			return nil, errors.New("Connection refused too many times.")
		}
		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{"localhost:" + strconv.Itoa(port), nodeID},
		}
		var reply storagerpc.RegisterReply
		notReady := true
		for notReady {
			//fmt.Println("Slave: time to register myself! " + strconv.Itoa(int(ss.NodeID)))
			if err := masterConn.Call("StorageServer.RegisterServer", args, &reply); err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				//fmt.Println("Slave: master is ready! " + strconv.Itoa(int(ss.NodeID)))
				ss.Nodes = reply.Servers
				notReady = false
				ss.ExpectedNumNodes = len(reply.Servers)
				ss.Ready = true
			} else {
				//fmt.Println("Slave: master is not ready " + strconv.Itoa(int(ss.NodeID)))
				time.Sleep(time.Second)
			}
		}
		ss.minHash = getMinHash(&ss)
	}
	return &ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	found := false
	for i := 0; i < ss.ExpectedNumNodes; i++ {
		if ss.Nodes[i].NodeID == args.ServerInfo.NodeID {
			found = true
		}
	}
	if !found {
		//fmt.Println("registering new server " + strconv.Itoa(int(args.ServerInfo.NodeID)))
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
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.DataMutex.Lock()
	i, ok := ss.Data[args.Key]
	ss.DataMutex.Unlock()
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = i
		if args.WantLease {
			ss.LeasesMutex.Lock()
			val, ok := ss.Leases[args.Key]
			ss.LeasesMutex.Unlock()
			if ok {
				if val.revokeInProgress {
					reply.Lease = storagerpc.Lease {false, 0}
				} else {
					reply.Lease = storagerpc.Lease{true, storagerpc.LeaseSeconds}
					subLease := subLeaseStruct{
						HostPort:  args.HostPort,
						GrantTime: time.Now(),
					}
					val.serverList.PushBack(&subLease)
				}
			} else {
				reply.Lease = storagerpc.Lease{true, storagerpc.LeaseSeconds}
				ss.LeasesMutex.Lock()
				ss.Leases[args.Key] = &leaseStruct{
					revokeInProgress: false,
					serverList:       list.New(),
				}
				subLease := subLeaseStruct{
					HostPort:  args.HostPort,
					GrantTime: time.Now(),
				}
				ss.Leases[args.Key].serverList.PushBack(&subLease)
				ss.LeasesMutex.Unlock()
			}
		}
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.DataMutex.Lock()
	_, ok := ss.Data[args.Key]
	ss.DataMutex.Unlock()
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		ss.LeasesMutex.Lock()
		_, exists := ss.Leases[args.Key]
		ss.LeasesMutex.Unlock()
		if exists{
			callback := make(chan bool, 1)
			go handleRevoke(ss, args.Key, &callback)
			<- callback
		}
		ss.DataMutex.Lock()
		delete(ss.Data, args.Key)
		ss.DataMutex.Unlock()
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	//fmt.Println("entering get list")
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.ListDataMutex.Lock()
	i, ok := ss.ListData[args.Key]
	ss.ListDataMutex.Unlock()
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = i
		if args.WantLease {
			ss.LeasesMutex.Lock()
			val, ok := ss.Leases[args.Key]
			ss.LeasesMutex.Unlock()
			if ok {
				if val.revokeInProgress {
					reply.Lease = storagerpc.Lease{false, 0}
				} else {
					reply.Lease = storagerpc.Lease{true, storagerpc.LeaseSeconds}
					subLease := subLeaseStruct{
						HostPort:  args.HostPort,
						GrantTime: time.Now(),
					}
					val.serverList.PushBack(&subLease)
				}
			} else {
				reply.Lease = storagerpc.Lease{true, storagerpc.LeaseSeconds}
				ss.LeasesMutex.Lock()
				ss.Leases[args.Key] = &leaseStruct{
					revokeInProgress: false,
					serverList:       list.New(),
				}
				subLease := subLeaseStruct{
					HostPort:  args.HostPort,
					GrantTime: time.Now(),
				}
				ss.Leases[args.Key].serverList.PushBack(&subLease)
				ss.LeasesMutex.Unlock()
			}
		}
	}
	//fmt.Println("leaving get list")
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.LeasesMutex.Lock()
	_, exists := ss.Leases[args.Key]
	ss.LeasesMutex.Unlock()
	if exists {
		callback := make(chan bool, 1)
		go handleRevoke(ss, args.Key, &callback)
		<- callback
	}
	ss.DataMutex.Lock()
	ss.Data[args.Key] = args.Value
	ss.DataMutex.Unlock()
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//fmt.Println("entering AppendToList")
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.ListDataMutex.Lock()
	list, ok := ss.ListData[args.Key]
	ss.ListDataMutex.Unlock()
	if !ok {
		ss.ListDataMutex.Lock()
		ss.ListData[args.Key] = []string{args.Value}
		ss.ListDataMutex.Unlock()
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
			_, exists := ss.Leases[args.Key]
			if exists {
				callback := make(chan bool, 1)
				go handleRevoke(ss, args.Key, &callback)
				<- callback
			}
			ss.ListDataMutex.Lock()
			ss.ListData[args.Key] = append(ss.ListData[args.Key], args.Value)
			ss.ListDataMutex.Unlock()
			reply.Status = storagerpc.OK
		}
	}
	//fmt.Println("return from AppendToList")
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//fmt.Println("entering RemoveFromList")
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.ListDataMutex.Lock()
	_, ok := ss.ListData[args.Key]
	ss.ListDataMutex.Unlock()
	if !ok {
		reply.Status = storagerpc.ItemNotFound
	} else {
		found := false
		ss.ListDataMutex.Lock()
		for index, element := range ss.ListData[args.Key] {
			if element == args.Value {
				found = true
			}
			if found && index < len(ss.ListData[args.Key])-1 {
				ss.ListData[args.Key][index] = ss.ListData[args.Key][index+1]
			}
		}
		ss.ListDataMutex.Unlock()
		if found {
			ss.LeasesMutex.Lock()
			_, exists := ss.Leases[args.Key]
			ss.LeasesMutex.Unlock()
			if exists {
				callback := make(chan bool, 1)
				go handleRevoke(ss, args.Key, &callback)
				<-callback
			}
			ss.ListDataMutex.Lock()
			length := len(ss.ListData[args.Key])
			ss.ListData[args.Key] = ss.ListData[args.Key][:length-1]
			ss.ListDataMutex.Unlock()
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
	}
	//fmt.Println("leaving RemoveFromList")
	return nil
}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~helpers~~~~~~~~~~~~~~~~~~~~~

func getMinHash(ss *storageServer) uint32 {
	minHash := ss.NodeID
	for i := 0; i < len(ss.Nodes); i++ {
		currHash := ss.Nodes[i].NodeID
		//hash is smaller than us
		if currHash < ss.NodeID {
			if minHash >= ss.NodeID {
				minHash = currHash
			} else if currHash > minHash {
				minHash = currHash
			}
		} else {
			if minHash >= ss.NodeID {
				if currHash > minHash {
					minHash = currHash
				}
			}
		}
	}
	//fmt.Println("my hash: " + strconv.Itoa(int(ss.NodeID)) + " my minimum: " + strconv.Itoa(int(minHash)))
	return minHash
}

func withinBounds(key string, ss *storageServer) bool {
	//fmt.Println("min: " + strconv.Itoa(int(ss.minHash)) + " max: " + strconv.Itoa(int(ss.NodeID)))
	if len(ss.Nodes) == 1 {
		return true
	}
	hash := libstore.StoreHash(key)
	if ss.minHash == ss.NodeID {
		return true
	}
	if ss.minHash < ss.NodeID {
		return ss.minHash < hash && hash <= ss.NodeID
	}
	return hash <= ss.NodeID || hash >= ss.minHash
}

func handleRevoke(ss *storageServer, key string, callback *(chan bool)) {
	//fmt.Println("revoking key: " + key)
	ss.LeasesMutex.Lock()
	leases := ss.Leases[key]
	ss.LeasesMutex.Unlock()
	leases.revokeInProgress = true
	for leases.serverList.Len() != 0 {
		//fmt.Println("entering lease loop")
		for e := leases.serverList.Front(); e != nil; e = e.Next() {
			subLease := e.Value.(*subLeaseStruct)
			if time.Since(subLease.GrantTime) > time.Duration(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second {
				leases.serverList.Remove(e)
				fmt.Println()
				//fmt.Println("It has expired!")
				continue
			}
			revokeConn, err := rpc.DialHTTP("tcp", subLease.HostPort)
			args := &storagerpc.RevokeLeaseArgs{
				Key: key,
			}
			var reply storagerpc.RevokeLeaseReply
			if err != nil {
				leases.serverList.Remove(e)
				continue
			}
			//fmt.Println("about to call!")
			rpcFinish := revokeConn.Go("LeaseCallbacks.RevokeLease", args, &reply, nil)
			timeout := time.NewTimer(time.Duration(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second - time.Since(subLease.GrantTime))
			select {
			case <- rpcFinish.Done:
				if rpcFinish.Error != nil {
					fmt.Println(err)
					leases.serverList.Remove(e)
				} else {
					leases.serverList.Remove(e)
				}
				timeout.Stop()
			case <- timeout.C:
				leases.serverList.Remove(e)
			}
		}
	}
	ss.LeasesMutex.Lock()
	delete(ss.Leases, key)
	ss.LeasesMutex.Unlock()
	*callback <- true
}
