package storageserver

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"hash/fnv"
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

type revokeStruct struct {
	Key      string
	Callback *chan bool
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
	RevokeQueue      chan revokeStruct
	WriteLocks       []sync.Mutex
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
		RevokeQueue:      make(chan revokeStruct),
		WriteLocks:       make([]sync.Mutex, 10),
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
		needMoreSlaves := numNodes > 1
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
	go handleRevoke(&ss)
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
	//fmt.Println("entering Get")
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
	//fmt.Println("leaving get")
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	//fmt.Println("entering delete")
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Lock()
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
		if exists {
			callback := make(chan bool, 1)
			ss.RevokeQueue <- revokeStruct{args.Key, &callback}
			<-callback
		}
		ss.DataMutex.Lock()
		delete(ss.Data, args.Key)
		ss.DataMutex.Unlock()
		reply.Status = storagerpc.OK
	}
	//fmt.Println("leaving delete")
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	//fmt.Println("entering get list")
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.ListDataMutex.Lock()
	_, ok := ss.ListData[args.Key]
	ss.ListDataMutex.Unlock()
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		ss.ListDataMutex.Lock()
		newArray := make([]string, len(ss.ListData[args.Key]))
		copy(newArray, ss.ListData[args.Key])
		reply.Value = newArray
		ss.ListDataMutex.Unlock()
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
	ss.ListDataMutex.Lock()
	check := ss.ListData[args.Key]
	set := make(map[string]bool, len(check))
	fail := false
	for _, data := range check {
		if set[data] == true {
			fmt.Println("POST CONDITION FAIL")
			if args.WantLease {
				fmt.Println("And we wanted a lease!")
			}
			fail = true
		}
		set[data] = true
	}
	if fail {
		fmt.Print("Get: ")
		fmt.Print(ss.ListData[args.Key])
		fmt.Print("\n")
	}
	ss.ListDataMutex.Unlock()
	
	//fmt.println("leaving get list")
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Lock()
	//fmt.Println("entering Put "  + args.Key + " " + args.Value)
	if !withinBounds(args.Key, ss) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.LeasesMutex.Lock()
	_, exists := ss.Leases[args.Key]
	ss.LeasesMutex.Unlock()
	if exists {
		callback := make(chan bool, 1)
		ss.RevokeQueue <- revokeStruct{args.Key, &callback}
		<-callback
	}
	ss.DataMutex.Lock()
	ss.Data[args.Key] = args.Value
	ss.DataMutex.Unlock()
	reply.Status = storagerpc.OK
	//fmt.Println("Leaving put "  + args.Key + " " + args.Value)
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Unlock()
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Lock()
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
			ss.LeasesMutex.Lock()
			_, exists := ss.Leases[args.Key]
			ss.LeasesMutex.Unlock()
			if exists {
				callback := make(chan bool, 1)
				ss.RevokeQueue <- revokeStruct{args.Key, &callback}
				<-callback
			}
			ss.ListDataMutex.Lock()
			ss.ListData[args.Key] = append(ss.ListData[args.Key], args.Value)
			ss.ListDataMutex.Unlock()
			reply.Status = storagerpc.OK
		}
	}
	//fmt.Println("return from AppendToList")
	ss.ListDataMutex.Lock()
	check := ss.ListData[args.Key]
	set := make(map[string]bool, len(check))
	fail := false
	for _, data := range check {
		if set[data] == true {
			fmt.Println("POST CONDITION FAIL")
			fail = true
		}
		set[data] = true
	}
	if fail {
		fmt.Println("key: " + args.Key)
		fmt.Print("AppendToList: ")
		fmt.Print(ss.ListData[args.Key])
		fmt.Print("\n")
	}
	ss.ListDataMutex.Unlock()
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Lock()
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
		if found {
			ss.LeasesMutex.Lock()
			_, exists := ss.Leases[args.Key]
			ss.LeasesMutex.Unlock()
			if exists {
				callback := make(chan bool, 1)
				ss.RevokeQueue <- revokeStruct{args.Key, &callback}
				<-callback
			}
			length := len(ss.ListData[args.Key])
			ss.ListData[args.Key] = ss.ListData[args.Key][:length-1]
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		ss.ListDataMutex.Unlock()
	}
	// /fmt.Println("leaving RemoveFromList")
	ss.ListDataMutex.Lock()
	check := ss.ListData[args.Key]
	set := make(map[string]bool, len(check))
	fail := false
	for _, data := range check {
		if set[data] == true {
			fmt.Println("POST CONDITION FAIL")
			fail = true
		}
		set[data] = true
	}
	if fail {
		fmt.Println("key: " + args.Key)
		fmt.Print("RemoveToList: ")
		fmt.Print(ss.ListData[args.Key])
		fmt.Print("\n")
	}
	ss.ListDataMutex.Unlock()
	ss.WriteLocks[getHash(args.Key) % len(ss.WriteLocks)].Unlock()
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

func getHash(s string) int {
	hasher := fnv.New32()
	hasher.Write([]byte(s))
	return int(hasher.Sum32())
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

func handleRevoke(ss *storageServer) {
	//fmt.Println("revoking key: " + key)
	for {
		select {
		case newrevoke := <- ss.RevokeQueue:
			ss.LeasesMutex.Lock()
			leases, ok := ss.Leases[newrevoke.Key]
			ss.LeasesMutex.Unlock()
			if !ok {
				*newrevoke.Callback <- true
				continue;
			}
			leases.revokeInProgress = true
			for leases.serverList.Len() != 0 {
				ss.LeasesMutex.Lock()
				for e := leases.serverList.Front(); e != nil; e = e.Next() {
					subLease := e.Value.(*subLeaseStruct)
					if time.Since(subLease.GrantTime) > time.Duration(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)*time.Second {
						leases.serverList.Remove(e)
						fmt.Println()
						//fmt.Println("It has expired!")
						continue
					}
					revokeConn, err := rpc.DialHTTP("tcp", subLease.HostPort)
					args := &storagerpc.RevokeLeaseArgs{
						Key: newrevoke.Key,
					}
					var reply storagerpc.RevokeLeaseReply
					if err != nil {
						leases.serverList.Remove(e)
						continue
					}
					//fmt.Println("about to call!")
					rpcFinish := revokeConn.Go("LeaseCallbacks.RevokeLease", args, &reply, nil)
					timeout := time.NewTimer(time.Duration(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)*time.Second - time.Since(subLease.GrantTime))
					ss.LeasesMutex.Unlock()
					select {
					case <-rpcFinish.Done:
						ss.LeasesMutex.Lock()
						if rpcFinish.Error != nil {
							fmt.Println(err)
							leases.serverList.Remove(e)
						} else {
							leases.serverList.Remove(e)
						}
						ss.LeasesMutex.Unlock()
						timeout.Stop()
					case <-timeout.C:
						ss.LeasesMutex.Lock()
						leases.serverList.Remove(e)
						ss.LeasesMutex.Unlock()

					}
					ss.LeasesMutex.Lock()
				}
				ss.LeasesMutex.Unlock()
			}
			ss.LeasesMutex.Lock()
			delete(ss.Leases, newrevoke.Key)
			ss.LeasesMutex.Unlock()
			*newrevoke.Callback <- true
		}
	}
}
