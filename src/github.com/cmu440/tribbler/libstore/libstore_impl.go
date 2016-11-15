package libstore

import (
	"errors"
	"fmt"
	//"net"
	//"net/http"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
	"strings"
	"sync"
	"log"
	"os"
)

const UINT32_MAX uint32 = 4294967295

// TODO: store the connection to the storage server in each elem in the server list?
//    make a struct of storagerpc.Node, conn
type ssInfo struct {
	hostPort string
	nodeID   uint32
	cli      *rpc.Client
}

type cacheStringData struct {
	value        string
	leaseSeconds int
	timer        *time.Timer
}

type cacheListData struct {
	value        []string
	leaseSeconds int
	timer        *time.Timer
}

type accessInfo struct {
	key         string
	lastAccess  time.Time
	accessCount int
}

type libstore struct {
	storageCli           *rpc.Client
	mode                 LeaseMode
	masterServerHostPort string
	myHostPort           string
	servers              []*ssInfo
	stringCache          map[string]*cacheStringData
	listCache            map[string]*cacheListData
	keyAccesses          []*accessInfo
	stringMutex          *sync.Mutex
	listMutex            *sync.Mutex
}

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
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {

	l := log.New(os.Stderr, "", 0)
	l.Println("newlibstore called")

	libstore := new(libstore)
	libstore.mode = mode
	libstore.myHostPort = myHostPort
	libstore.masterServerHostPort = masterServerHostPort

	err := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	if err != nil && !strings.Contains(err.Error(), "service already defined") {
		l.Println(err)
		return nil, err
	}

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		l.Println(err)
		l.Println("could not reach master server")
		return nil, err
	}
	libstore.storageCli = cli

	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	connected := false
	for numTries := 0; numTries < 5; numTries++ {
		if err := libstore.storageCli.Call("StorageServer.GetServers", args, &reply); err != nil {
			l.Println("get servers fails")
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			fmt.Println(reply.Status)
			connected = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !connected {
		fmt.Println("libstore:123 could not connect to storage server after 5 tries")
		cli.Close()
		return nil, errors.New("Could not connect to storage server.")
	}
	//time.Sleep(1*time.Second)

	//libstore.servers = reply.Servers
	fmt.Printf("length of server list: %d\n", len(reply.Servers))
	ssList := []*ssInfo{}
	for _, node := range reply.Servers {
		cli, err := rpc.DialHTTP("tcp", node.HostPort)
		if err != nil {
			return nil, err // improper server in list
		}
		ss := &ssInfo{
			hostPort: node.HostPort,
			nodeID:   node.NodeID,
			cli:      cli}
		ssList = append(ssList, ss)
	}
	libstore.servers = ssList

	libstore.stringCache = make(map[string]*cacheStringData)
	libstore.listCache = make(map[string]*cacheListData)
	libstore.keyAccesses = []*accessInfo{}
	libstore.stringMutex = &sync.Mutex{}
	libstore.listMutex = &sync.Mutex{}

	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	for k, v := range ls.stringCache {
		if k == key {
			return v.value, nil
		}
	}

	// look up key in list of keys, see how many times it's been accessed
	keyFound := false
	requestLease := false
	if ls.mode == Always {
		requestLease = true
	}
	for _, keyAccess := range ls.keyAccesses {
		if keyAccess.key == key {
			keyFound = true
			currentTime := time.Now()
			if currentTime.Sub(keyAccess.lastAccess) < (storagerpc.QueryCacheSeconds * time.Second) {
				keyAccess.accessCount++
				if keyAccess.accessCount >= storagerpc.QueryCacheThresh && ls.myHostPort != "" {
					requestLease = true
					keyAccess.accessCount = 0 // reset???
				}
			} else {
				// has been too much time, don't request a lease
				keyAccess.accessCount = 0
			}
			keyAccess.lastAccess = currentTime
			break
		}
	}

	if !keyFound {
		newKey := &accessInfo{
			key:         key,
			lastAccess:  time.Now(),
			accessCount: 1}
		ls.keyAccesses = append(ls.keyAccesses, newKey)
	}

	args := &storagerpc.GetArgs{Key: key, WantLease: requestLease, HostPort: ls.myHostPort}

	cli, err := findServer(ls, key)
	if err != nil {
		return "", err
	}
	var reply storagerpc.GetReply
	if err := cli.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		fmt.Printf("error: %d\n", reply.Status)
		return "", errors.New("Key not found.")
	}
	if ls.mode != Never {
		lease := reply.Lease
		if lease.Granted {
			newData := &cacheStringData{
				value:        reply.Value,
				leaseSeconds: lease.ValidSeconds,
				timer: time.AfterFunc(time.Duration(lease.ValidSeconds)*time.Second, func() {
					clearStringCache(ls, key)
				})}
			ls.stringMutex.Lock()
			ls.stringCache[key] = newData
			ls.stringMutex.Unlock()
		}
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key: key, Value: value}
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}
	var reply storagerpc.PutReply

	if err := cli.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		fmt.Printf("error: %d\n", reply.Status)
		return fmt.Errorf("Wrong key range (shouldn't happen for checkpoint).")
	}

	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}

	var reply storagerpc.DeleteReply
	if err := cli.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		fmt.Printf("error: %d\n", reply.Status)
		return errors.New("Key not found.")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	for k, v := range ls.listCache {
		if k == key {
			return v.value, nil
		}
	}

	// look up key in list of keys, see how many times it's been accessed
	keyFound := false
	requestLease := false
	if ls.mode == Always {
		requestLease = true
	}
	for _, keyAccess := range ls.keyAccesses {
		if keyAccess.key == key {
			keyFound = true
			currentTime := time.Now()
			if currentTime.Sub(keyAccess.lastAccess) < (storagerpc.QueryCacheSeconds * time.Second) {
				keyAccess.accessCount++
				if keyAccess.accessCount >= storagerpc.QueryCacheThresh && ls.myHostPort != "" {
					requestLease = true
					keyAccess.accessCount = 0 // reset???
				}
			} else {
				// has been too much time, don't request a lease
				keyAccess.accessCount = 0
			}
			keyAccess.lastAccess = currentTime
			break
		}
	}

	if !keyFound {
		newKey := &accessInfo{
			key:         key,
			lastAccess:  time.Now(),
			accessCount: 1}
		ls.keyAccesses = append(ls.keyAccesses, newKey)
	}

	args := &storagerpc.GetArgs{Key: key, WantLease: requestLease, HostPort: ls.myHostPort}
	cli, err := findServer(ls, key)
	if err != nil {
		return []string{}, err
	}

	var reply storagerpc.GetListReply
	if err := cli.Call("StorageServer.GetList", args, &reply); err != nil {
		return []string{}, err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		fmt.Printf("error: %d\n", reply.Status)
		return []string{}, errors.New("Key not found.")
	}

	if ls.mode != Never {
		lease := reply.Lease
		if lease.Granted {
			newData := &cacheListData{
				value:        reply.Value,
				leaseSeconds: lease.ValidSeconds,
				timer: time.AfterFunc(time.Duration(lease.ValidSeconds)*time.Second, func() {
					clearListCache(ls, key)
				})}
			ls.listMutex.Lock()
			ls.listCache[key] = newData
			ls.listMutex.Unlock()
		}
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}
	var reply storagerpc.PutReply
	if err := cli.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		fmt.Printf("error: %d\n", reply.Status)
		return errors.New("Item not found.")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}
	var reply storagerpc.PutReply
	if err := cli.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}

	if reply.Status == storagerpc.WrongServer {
		// TODO: storageserver should handle "wrong key range" separately
		// for now, just return a new error
		return errors.New("Wrong server.")
	} else if reply.Status == storagerpc.ItemExists {
		return errors.New("Item exists.")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	for k, _ := range ls.stringCache {
		if k == args.Key {
			if err := clearStringCache(ls, args.Key); err != nil {
				return err
			}
			reply.Status = storagerpc.OK
			return nil
		}
	}
	for k, _ := range ls.listCache {
		if k == args.Key {
			if err := clearListCache(ls, args.Key); err != nil {
				return err
			}
			reply.Status = storagerpc.OK
			return nil
		}
	}
	reply.Status = storagerpc.KeyNotFound
	return nil
}

func clearStringCache(ls *libstore, key string) error {
	for k, _ := range ls.stringCache {
		if key == k {
			ls.stringMutex.Lock()
			delete(ls.stringCache, key)
			ls.stringMutex.Unlock()
			return nil
		}

	}
	return errors.New("key not found in cache")
}

func clearListCache(ls *libstore, key string) error {
	for k, _ := range ls.listCache {
		if key == k {
			ls.listMutex.Lock()
			delete(ls.listCache, key)
			ls.listMutex.Unlock()
			return nil
		}
	}
	return errors.New("key not found in cache")
}

func findServer(ls *libstore, key string) (*rpc.Client, error) {
	// hash upper is going to be the server we want, hash lower is just to make sure
	//    we know the lower bound (do we even need the lower bound??)
	hash := StoreHash(key)
	hashUpper := UINT32_MAX
	//hashLower := 0
	var correctServer *rpc.Client
	for _, server := range ls.servers {
		serverHash := server.nodeID
		// TODO: check for wraparound!!
		if hash <= serverHash && serverHash < hashUpper {
			hashUpper = serverHash
			correctServer = server.cli
		}
	}

	//fmt.Printf("%s\n", correctServer.HostPort)
	/*cli, err := rpc.DialHTTP("tcp", correctServer.HostPort)
	if err != nil {
		return nil, err
	}*/
	if correctServer == nil {
		// for now, try the master server?
		//fmt.Println("here")
		correctServer = ls.storageCli
	}
	return correctServer, nil
}
