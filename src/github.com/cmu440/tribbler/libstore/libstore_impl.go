package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

const UINT32_MAX uint32 = 4294967295

type ssInfo struct {
	hostPort string      // storage server's hostport
	nodeID   uint32      // storage server's hash
	cli      *rpc.Client // rpc connection to storage server
}

type cacheStringData struct {
	value        string      // string value corresponding to key
	leaseSeconds int         // seconds this lease is valid
	timer        *time.Timer // timer that counts down for leaseseconds
}

type cacheListData struct {
	value        []string    // list value corresponding to key
	leaseSeconds int         // seconds this lease is valid
	timer        *time.Timer // timer that counts down for leaseseconds
}

type accessInfo struct {
	key         string    
	lastAccess  time.Time // last time of access to this key
	accessCount int       // how many times this key has been accessed in querycacheseconds
}

type libstore struct {
	storageCli           *rpc.Client 				 // connection to master server
	mode                 LeaseMode					 // mode of operation
	myHostPort           string 					 
	servers              []*ssInfo 					 // list of storage servers
	stringCache          map[string]*cacheStringData // cache of string data
	listCache            map[string]*cacheListData   // cache of list data
	keyAccesses          []*accessInfo               // all seen keys and access pattern data
	stringMutex          *sync.Mutex                 // mutex for string cache
	listMutex            *sync.Mutex                 // mutex for list cache
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
	libstore := new(libstore)
	libstore.mode = mode
	libstore.myHostPort = myHostPort

	// register rpc call; if already registered, ignore (necessary because of tribserver retries)
	err := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	if err != nil && !strings.Contains(err.Error(), "service already defined") {
		return nil, err
	}

	// find master storage server
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		// could not reach master server
		return nil, err
	}
	libstore.storageCli = cli

	// get list of all storage servers; retry 5 times if master replies with "not ready"
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	connected := false
	for numTries := 0; numTries < 5; numTries++ {
		if err := libstore.storageCli.Call("StorageServer.GetServers", args, &reply); err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			connected = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !connected {
		cli.Close()
		return nil, errors.New("Could not connect to storage server.")
	}

	// dial all storage servers and store their connections
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

	// instantiate caches and mutexes
	libstore.stringCache = make(map[string]*cacheStringData)
	libstore.listCache = make(map[string]*cacheListData)
	libstore.keyAccesses = []*accessInfo{}
	libstore.stringMutex = &sync.Mutex{}
	libstore.listMutex = &sync.Mutex{}

	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// check cache for key; return cached version if found
	ls.stringMutex.Lock()
	for k, v := range ls.stringCache {
		if k == key {
			val := v.value
			ls.stringMutex.Unlock()
			return val, nil
		}
	}
	ls.stringMutex.Unlock()

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
			// compare this time with last access time; increment counter if within querycacheseconds
			if currentTime.Sub(keyAccess.lastAccess) < (storagerpc.QueryCacheSeconds * time.Second) {
				keyAccess.accessCount++
				if keyAccess.accessCount >= storagerpc.QueryCacheThresh && ls.myHostPort != "" {
					requestLease = true
					keyAccess.accessCount = 0 // reset count for when lease expires
				}
			} else {
				// has been too much time, don't request a lease
				keyAccess.accessCount = 0
			}
			keyAccess.lastAccess = currentTime
			break
		}
	}
	// add key to list of access info if it wasn't there already
	if !keyFound {
		newKey := &accessInfo{
			key:         key,
			lastAccess:  time.Now(),
			accessCount: 1}
		ls.keyAccesses = append(ls.keyAccesses, newKey)
	}

	args := &storagerpc.GetArgs{Key: key, WantLease: requestLease, HostPort: ls.myHostPort}
	// route request to proper server
	cli, err := findServer(ls, key)
	if err != nil {
		return "", err
	}
	var reply storagerpc.GetReply
	if err := cli.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}
	if reply.Status != storagerpc.OK {
		return "", errors.New("Key not found.")
	}

	// check if lease was granted, and if so, add data to cache
	if ls.mode != Never {
		lease := reply.Lease
		if lease.Granted {
			newData := &cacheStringData{
				value:        reply.Value,
				leaseSeconds: lease.ValidSeconds,
				timer: time.AfterFunc(time.Duration(lease.ValidSeconds)*time.Second, func() {
					clearStringCache(ls, key) // when time is up, run function that invalidates entry
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
	// route request to proper server
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}

	var reply storagerpc.PutReply
	if err := cli.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return fmt.Errorf("Put failed for key %s\n", key)
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	// route request to proper server
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}

	var reply storagerpc.DeleteReply
	if err := cli.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Key not found.")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// check cache for key; return cached version if found
	ls.listMutex.Lock()
	for k, v := range ls.listCache {
		if k == key {
			val := v.value
			ls.listMutex.Unlock()
			return val, nil
		}
	}
	ls.listMutex.Unlock()

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
			// compare this time with last access time; increment counter if within querycacheseconds
			if currentTime.Sub(keyAccess.lastAccess) < (storagerpc.QueryCacheSeconds * time.Second) {
				keyAccess.accessCount++
				if keyAccess.accessCount >= storagerpc.QueryCacheThresh && ls.myHostPort != "" {
					requestLease = true
					keyAccess.accessCount = 0 // reset count for when lease expires
				}
			} else {
				// has been too much time, don't request a lease
				keyAccess.accessCount = 0
			}
			keyAccess.lastAccess = currentTime
			break
		}
	}
	// add key to list of access info if it wasn't there already
	if !keyFound {
		newKey := &accessInfo{
			key:         key,
			lastAccess:  time.Now(),
			accessCount: 1}
		ls.keyAccesses = append(ls.keyAccesses, newKey)
	}

	args := &storagerpc.GetArgs{Key: key, WantLease: requestLease, HostPort: ls.myHostPort}
	// route request to proper server
	cli, err := findServer(ls, key)
	if err != nil {
		return []string{}, err
	}

	var reply storagerpc.GetListReply
	if err := cli.Call("StorageServer.GetList", args, &reply); err != nil {
		return []string{}, err
	}
	if reply.Status != storagerpc.OK {
		return []string{}, errors.New("Key not found.")
	}

	// check if lease was granted, and if so, add data to cache
	if ls.mode != Never {
		lease := reply.Lease
		if lease.Granted {
			newData := &cacheListData{
				value:        reply.Value,
				leaseSeconds: lease.ValidSeconds,
				timer: time.AfterFunc(time.Duration(lease.ValidSeconds)*time.Second, func() {
					clearListCache(ls, key) // when time is up, run function that invalidates entry
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
	// route request to proper server
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}
	var reply storagerpc.PutReply
	if err := cli.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Item not found.")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	// route request to proper server
	cli, err := findServer(ls, key)
	if err != nil {
		return err
	}
	var reply storagerpc.PutReply
	if err := cli.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}

	if reply.Status == storagerpc.ItemExists {
		return errors.New("Item exists.")
	} else if reply.Status != storagerpc.OK {
		return fmt.Errorf("Could not append to list %s\n", key)
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	// look through cache of string data, call invalidation function if found
	for k, _ := range ls.stringCache {
		if k == args.Key {
			if err := clearStringCache(ls, args.Key); err != nil {
				return err
			}
			reply.Status = storagerpc.OK
			return nil
		}
	}

	// look through cache of list data, call invalidation function if found
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


/* HELPER FUNCTIONS */

/* clearStringCache: deletes entry with specified key from string cache map */
func clearStringCache(ls *libstore, key string) error {
	for k, _ := range ls.stringCache {
		if key == k {
			ls.stringMutex.Lock()
			delete(ls.stringCache, key)
			ls.stringMutex.Unlock()
			return nil
		}
	}
	return errors.New("key not found in string cache")
}

/* clearListCache: deletes entry with specified key from list cache map */
func clearListCache(ls *libstore, key string) error {
	for k, _ := range ls.listCache {
		if key == k {
			ls.listMutex.Lock()
			delete(ls.listCache, key)
			ls.listMutex.Unlock()
			return nil
		}
	}
	return errors.New("key not found in list cache")
}

/* findServer: find the server that is in control of this key */
func findServer(ls *libstore, key string) (*rpc.Client, error) {
	// hash upper is going to be the server we want
	hash := StoreHash(key)
	hashUpper := UINT32_MAX

	var correctServer *rpc.Client
	for _, server := range ls.servers {
		serverHash := server.nodeID
		if hash <= serverHash && serverHash < hashUpper {
			hashUpper = serverHash
			correctServer = server.cli
		}
	}

	if correctServer == nil {
		// dial the master server if proper storage server wasn't found
		correctServer = ls.storageCli
	}
	return correctServer, nil
}
