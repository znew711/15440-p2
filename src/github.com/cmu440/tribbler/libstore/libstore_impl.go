package libstore

import (
	"errors"
	"fmt"
	//"net"
	//"net/http"
	"net/rpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/rpc/librpc"
)

type libstore struct {
	// TODO: implement this!
	storageCli *rpc.Client
	mode LeaseMode
	masterServerHostPort string
	myHostPort string
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
	libstore.masterServerHostPort = masterServerHostPort

    err := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
    fmt.Println("registered")
    if err != nil {
        return nil, err
    }

	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	fmt.Println("dialed")
    if err != nil {
		return nil, err
	}
	libstore.storageCli = cli

	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	var reply storagerpc.GetReply
	if err := ls.storageCli.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		return "", errors.New("Key not found.")
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	if err := ls.storageCli.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		return errors.New("Wrong key range (shouldn't happen for checkpoint).")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	if err := ls.storageCli.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		return errors.New("Key not found.")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myHostPort}
	var reply storagerpc.GetListReply
	if err := ls.storageCli.Call("StorageServer.GetList", args, &reply); err != nil {
		return []string{}, err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		return []string{}, errors.New("Key not found.")
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	if err := ls.storageCli.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		// TODO: storageserver should handle "wrong key range"
		// for now, just return a new error
		return errors.New("Key not found.")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	if err := ls.storageCli.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}

	if reply.Status == storagerpc.WrongServer || reply.Status == storagerpc.ItemExists {
		// TODO: storageserver should handle "wrong key range" separately
		// for now, just return a new error
		return errors.New("Wrong key range (shouldn't happen for checkpoint).")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
