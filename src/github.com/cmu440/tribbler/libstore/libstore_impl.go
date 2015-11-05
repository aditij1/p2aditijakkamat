package libstore

import (
	"errors"
	"time"
	"fmt"
	"net/rpc"
	//"strconv"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	masterServ *rpc.Client
	allServers [] storagerpc.Node
	mode LeaseMode
	hostPort string

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


/**
	Libstore deals with rerouting


	-Ask for servers until responds without error
	-Is responsible for asking thr correct server

	-StorageServer trick:
		each user is mapped to a key, and the key maps to a list of tribbles
*/
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {

	masterServ, err := rpc.DialHTTP("tcp", masterServerHostPort)

	if err != nil {
		return nil, err
	}

	getServArgs := storagerpc.GetServersArgs{}
	//declare empty struct to store return
	var getServReply storagerpc.GetServersReply

	for {
		err = masterServ.Call("StorageServer.GetServers", getServArgs, &getServReply)

		if(err == nil) {
			break
		}

		if(err != nil) {
			return nil,err

		} else if(getServReply.Status != storagerpc.NotReady) {

			return nil, errors.New("Error!")
		}

		//retry every second
		timer := time.NewTimer(time.Second * 1)
		<-timer.C
	}


	var newLs libstore = libstore {
		masterServ: masterServ,
		allServers: getServReply.Servers,
		mode: mode,
		hostPort: myHostPort,
	}

	return &newLs, nil
}


func (ls *libstore) Get(key string) (string, error) {

	var wantLease bool = false
	if(ls.mode == Never) {
		wantLease = false
	}
	//TODO else clause after checkpoint

	getArgs := storagerpc.GetArgs{
		Key: key,
		WantLease: wantLease,
		HostPort: ls.hostPort,
	}

	var reply storagerpc.GetReply

	err := ls.masterServ.Call("StorageServer.Get", getArgs, &reply)

	if(err != nil) {
		fmt.Println("LibStore Get: Error")
		return "",err

	} else if(reply.Status != storagerpc.OK) {
		fmt.Println("LibStore Get: Error")
		return "", errors.New("Reply status not Ok")
	}

	return reply.Value,nil
}


func (ls *libstore) Put(key, value string) error {
	putArgs := storagerpc.PutArgs{
		Key: key,
		Value: value,
	}

	var reply storagerpc.PutReply

	err := ls.masterServ.Call("StorageServer.Put", putArgs, &reply)

	if(err != nil) {
		return err

	} else if(reply.Status != storagerpc.OK) {

		return errors.New("Reply status not Ok")
	}

	return nil
}


func (ls *libstore) Delete(key string) error {

	delArgs := storagerpc.DeleteArgs{
		Key: key,
	}

	var reply storagerpc.DeleteReply

	err := ls.masterServ.Call("StorageServer.Delete", delArgs, &reply)

	if(err != nil) {
		fmt.Println("LibStore Delete: ")
		return err

	} else if(reply.Status != storagerpc.OK) {

		return errors.New("Reply status not Ok")
	}

	return nil
}


func (ls *libstore) GetList(key string) ([]string, error) {

	var wantLease bool = false

	if(ls.mode == Never) {
		wantLease = false
	}
	//TODO else clause after checkpoint

	getArgs := storagerpc.GetArgs{
		Key: key,
		WantLease: wantLease,
		HostPort: ls.hostPort,
	}

	var reply storagerpc.GetListReply

	err := ls.masterServ.Call("StorageServer.GetList", getArgs, &reply)

	if(err != nil) {
		return make([]string,0),err

	} else if(reply.Status != storagerpc.OK) {

		return make([]string,0), errors.New("Reply status not Ok")
	}

	return reply.Value,nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	putArgs := storagerpc.PutArgs{
		Key: key,
		Value: removeItem,
	}

	var reply storagerpc.PutReply

	err := ls.masterServ.Call("StorageServer.RemoveFromList", putArgs, &reply)

	if(err != nil) {
		return err

	} else if(reply.Status != storagerpc.OK) {

		return errors.New("Reply status not Ok")
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	putArgs := storagerpc.PutArgs{
		Key: key,
		Value: newItem,
	}

	var reply storagerpc.PutReply

	err := ls.masterServ.Call("StorageServer.AppendToList", putArgs, &reply)

	if(err != nil) {
		fmt.Println("Libstore AppendToList: Error")
		return nil

	} else if(reply.Status != storagerpc.OK) {

		return errors.New("Reply status not Ok")
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
