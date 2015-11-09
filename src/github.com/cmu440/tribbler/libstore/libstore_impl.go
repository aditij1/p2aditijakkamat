package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
	//"strconv"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	masterServ *rpc.Client
	allServerNodes []storagerpc.Node
	mode       LeaseMode
	hostPort   string
}

const TIMEOUT_GETTING_SERVERS = "GET_SERVER_TIMEOUT"
const WRONG_SERVER = "WRONG_SERVER"
const KEY_NOT_FOUND = "KEY_NOT_FOUND"
const ITEM_EXISTS = "ITEM_EXISTS"
const ITEM_NOT_FOUND = "ITEM_NOT_FOUND"
const UNEXPECTED_ERROR = "UNEXPECTED_ERROR"

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
	//retry every second, if not yet ready
	timer := time.NewTimer(time.Second * 1)
	var tryCount int = 0

	for {
		err = masterServ.Call("StorageServer.GetServers", getServArgs, &getServReply)

		if err != nil {
			return nil, err

		} else {
			//err is nil
			if getServReply.Status == storagerpc.OK {
				break
			}
		}

		if(tryCount == 5) {
			return nil,errors.New(TIMEOUT_GETTING_SERVERS)
		}
		//status is not OK; wait for timer
		<-timer.C
		tryCount++
	}

	var newLs libstore = libstore{
		masterServ: masterServ,
		allServerNodes: getServReply.Servers,
		mode:       mode,
		hostPort:   myHostPort,
	}

	return &newLs, nil
}

func (ls *libstore) Get(key string) (string, error) {

	var wantLease bool = false

	if ls.mode == Never {
		wantLease = false
	}
	//TODO else clause 

	getArgs := storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort,
	}

	var reply storagerpc.GetReply

	err := ls.masterServ.Call("StorageServer.Get", getArgs, &reply)

	if err != nil {
		fmt.Println("LibStore Get: Error")
		return "", err

	} 
		
	switch reply.Status {

		case storagerpc.OK:
			return reply.Value, nil
			break
		
		case storagerpc.WrongServer:
			return "", errors.New(WRONG_SERVER)
			break
		
		default:
			fmt.Println("LibStore received unexpected error")
			return "", errors.New(UNEXPECTED_ERROR)
			break
	}
	
	//fmt.Println("LibStore Get: Error")
	//return "", errors.New("Reply status not Ok")
	

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	putArgs := storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}

	var reply storagerpc.PutReply

	err := ls.masterServ.Call("StorageServer.Put", putArgs, &reply)

	if err != nil {
		return err

	} 

	switch reply.Status {
		case storagerpc.OK:
			return nil
			break
		
		case storagerpc.WrongServer:
			return errors.New(WRONG_SERVER)
			break;
		
		default:
			fmt.Println("LibStore received unexpected error")
			return errors.New(UNEXPECTED_ERROR)
	}

	return nil
}

func (ls *libstore) Delete(key string) error {

	delArgs := storagerpc.DeleteArgs{
		Key: key,
	}

	var reply storagerpc.DeleteReply

	err := ls.masterServ.Call("StorageServer.Delete", delArgs, &reply)

	if err != nil {
		fmt.Println("LibStore Delete: error")
		return err

	} 

	switch reply.Status {
		case storagerpc.OK:
			return nil
			break

		case storagerpc.KeyNotFound:
			return errors.New(KEY_NOT_FOUND)
			break
		
		case storagerpc.WrongServer:
			return errors.New(WRONG_SERVER)
			break
		
		default:
			fmt.Println("LibStore received unexpected error")
			return errors.New(UNEXPECTED_ERROR)
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {

	var wantLease bool = false

	if ls.mode == Never {
		wantLease = false
	}
	//TODO else clause after checkpoint

	getArgs := storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort,
	}

	var reply storagerpc.GetListReply

	err := ls.masterServ.Call("StorageServer.GetList", getArgs, &reply)

	if err != nil {
		return make([]string, 0), err

	} 

	switch reply.Status {
		case storagerpc.OK:
			return reply.Value,nil
			break

		case storagerpc.KeyNotFound:
			return make([]string, 0), errors.New(KEY_NOT_FOUND)
			break
		
		case storagerpc.WrongServer:
			return make([]string, 0), errors.New(WRONG_SERVER)
			break
		
		default:
			fmt.Println("LibStore received unexpected error")
			return make([]string, 0), errors.New(UNEXPECTED_ERROR)
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {

	putArgs := storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}

	var reply storagerpc.PutReply

	err := ls.masterServ.Call("StorageServer.RemoveFromList", putArgs, &reply)

	if err != nil {
		return err

	} 

	switch reply.Status {
		case storagerpc.OK:
			return nil
			break

		case storagerpc.ItemNotFound:
			return errors.New(ITEM_NOT_FOUND)
			break
		
		case storagerpc.WrongServer:
			return errors.New(WRONG_SERVER)
			break
		
		default:
			fmt.Println("LibStore received unexpected error")
			return errors.New(UNEXPECTED_ERROR)
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	putArgs := storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}

	var reply storagerpc.PutReply

	err := ls.masterServ.Call("StorageServer.AppendToList", putArgs, &reply)

	if err != nil {
		fmt.Println("Libstore AppendToList: Error")
		return err

	} 

	switch reply.Status {
		case storagerpc.OK:
			return nil
			break

		case storagerpc.ItemExists:
			return errors.New(ITEM_EXISTS)
			break
		
		case storagerpc.WrongServer:
			return errors.New(WRONG_SERVER)
			break
		
		default:
			fmt.Println("LibStore received unexpected error")
			return errors.New(UNEXPECTED_ERROR)
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
