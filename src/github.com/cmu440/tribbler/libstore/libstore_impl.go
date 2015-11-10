package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"
	"sort"
	//"strconv"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

const TIMEOUT_GETTING_SERVERS = "GET_SERVER_TIMEOUT"
const WRONG_SERVER = "WRONG_SERVER"
const KEY_NOT_FOUND = "KEY_NOT_FOUND"
const ITEM_EXISTS = "ITEM_EXISTS"
const ITEM_NOT_FOUND = "ITEM_NOT_FOUND"

const ERROR_DIAL_TCP = "ERROR_DIAL_TCP"
const UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


type libstore struct {
	masterServ *rpc.Client
	allServerNodes []storagerpc.Node
	serverConnMap map[storagerpc.Node]*rpc.Client
	mode       LeaseMode
	hostPort   string
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

/* Begin defining interface to enable sorting of Nodes by NodeID */

type NodeByID []storagerpc.Node

func (nodeList NodeByID) Len() int {
	return len(nodeList)
}

func (nodeList NodeByID) Swap(i,j int) {
	nodeList[i], nodeList[j] = nodeList[j], nodeList[i]
}

func (nodeList NodeByID) Less(i,j int) bool {
	return nodeList[i].NodeID < nodeList[j].NodeID
}

/* End interface definition */

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

	//Convert to sort interface type and sort
	var toBeSortedNodes NodeByID = NodeByID(getServReply.Servers)
	sort.Sort(toBeSortedNodes)


	//Convert back to []Node type
	var sortedNodes []storagerpc.Node = []storagerpc.Node(toBeSortedNodes)
	fmt.Println(sortedNodes)

	//TODo add master server to map?

	var newLs libstore = libstore{
		masterServ: masterServ,
		allServerNodes: sortedNodes,
		serverConnMap: make(map[storagerpc.Node]*rpc.Client),
		mode:       mode,
		hostPort:   myHostPort,
	}

	return &newLs, nil
}

func (ls *libstore) getAndCacheNode(key string) (*rpc.Client, error) {
	var hashCode uint32 = StoreHash(key)

	//fmt.Println("allServerNodes len:", len(ls.allServerNodes), "\ndetails:")
	//fmt.Println(ls.allServerNodes)

	var targetNode storagerpc.Node = ls.allServerNodes[0]

	//Find target server
	for _,currNode := range(ls.allServerNodes) {
		if(currNode.NodeID < hashCode) {
			//take the first NodeID that > hashCode
			targetNode = currNode
			break
		} 
	}

	serverConn,isCached := ls.serverConnMap[targetNode]

	if(!isCached) {
		//initialise err variable
		err := errors.New("Initialise value for err variable")
		serverConn, err = rpc.DialHTTP("tcp", targetNode.HostPort)

		if(err != nil) {
			fmt.Println("Error DialHTTP in libstore")
			return nil, errors.New(ERROR_DIAL_TCP)
		
		} else {
			//add to cache
			ls.serverConnMap[targetNode] = serverConn
		} 
	}

	return serverConn,nil
}

func (ls *libstore) Get(key string) (string, error) {

	var wantLease bool = false

	if ls.mode == Never {
		wantLease = false
	}
	//TODO else clause

	serverConn, err := ls.getAndCacheNode(key)

	if(err != nil) {
		//error dialling
		return "", err
	}

	getArgs := storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort,
	}	

	var reply storagerpc.GetReply
	err = serverConn.Call("StorageServer.Get", getArgs, &reply)

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

	serverConn, err := ls.getAndCacheNode(key)

	if(err != nil) {
		//error dialling
		return err
	}	

	var reply storagerpc.PutReply
	err = serverConn.Call("StorageServer.Put", putArgs, &reply)

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

	serverConn, err := ls.getAndCacheNode(key)

	if(err != nil) {
		//error dialling
		return err
	}

	var reply storagerpc.DeleteReply

	err = serverConn.Call("StorageServer.Delete", delArgs, &reply)

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

	serverConn, err := ls.getAndCacheNode(key)

	if(err != nil) {
		//error dialling
		return make([]string,0),err
	}

	getArgs := storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort,
	}

	var reply storagerpc.GetListReply

	err = serverConn.Call("StorageServer.GetList", getArgs, &reply)

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

	serverConn, err := ls.getAndCacheNode(key)

	if(err != nil) {
		//error dialling
		return err
	}	

	putArgs := storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}

	var reply storagerpc.PutReply

	err = serverConn.Call("StorageServer.RemoveFromList", putArgs, &reply)

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
	serverConn, err := ls.getAndCacheNode(key)

	if(err != nil) {
		//error dialling
		return err
	}

	putArgs := storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}

	var reply storagerpc.PutReply

	err = serverConn.Call("StorageServer.AppendToList", putArgs, &reply)

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

/*

How does libstore know where to revoke?

*/