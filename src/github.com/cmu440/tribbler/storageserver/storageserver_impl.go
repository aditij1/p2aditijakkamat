package storageserver

import (
	"errors"
	"container/list"
	"fmt"
	//"github.com/cmu440/tribbler/util"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
)

type storageServer struct {
	nodeList *list.List
	node storagerpc.Node
	userIdTribbleMap map[string]*list.List  // Maps userId to a list of tribble Ids for that user
	userIdSubscriptionMap map[string]*list.List  // Maps userId to list of users the user is subscribed to
	tribbleIdTribbleMap map[int]string  // Maps tribbleId to string form of Tribble
	mu *sync.Mutex

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
	server := storageServer{
		nodeList: list.New(),
		node: storagerpc.Node{HostPort: net.JoinHostPort("localhost", strconv.Itoa(port)), NodeID: nodeID},
		userIdTribbleMap: make(map[string]*list.List),
		userIdSubscriptionMap: make(map[string]*list.List),
		tribbleIdTribbleMap: make(map[int]string),
		mu: &sync.Mutex{}}
	if masterServerHostPort != "" {  // Server is a master
		server.nodeList.PushBack(server.node)
		err1 := rpc.RegisterName("StorageServer", storagerpc.Wrap(&server))
		if err1 != nil {
			fmt.Println(err1)
			return nil, err1
		}
		rpc.HandleHTTP()
		l, err2 := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err2 != nil {
			fmt.Println(err2)
			return nil, err2
		}
		go http.Serve(l, nil)
	}



	return &server, nil

}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
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
