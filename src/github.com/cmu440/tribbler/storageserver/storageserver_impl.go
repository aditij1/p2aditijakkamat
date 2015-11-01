package storageserver

import (
	"container/list"
	"errors"
	"fmt"
	//"github.com/cmu440/tribbler/util"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type storageServer struct {
	node                  storagerpc.Node
	servers               []storagerpc.Node
	numNodes              int
	nextNode              int             // Idx in servers for next incoming Register request, used in startup phase for master server
	seenNodes             map[uint32]bool // Seen node ids for master storage to keep track of which nodes are ready
	allServersReady       chan int
	userIdTribbleMap      map[string]*list.List // Maps userId to a list of tribble Ids for that user
	userIdSubscriptionMap map[string]*list.List // Maps userId to list of users the user is subscribed to
	tribbleIdTribbleMap   map[int]string        // Maps tribbleId to string form of Tribble
	mu                    *sync.Mutex
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
		servers:               make([]storagerpc.Node, numNodes),
		numNodes:              numNodes,
		nextNode:              0,
		seenNodes:             make(map[uint32]bool),
		allServersReady:       make(chan int),
		node:                  storagerpc.Node{HostPort: net.JoinHostPort("localhost", strconv.Itoa(port)), NodeID: nodeID},
		userIdTribbleMap:      make(map[string]*list.List),
		userIdSubscriptionMap: make(map[string]*list.List),
		tribbleIdTribbleMap:   make(map[int]string),
		mu:                    &sync.Mutex{}}
	server.mu.Lock()
	defer server.mu.Unlock()

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

	if masterServerHostPort != "" { // Server is a master
		server.servers[server.nextNode] = server.node
		server.nextNode++
		if numNodes == 1 {
			return &server, nil
		} else {
			for {
				select {
				case <-server.allServersReady:
					return &server, nil
				}
			}
		}
	} else { // Slave server
		cli, err1 := rpc.DialHTTP("tcp", masterServerHostPort)
		if err1 != nil {
			fmt.Println(err1)
			return nil, err1
		}
		registerArgs := storagerpc.RegisterArgs{ServerInfo: server.node}
		var reply storagerpc.RegisterReply
		if err2 := cli.Call("StorageServer.RegisterServer", registerArgs, &reply); err2 != nil {
			return nil, err2
		}
		if reply.Status == storagerpc.OK {
			server.servers = reply.Servers
			return &server, nil
		} else {
			for {
				time.Sleep(time.Second)
				cli.Call("StorageServer.RegisterServer", registerArgs, &reply)
				if reply.Status == storagerpc.OK {
					server.servers = reply.Servers
					return &server, nil
				}
			}
		}
	}
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	node := args.ServerInfo
	_, ok := ss.seenNodes[node.NodeID]
	if !ok {
		ss.seenNodes[node.NodeID] = true
		ss.servers[ss.nextNode] = node
		ss.nextNode++
	}

	if cap(ss.servers) == 0 {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		ss.allServersReady <- 1
	} else {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	}
	return nil
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
