package storageserver

import (
	"container/list"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type storageServer struct {
	node            storagerpc.Node
	servers         []storagerpc.Node
	numNodes        int
	nextNode        int             // Idx in servers for next incoming Register request
	seenNodes       map[uint32]bool // Seen node ids for master storage to keep track of which nodes are ready
	allServersReady chan int
	dataStore       map[string]interface{}
	mu              *sync.Mutex
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
		servers:         make([]storagerpc.Node, numNodes),
		numNodes:        numNodes,
		nextNode:        0,
		seenNodes:       make(map[uint32]bool),
		allServersReady: make(chan int),
		node:            storagerpc.Node{HostPort: net.JoinHostPort("localhost", strconv.Itoa(port)), NodeID: nodeID},
		dataStore:       make(map[string]interface{}),
		mu:              &sync.Mutex{}}
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

	if masterServerHostPort == "" { // Server is a master
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
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if cap(ss.servers) == 0 {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	fmt.Println("Entered storage server get")
	ss.mu.Lock()
	defer ss.mu.Unlock()
	val, ok := ss.dataStore[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = val.(string)
		// TODO: leasing
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	fmt.Println("Entered storageserver delete")
	ss.mu.Lock()
	defer ss.mu.Unlock()
	_, ok := ss.dataStore[args.Key]
	if ok {
		delete(ss.dataStore, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func ListToSlice(list *list.List) []string {
	slice := make([]string, list.Len())
	i := 0
	for e := list.Front(); e != nil; e = e.Next() {
		slice[i] = e.Value.(string)
		i++
	}
	return slice
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	fmt.Println("Entered storageserver GetList")
	ss.mu.Lock()
	defer ss.mu.Unlock()
	val, ok := ss.dataStore[args.Key]
	if ok {
		reply.Status = storagerpc.OK
		reply.Value = ListToSlice(val.(*list.List))
		// TODO: leasing
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver Put")
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.dataStore[args.Key] = args.Value
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver AppendToList")
	ss.mu.Lock()
	defer ss.mu.Unlock()
	val, ok := ss.dataStore[args.Key]
	if !ok { // Create a new list and add this element
		l := list.New()
		l.PushBack(args.Value)
		ss.dataStore[args.Key] = l
	} else {
		// Check if element is already in list
		listVal := val.(*list.List)
		for e := listVal.Front(); e != nil; e = e.Next() {
			if args.Value == e.Value.(string) {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
		fmt.Println("Casted interface to list")
		listVal.PushBack(args.Value)
	}
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver RemoveFromList")
	ss.mu.Lock()
	defer ss.mu.Unlock()
	val, ok := ss.dataStore[args.Key]
	fmt.Println("Checked the dataStore")
	if !ok {
		fmt.Println("!ok in RemoveFromList")
		reply.Status = storagerpc.KeyNotFound
		return nil
	} else {
		listVal := val.(*list.List)
		fmt.Println("casted to list")
		fmt.Println("About to iterate through list")
		for e := listVal.Front(); e != nil; e = e.Next() {
			fmt.Println("Iterating through list")
			if args.Value == e.Value.(string) {
				reply.Status = storagerpc.OK
				listVal.Remove(e)
				return nil
			}
		}
		reply.Status = storagerpc.ItemNotFound
		return nil
	}
}
