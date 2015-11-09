package storageserver

import (
	"container/list"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/libstore"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"
)

type LeaseWrapper struct {
	lease storagerpc.Lease
	timeGranted time.Time
	hostport string
}

type storageServer struct {
	node            storagerpc.Node
	servers         []storagerpc.Node
	numNodes        int
	nextNode        int             // Idx in servers for next incoming Register request
	seenNodes       map[uint32]bool // Seen node ids for master storage to keep track of which nodes are ready
	allServersReady chan int
	dataStore       map[string]interface{}  // Maps key to value
	dataLock        *sync.Mutex
	leaseStore      map[string]*list.List  // Maps key to hostports (LeaseWrapper) that have a lease for that key
	leaseLock       *sync.Mutex
	lbRange         uint32          // Lowerbound for range (for userId hash) that this server handles
	ubRange         uint32          // Upperbound for range that this server handles
	isTopRing       bool            // True iff node is at top of ring (smallest nodeID)
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
		dataLock:        &sync.Mutex{},
		leaseStore:      make(map[string]*list.List),
		leaseLock:       &sync.Mutex{},
	        ubRange:         nodeID,
	        isTopRing:       false}
	//server.mu.Lock()
	//defer server.mu.Unlock()

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
					server.lbRange, server.isTopRing = findLowerbound(server.servers, nodeID)
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
					server.lbRange, server.isTopRing = findLowerbound(server.servers, nodeID)
					return &server, nil
				}
			}
		}
	}
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()
	node := args.ServerInfo
	_, ok := ss.seenNodes[node.NodeID]
	if !ok {
		ss.seenNodes[node.NodeID] = true
		ss.servers[ss.nextNode] = node
		ss.nextNode++
	}
	if ss.nextNode == ss.numNodes {
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
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()
	if ss.nextNode == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	fmt.Println("Entered storage server get")
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	val, ok := ss.dataStore[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = val.(string)
		if args.WantLease {
			lease := storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
			reply.Lease = lease
			// Track that this lease was issued
			leaseWrap := LeaseWrapper{lease: lease, timeGranted: time.Now(), hostport: args.HostPort}
			_, ok := ss.leaseStore[args.Key]
			if !ok {
				ss.leaseStore[args.Key] = list.New()
			}
			ss.leaseStore[args.Key].PushBack(leaseWrap)
		}
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	fmt.Println("Entered storageserver delete")
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
                return nil
        }

	_, ok := ss.dataStore[args.Key]
	if ok {
		delete(ss.dataStore, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	fmt.Println("Entered storageserver GetList")
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
                return nil
        }

	val, ok := ss.dataStore[args.Key]
	if ok {
		reply.Status = storagerpc.OK
		reply.Value = ListToSlice(val.(*list.List))
		if args.WantLease {
			lease := storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
			reply.Lease = lease
                        // Track that this lease was issued
			leaseWrap := LeaseWrapper{lease: lease, timeGranted: time.Now(), hostport: args.HostPort}
			_, ok := ss.leaseStore[args.Key]
			if !ok {
				ss.leaseStore[args.Key] = list.New()
			}
			ss.leaseStore[args.Key].PushBack(leaseWrap)
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver Put")
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
                return nil
        }

	// Leasing check
	leaseHolders, ok := ss.leaseStore[args.Key]
	if ok {
		for e := leaseHolders.Front(); leaseHolders != nil; e = e.Next() {
			leaseWrap := e.Value.(LeaseWrapper)
			cli, err := rpc.DialHTTP("tcp",  leaseWrap.hostport)
			if err != nil {
				fmt.Println(err)
			}
			args := storagerpc.RevokeLeaseArgs{Key: args.Key}

			var reply storagerpc.RevokeLeaseReply
			cli.Call("Libstore.RevokeLease", args, &reply)
			if reply.Status != storagerpc.OK {
				// Wait for lease to expire
				for {
					time.Sleep(time.Second)
					if (time.Now().Unix() - leaseWrap.timeGranted.Unix() >
						storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) {
						break
					}
				}
			}

		}


	}

	ss.dataStore[args.Key] = args.Value
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver AppendToList")
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
                return nil
        }

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
		listVal.PushBack(args.Value)
	}
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver RemoveFromList")
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
                return nil
        }

	val, ok := ss.dataStore[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	} else {
		listVal := val.(*list.List)
		for e := listVal.Front(); e != nil; e = e.Next() {
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

// Helper functions below this point
func (ss *storageServer) inRange(hash uint32) bool {
	if ss.isTopRing {
		if hash > ss.lbRange || hash < ss.ubRange {
			return false
		} else {
			return true
		}
	} else {
		if hash > ss.ubRange || hash < ss.lbRange {
			return false
		} else {
			return true
		}
	}
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

// Defining type for sorting servers by nodeID
type ServersSlice []storagerpc.Node

func (servers ServersSlice) Len() int {
        return len(servers)
}

func (servers ServersSlice) Swap(i, j int) {
        servers[i], servers[j] = servers[j], servers[i]
}

func (servers ServersSlice) Less(i, j int) bool {
        return servers[i].NodeID < servers[j].NodeID
}

// Returns the 1 + first nodeID less than it
func findLowerbound(servers []storagerpc.Node, nodeID uint32) (uint32, bool) {
        sort.Sort(ServersSlice(servers))  // sorts servers by NodeID
        // First find idx of nodeID
        idx := 0
        for i := 0; i < len(servers); i++ {
                if nodeID == servers[i].NodeID {
                        idx = i
                        break
                }
        }
        if idx != 0 {
                return servers[idx-1].NodeID + 1, false
        }
        return servers[len(servers)-1].NodeID + 1, true  // wrap around case
}
