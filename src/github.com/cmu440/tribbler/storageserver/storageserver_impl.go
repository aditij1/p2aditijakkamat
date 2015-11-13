package storageserver

import (
	"container/list"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"
)

type LeaseWrapper struct {
	lease       storagerpc.Lease
	timeGranted time.Time
	hostport    string
}

type LeaseTracker struct {
	pending int
	pendingCh chan chan int
}

type storageServer struct {
	node                storagerpc.Node
	servers             []storagerpc.Node
	numNodes            int
	nextNode            int             // Idx in servers for next incoming Register request
	seenNodes           map[uint32]bool // Seen node ids for master storage to keep track of which nodes are ready
	allServersReady     chan int
	dataStore           map[string]interface{} // Maps key to value
	dataLock            *sync.Mutex
	leaseStore          map[string]*list.List // Maps key to hostports (LeaseWrapper) that have a lease for that key
	leaseLock           *sync.Mutex
	lbRange             uint32          // Lowerbound for range (for userId hash) that this server handles
	ubRange             uint32          // Upperbound for range that this server handles
	isTopRing           bool            // True iff node is at top of ring (smallest nodeID)
	success             chan error      // For revoke lease
	canGrantLease       map[string]bool // True iff server can grant a lease for this key
	allServersReadyBool bool
	leaseTrackers       map[string]*LeaseTracker  // Maps key to lease tracker
	connections         map[string]*rpc.Client   // Maps hostport to connection
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
		numNodes:            numNodes,
		nextNode:            0,
		seenNodes:           make(map[uint32]bool),
		allServersReady:     make(chan int),
		node:                storagerpc.Node{HostPort: net.JoinHostPort("localhost", strconv.Itoa(port)), NodeID: nodeID},
		dataStore:           make(map[string]interface{}),
		dataLock:            &sync.Mutex{},
		leaseStore:          make(map[string]*list.List),
		leaseLock:           &sync.Mutex{},
		ubRange:             nodeID,
		isTopRing:           false,
		success:             make(chan error),
		canGrantLease:       make(map[string]bool),
		allServersReadyBool: false,
	        leaseTrackers:       make(map[string]*LeaseTracker),
	        connections:         make(map[string]*rpc.Client)}
	//server.mu.Lock()
	//defer server.mu.Unlock()
	fmt.Println("nodeID is: ", nodeID)

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
		server.servers = make([]storagerpc.Node, numNodes)
		fmt.Println("numNodes master is: ", numNodes)
		server.servers[server.nextNode] = server.node
		server.nextNode++
		if numNodes == 1 {
			return &server, nil
		} else {
			for {
				select {
				case <-server.allServersReady:
					fmt.Println("got from allServersReady")
					server.allServersReadyBool = true
					server.lbRange, server.isTopRing = findLowerbound(server.servers, nodeID)
					return &server, nil
				}
			}
		}
	} else { // Slave server
		fmt.Println("numNodes slave is: ", numNodes)
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
	fmt.Println("Entered register server")
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
		if !ss.allServersReadyBool {
			ss.allServersReady <- 1
		}
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
		if args.WantLease && ss.leaseTrackers[args.Key].pending==0 {
			ss.leaseLock.Lock()
			lease := storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
			reply.Lease = lease
			// Track that this lease was issued
			leaseWrap := LeaseWrapper{lease: lease, timeGranted: time.Now(), hostport: args.HostPort}
			fmt.Println("time granted: ", leaseWrap.timeGranted)
			_, ok := ss.leaseStore[args.Key]
			if !ok {
				ss.leaseStore[args.Key] = list.New()
			}
			ss.leaseStore[args.Key].PushBack(leaseWrap)
			ss.leaseLock.Unlock()
		}
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	fmt.Println("Entered storageserver delete")
	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	_, ok := ss.dataStore[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}

	// Leasing check
	ss.leaseLock.Lock()
	ss.canGrantLease[args.Key] = false
	leaseTracker, _ := ss.leaseTrackers[args.Key]
	leaseTracker.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]
	ss.leaseLock.Unlock()

	if leaseTracker.pending > 1 {  // Block until it's our turn to modify key
		fmt.Println("Got to pending case")
		response := make(chan int)
		leaseTracker.pendingCh <- response
		<- response
	}

	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}
	//ss.leaseLock.Unlock()

	ss.dataLock.Lock()
	delete(ss.dataStore, args.Key)
	ss.canGrantLease[args.Key] = true
	ss.dataLock.Unlock()
	reply.Status = storagerpc.OK
	leaseTracker.pending--
        Loop:
	for {
		select {
		case ch := <- leaseTracker.pendingCh:
			ch <- 1
			break Loop
		default:
			break Loop
		}
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
		if args.WantLease && ss.leaseTrackers[args.Key].pending==0 {
			ss.leaseLock.Lock()
			lease := storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
			reply.Lease = lease
			// Track that this lease was issued
			leaseWrap := LeaseWrapper{lease: lease, timeGranted: time.Now(), hostport: args.HostPort}
			_, ok := ss.leaseStore[args.Key]
			if !ok {
				ss.leaseStore[args.Key] = list.New()
			}
			ss.leaseStore[args.Key].PushBack(leaseWrap)
			ss.leaseLock.Unlock()
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storage server put with key ", args.Key)

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Leasing check
	ss.leaseLock.Lock()
	ss.canGrantLease[args.Key] = false
	// If we have not seen this key before, initialize lease tracker
	_, ok := ss.leaseTrackers[args.Key]
	if !ok {
		ss.leaseTrackers[args.Key] = &LeaseTracker{pending: 0, pendingCh: make(chan chan int, 1)}
	}
	leaseTracker, _ := ss.leaseTrackers[args.Key]
	leaseTracker.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]
	ss.leaseLock.Unlock()
	fmt.Println("Pending for key " + args.Key + " is ", leaseTracker.pending)
	if leaseTracker.pending > 1 {  // Block until it's our turn to modify key
		fmt.Println("got to this case")
		response := make(chan int)
		leaseTracker.pendingCh <- response
		<- response
		fmt.Println("got response")
	}
	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}
	fmt.Println("finished revoking leases")
	//ss.leaseLock.Unlock()
	ss.dataLock.Lock()
	ss.dataStore[args.Key] = args.Value
	ss.canGrantLease[args.Key] = true
	reply.Status = storagerpc.OK
	ss.dataLock.Unlock()
	leaseTracker.pending--
        Loop:
	for {
		select {
		case ch := <-leaseTracker.pendingCh:
			ch <- 1
			break Loop
		default:
			break Loop
		}
	}
	fmt.Println("Made update to key ", args.Key)
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver AppendToList")

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Leasing check
	ss.leaseLock.Lock()
	ss.canGrantLease[args.Key] = false
	// If we have not seen this key before, initialize lease tracker
	_, ok := ss.leaseTrackers[args.Key]
	if !ok {
		ss.leaseTrackers[args.Key] = &LeaseTracker{pending: 0, pendingCh: make(chan chan int, 1)}
	}
	leaseTracker, _ := ss.leaseTrackers[args.Key]
	leaseTracker.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]

	ss.leaseLock.Unlock()

	if leaseTracker.pending > 1 { // Block until it's our turn to modify key
		fmt.Println("got to wait case")
		response := make(chan int)
		leaseTracker.pendingCh <- response
		<- response
	}
	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}
	//ss.leaseLock.Unlock()
	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()
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
				leaseTracker.pending--
				return nil
			}
		}
		listVal.PushBack(args.Value)
	}
	ss.canGrantLease[args.Key] = true
	reply.Status = storagerpc.OK
	leaseTracker.pending--
        Loop:
	for {
		select {
		case ch:= <-leaseTracker.pendingCh:
			ch <- 1
			break Loop
		default:
			break Loop
		}
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Entered storageserver RemoveFromList")

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Leasing check
	ss.leaseLock.Lock()
	ss.canGrantLease[args.Key] = false
	leaseTracker, _ := ss.leaseTrackers[args.Key]
	leaseTracker.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]
	ss.leaseLock.Unlock()
	if leaseTracker.pending > 1 {  // Block until it's our turn to modify key
		fmt.Println("Got to wait2 case")
		response := make(chan int)
		leaseTracker.pendingCh <- response
		<- response
	}

	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}
	//ss.leaseLock.Unlock()

	ss.dataLock.Lock()
	defer ss.dataLock.Unlock()
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
				ss.canGrantLease[args.Key] = true
				leaseTracker.pending--
				return nil
			}
		}
		reply.Status = storagerpc.ItemNotFound
		ss.canGrantLease[args.Key] = true
		leaseTracker.pending--
	        Loop:
		for {
			select {
			case ch := <-leaseTracker.pendingCh:
				ch <- 1
				break Loop
			default:
				break Loop
			}
		}
		return nil
	}
}

// Helper functions below this point

func (ss *storageServer) revokeLeases(leaseHolders *list.List, key string) {
	for e := leaseHolders.Front(); e != nil; e = e.Next() {
		leaseWrap := e.Value.(LeaseWrapper)
		// If lease has already expired, don't do anything
		fmt.Println("Curr time in revokeLeases: ", time.Now())
		if time.Now().Unix()-leaseWrap.timeGranted.Unix() >
			storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds {
			leaseHolders.Remove(e)
			fmt.Println("Lease already expired")
			continue
		}
		successChan := make(chan error)
		go ss.waitForRevokeLease(leaseWrap.hostport, key, successChan)
	        Loop:
		for {
			select {
			case err := <-successChan:
				fmt.Println("got response from leasecallbacks")
				if err != nil {
					fmt.Println(err)
				} else {
					break Loop
				}
			default:
				if time.Now().Unix()-leaseWrap.timeGranted.Unix() >
					storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds {
					fmt.Println("timed out")
					break Loop
				}
				//time.Sleep(time.Second)
			}
		}
		leaseHolders.Remove(e)
	}

}

func (ss *storageServer) waitForRevokeLease(hostport string, key string, successChan chan error) {
	_, ok := ss.connections[hostport]
	if !ok {
		cli, err := rpc.DialHTTP("tcp", hostport)
		ss.connections[hostport] = cli
		if err != nil {
			fmt.Println(err)
		}
	}
	args := storagerpc.RevokeLeaseArgs{Key: key}
	var reply storagerpc.RevokeLeaseReply
	successChan <- ss.connections[hostport].Call("LeaseCallbacks.RevokeLease", args, &reply)
}

func (ss *storageServer) inRange(hash uint32) bool {
	if len(ss.servers) == 1 {
		return true
	}
	if ss.isTopRing {
		if hash <= ss.ubRange || hash > ss.lbRange {
			return true
		} else {
			return false
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
	sort.Sort(ServersSlice(servers)) // sorts servers by NodeID
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
	return servers[len(servers)-1].NodeID + 1, true // wrap around case
}
