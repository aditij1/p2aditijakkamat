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

const DBG = false

type LeaseWrapper struct {
	lease       storagerpc.Lease
	timeGranted time.Time
	hostport    string
}

// For each key to track modifications to it
type TrackPending struct {
	pending   int           // Number of modifications waiting
	pendingCh chan chan int // When a function wants to modify the key, puts a request on this channel
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
	pendingMap          map[string]*TrackPending // Maps key to struct that tracks who wants to modify key next
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
		pendingMap:          make(map[string]*TrackPending),
		connections:         make(map[string]*rpc.Client)}

	l, err2 := net.Listen("tcp", net.JoinHostPort("localhost", strconv.Itoa(port)))
        if err2 != nil {
                fmt.Println(err2)
                return nil, err2
        }
       // go http.Serve(l, nil)

	err1 := rpc.RegisterName("StorageServer", storagerpc.Wrap(&server))
	if err1 != nil {
		fmt.Println(err1)
		return nil, err1
	}
	rpc.HandleHTTP()
	go http.Serve(l, nil)

	if masterServerHostPort == "" { // Server is a master
		server.servers = make([]storagerpc.Node, numNodes)
		server.servers[server.nextNode] = server.node
		server.nextNode++
		if numNodes == 1 {
			return &server, nil
		} else {
			for {
				select {
				case <-server.allServersReady:
					server.allServersReadyBool = true
					server.lbRange, server.isTopRing = findLowerbound(server.servers, nodeID)
					return &server, nil
				}
			}
		}
	} else { // Slave server
		cli, err1 := rpc.DialHTTP("tcp", masterServerHostPort)
		if err1 != nil {
			fmt.Println("got in this case")
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
	if DBG {
		fmt.Println("Entered storage server get")
	}

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.dataLock.Lock()
	val, ok := ss.dataStore[args.Key]
	ss.dataLock.Unlock()

	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = val.(string)
		if args.WantLease && ss.pendingMap[args.Key].pending == 0 {
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
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if DBG {
		fmt.Println("Entered storageserver delete")
	}
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
	pendingModifications, ok := ss.pendingMap[args.Key]

	if !ok {
		reply.Status = storagerpc.KeyNotFound
		ss.leaseLock.Unlock()
		return nil
	}

	pendingModifications.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]
	ss.leaseLock.Unlock()

	if pendingModifications.pending > 1 { // Block until it's our turn to modify key
		response := make(chan int)
		pendingModifications.pendingCh <- response
		<-response
	}

	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}

	ss.dataLock.Lock()
	delete(ss.dataStore, args.Key)
	ss.dataLock.Unlock()
	reply.Status = storagerpc.OK
	pendingModifications.pending--
Loop:
	for {
		select {
		case ch := <-pendingModifications.pendingCh:
			ch <- 1
			break Loop
		default:
			break Loop
		}
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if DBG {
		fmt.Println("Entered storageserver GetList")
	}
	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.dataLock.Lock()
	val, ok := ss.dataStore[args.Key]
	ss.dataLock.Unlock()
	if ok {
		reply.Status = storagerpc.OK
		reply.Value = ListToSlice(val.(*list.List))
		if args.WantLease && ss.pendingMap[args.Key].pending == 0 {
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
	if DBG {
		fmt.Println("Entered storage server put with key ", args.Key)
	}

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Leasing check
	ss.leaseLock.Lock()
	// If we have not seen this key before, initialize lease tracker
	_, ok := ss.pendingMap[args.Key]
	if !ok {
		ss.pendingMap[args.Key] = &TrackPending{pending: 0, pendingCh: make(chan chan int, 1)}
	}
	pendingModifications, _ := ss.pendingMap[args.Key]
	pendingModifications.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]
	ss.leaseLock.Unlock()
	if pendingModifications.pending > 1 { // Block until it's our turn to modify key
		response := make(chan int)
		pendingModifications.pendingCh <- response
		<-response
	}
	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}
	ss.dataLock.Lock()
	ss.dataStore[args.Key] = args.Value
	reply.Status = storagerpc.OK
	ss.dataLock.Unlock()
	pendingModifications.pending--
Loop:
	for {
		select {
		case ch := <-pendingModifications.pendingCh:
			ch <- 1
			break Loop
		default:
			break Loop
		}
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if DBG {
		fmt.Println("Entered storageserver AppendToList")
	}

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// Leasing check
	ss.leaseLock.Lock()
	// If we have not seen this key before, initialize lease tracker
	_, ok := ss.pendingMap[args.Key]
	if !ok {
		ss.pendingMap[args.Key] = &TrackPending{pending: 0, pendingCh: make(chan chan int, 1)}
	}
	pendingModifications, _ := ss.pendingMap[args.Key]
	pendingModifications.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]

	ss.leaseLock.Unlock()

	if pendingModifications.pending > 1 { // Block until it's our turn to modify key
		response := make(chan int)
		pendingModifications.pendingCh <- response
		<-response
	}
	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}
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
				pendingModifications.pending--
			Loop1:
				for {
					select {
					case ch := <-pendingModifications.pendingCh:
						ch <- 1
						break Loop1
					default:
						break Loop1
					}
				}
				return nil
			}
		}
		listVal.PushBack(args.Value)
	}
	reply.Status = storagerpc.OK
	pendingModifications.pending--
Loop:
	for {
		select {
		case ch := <-pendingModifications.pendingCh:
			ch <- 1
			break Loop
		default:
			break Loop
		}
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if DBG {
		fmt.Println("Entered storageserver RemoveFromList")
	}

	if !ss.inRange(libstore.StoreHash(args.Key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// Leasing check
	ss.leaseLock.Lock()
	pendingModifications, ok := ss.pendingMap[args.Key]
	// Initialize lease tracker if we haven't seen this key yet
	if !ok {
		reply.Status = storagerpc.KeyNotFound
		ss.leaseLock.Unlock()
		return nil
	}
	pendingModifications.pending++
	leaseHolders, ok := ss.leaseStore[args.Key]
	ss.leaseLock.Unlock()
	if pendingModifications.pending > 1 { // Block until it's our turn to modify key
		response := make(chan int)
		pendingModifications.pendingCh <- response
		<-response
	}
	if ok {
		ss.revokeLeases(leaseHolders, args.Key)
	}

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
				pendingModifications.pending--
			Loop1:
				for {
					select {
					case ch := <-pendingModifications.pendingCh:
						ch <- 1
						break Loop1
					default:
						break Loop1
					}
				}
				return nil
			}
		}
		reply.Status = storagerpc.ItemNotFound
		pendingModifications.pending--
	Loop:
		for {
			select {
			case ch := <-pendingModifications.pendingCh:
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
		if time.Since(leaseWrap.timeGranted).Seconds() >
			storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds {
			leaseHolders.Remove(e)
			continue
		}
		successChan := make(chan error)
		go ss.waitForRevokeLease(leaseWrap.hostport, key, successChan)
	Loop:
		for {
			select {
			case err := <-successChan:
				if err != nil {
					fmt.Println(err)
				} else {
					break Loop
				}
			default:
				if time.Since(leaseWrap.timeGranted).Seconds() >
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
