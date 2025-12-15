package main

import (
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const m = 3 // finger table entries

type Node struct {
	Address     string
	Port        int
	Id          *big.Int
	Successor   *Node
	Predecessor *Node
	FingerTable []*Node

	bucket map[string][]byte

	next int

	mu sync.Mutex
}

// structs for RPC

type StoreArg struct {
	Key         *big.Int
	FileContent []byte
}

type StoreReply struct {
	Success bool
	Err     string
}

type NodeDetails struct {
	Address string
	Port    int
	Id      *big.Int
}

type FindSuccArgs struct {
	Id *big.Int
}

type FindSuccReply struct {
	Found bool
	Node  NodeDetails
}

type GetPredArgs struct {
}

type GetPredReply struct {
	Node NodeDetails
}

type NotifyArgs struct {
	Node NodeDetails
}

type NotifyReply struct {
	Success bool
}

type GetFilesArgs struct {
	PredId *big.Int
}

type GetFilesReply struct {
	Files map[string][]byte
}

type GetFileReply struct {
	Data []byte
}

type GetFileArgs struct {
	FileName string
}

// function to check if its local or need RPC
func (n *Node) findSucc(id *big.Int) (bool, *Node) {

	// make rpc call to the target node findsuccessor function
	if n.Address == node.Address && n.Port == node.Port {
		return n.findSuccessor(id)
	}

	args := FindSuccArgs{Id: id}
	reply := FindSuccReply{}

	add := fmt.Sprintf("%s:%d", n.Address, n.Port)
	ok := call("Node.FindSuccRPC", add, args, &reply)
	if !ok {
		return false, nil
	}

	// Convert the full node to only important details
	remoteNode := &Node{
		Address: reply.Node.Address,
		Port:    reply.Node.Port,
		Id:      reply.Node.Id,
	}

	return reply.Found, remoteNode

}

func (n *Node) FindSuccRPC(args *FindSuccArgs, reply *FindSuccReply) error {
	found, nextNode := n.findSuccessor(args.Id)
	reply.Found = found

	// fill reply data
	if nextNode != nil {
		reply.Node = NodeDetails{
			Address: nextNode.Address,
			Port:    nextNode.Port,
			Id:      nextNode.Id,
		}
	}

	return nil
}

// address should be IP:port
func call(method string, adress string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", adress)
	if err != nil {
		fmt.Printf("dialing:%s\n", err)
		return false
	}
	defer c.Close()

	err = c.Call(method, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (n *Node) findSuccessor(id *big.Int) (bool, *Node) {

	//check if id is between current node and successor
	if between(id, n.Id, n.Successor.Id) {
		return true, n.Successor
	} else {
		// Otherwise, find the closest node to id
		nprim := n.closestPrecedingNode(id)
		return false, nprim
	}
}

// create node used in creating the actual chord ring only
func (n *Node) Create() {

	n.Predecessor = nil
	n.Successor = n
	n.next = 0

}

// join is used instead of create when joining an existing chord ring
func (n *Node) Join(nprim *Node) {
	n.Predecessor = nil

	//use find successor to find our most suitable successor
	_, n.Successor = nprim.findSucc(n.Id)

	//check if out successor has any files that are more suitable for current node
	args := GetFilesArgs{n.Id}
	reply := GetFilesReply{}

	add := fmt.Sprintf("%s:%d", n.Successor.Address, n.Successor.Port)
	ok := call("Node.GetFilesPredRPC", add, &args, &reply)
	if !ok {
		return
	}

	//get thos file;;;
	for key, value := range reply.Files {
		n.bucket[key] = value
	}

}

func (n *Node) stabilize() {
	// get the current successors predecessor
	x := n.Successor.getPredecessor()

	// if x is a better successor, we update the successor to x
	if x != nil && between(x.Id, n.Id, n.Successor.Id) { // if x.id is between n.Id and n.successor.ID
		n.Successor = x // update our succesor to the one that was between
	}

	// Tell successor that we are the predecessor
	n.Successor.notifyRemote(n)
}

// Updates finger table entries
func (n *Node) fixFingers() {

	// take our id and add a offset
	two := big.NewInt(2)
	exponent := big.NewInt(int64(n.next))
	offset := new(big.Int).Exp(two, exponent, nil) // 2^(next - 1)

	sum := new(big.Int).Add(n.Id, offset) // n + 2^(next - 1)

	// Calculate ring size
	maxVal := new(big.Int).Exp(two, big.NewInt(m), nil)

	// Handle wrap-around, so it works like a circle
	target := new(big.Int).Mod(sum, maxVal)

	//find the successor for this target
	_, nextsuccessor := n.findSucc(target)

	//append to fingertable
	n.FingerTable[n.next] = nextsuccessor

	n.next = n.next + 1
	if n.next >= m {
		n.next = 0
	}

}

// Checks if a predecessor is still alive
// if not, it sets the predecessor to nil, so it can be replaced
func (n *Node) checkPredecessor() {
	if n.Predecessor != nil {
		add := fmt.Sprintf("%s:%d", n.Predecessor.Address, n.Predecessor.Port)
		c, err := net.DialTimeout("tcp", add, 5*time.Second)
		if err != nil {
			n.Predecessor = nil
			return
		}
		c.Close()
	}
}

// Find the closest node
func (n *Node) closestPrecedingNode(id *big.Int) *Node {

	for i := m - 1; i >= 0; i-- {

		if n.FingerTable[i] != nil {

			// If this finger is between "me" and the "target"
			if between(n.FingerTable[i].Id, n.Id, id) {
				// This takes us closer to the target
				// so we jump there
				return n.FingerTable[i]
			}
		}
	}

	// if all fingers fail, return self
	return n
}

// Check if id is in the correct range
func between(id, start, end *big.Int) bool {
	// if end > start (== 1)
	if end.Cmp(start) == 1 {
		// if id is > start and <= end,
		return id.Cmp(start) == 1 && id.Cmp(end) <= 0
	} else {
		// if id is > start or id is <= end (circular wrapping)
		return id.Cmp(start) == 1 || id.Cmp(end) <= 0
	}
}

// if the new node is a better fit to be the new predecessor that the current one
func (n *Node) notify(nprim *Node) {
	if n.Predecessor == nil || between(nprim.Id, n.Predecessor.Id, n.Id) {
		n.Predecessor = nprim
	}
}

// Iterates through the ring until the responsible node is found
func find(Id *big.Int, start *Node) *Node {
	found, nextNode := false, start
	i := 0
	// prevent infinite loops if the ring is broken
	for !found && i < 10 {
		found, nextNode = nextNode.findSucc(Id)
		i += 1
	}
	if found {
		return nextNode
	} else {
		fmt.Println("could not find node!!!")
		return nil
	}
}

// RPC handler for storing a file
func (n *Node) Put(args *StoreArg, reply *StoreReply) error {
	n.Store(args.Key, args.FileContent)

	reply.Success = true
	return nil
}

// lists files from current node that matches more closely to an id
func (n *Node) getFilesPred(id *big.Int) map[string][]byte {

	//make new map
	mfiles := make(map[string][]byte)

	//go through files
	for key, value := range n.bucket {
		//convert key (string) to big int
		keyBig := new(big.Int)
		keyBig.SetString(key, 10)

		//check if key matches other id better

		shouldKeep := between(keyBig, id, n.Id)

		if !shouldKeep {
			//copy over keys and values to new map and delete from our nodes map
			mfiles[key] = value
			delete(n.bucket, key)
		}

	}
	//return t

	return mfiles
}

func (n *Node) getFile(targetNode *Node, fileName string) []byte {

	// convert filename to hash
	key := hashString(fileName)
	// convert hash into string of hash
	keyStr := fmt.Sprintf("%d", key)

	// if the file is on the local node
	if targetNode.Id == n.Id {
		n.mu.Lock()
		defer n.mu.Unlock()
		data, exists := n.bucket[keyStr]
		if exists {
			return data
		} else {
			fmt.Println("File doesnt exist")
			return data
		}
	}

	args := GetFileArgs{keyStr}
	reply := GetFileReply{}

	// combine port + address into one string
	add := fmt.Sprintf("%s:%d", targetNode.Address, targetNode.Port)

	// connect to the other node and get the data
	ok := call("Node.GetFileRPC", add, args, &reply)
	if !ok {
		fmt.Println("Couldnt perform RPC on GetFile")
	}

	return reply.Data
}

func (n *Node) GetFileRPC(args *GetFileArgs, reply *GetFileReply) error {

	// Lock before accessing state
	n.mu.Lock()
	defer n.mu.Unlock()

	// if target node bucket doesnt contain any files
	if len(n.bucket) == 0 {
		fmt.Printf("bucket is empty\n")
		return nil

	} else {
		// grab file from bucket and check
		data, exists := n.bucket[args.FileName]
		if exists {
			reply.Data = data
		} else {
			fmt.Println("File doesnt exist")
		}
	}

	return nil
}

func (n *Node) GetFilesPredRPC(args *GetFilesArgs, reply *GetFilesReply) error {
	// Lock before accessing state
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.bucket) == 0 {
		//fmt.Printf("bucket is empty")
		return nil

	} else {
		reply.Files = n.getFilesPred(args.PredId)
	}

	return nil
}

// Store data to the local bucket map
func (n *Node) Store(key *big.Int, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// if the bucket is not defined
	if n.bucket == nil {
		n.bucket = make(map[string][]byte)
	}

	// convert hash to string
	str := fmt.Sprintf("%d", key)
	n.bucket[str] = data // store file
}

// gets the curreent node's predecessor
func (n *Node) GetPredecessorRPC(args *GetPredArgs, reply *GetPredReply) error {
	// Lock before accessing state
	n.mu.Lock()
	defer n.mu.Unlock()

	//
	if n.Predecessor != nil {
		reply.Node = NodeDetails{
			Address: n.Predecessor.Address,
			Port:    n.Predecessor.Port,
			Id:      n.Predecessor.Id,
		}
	} else {
		reply.Node = NodeDetails{
			Address: "",
			Port:    0,
			Id:      nil,
		}
	}

	return nil
}

func (n *Node) getPredecessor() *Node {
	// if its the local node
	if n.Address == node.Address && n.Port == node.Port {
		return n.Predecessor
	}

	args := GetPredArgs{}

	reply := GetPredReply{}

	address := fmt.Sprintf("%s:%d", n.Address, n.Port)
	ok := call("Node.GetPredecessorRPC", address, &args, &reply)
	if !ok || reply.Node.Address == "" {
		fmt.Println("Unable to getPredecessor RPC")
		return nil
	}

	return &Node{
		Address: reply.Node.Address,
		Port:    reply.Node.Port,
		Id:      reply.Node.Id,
	}
}

func (n *Node) notifyRemote(nprim *Node) {
	// if the target node is the local node
	if n.Address == nprim.Address && n.Port == nprim.Port {
		n.notify(nprim)
		return
	}

	args := NotifyArgs{
		Node: NodeDetails{
			Address: nprim.Address,
			Port:    nprim.Port,
			Id:      nprim.Id,
		},
	}

	reply := NotifyReply{}

	// combine port + address into one string
	address := fmt.Sprintf("%s:%d", n.Address, n.Port)
	ok := call("Node.NotifyRPC", address, &args, &reply)
	if !ok {
		fmt.Println("Unable perform notify RPC")
		nprim.Successor = nprim
	}

}

func (n *Node) NotifyRPC(args *NotifyArgs, reply *NotifyReply) error {
	updatedPred := &Node{
		Address: args.Node.Address,
		Port:    args.Node.Port,
		Id:      args.Node.Id,
	}

	n.notify(updatedPred)

	reply.Success = true
	return nil

}
