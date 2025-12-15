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

type StoreArg struct {
	Key         *big.Int
	FileContent []byte
}

type StoreReply struct {
	Success bool
	Err     string
}

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

type GetPredArgs struct{}

type GetPredReply struct {
	Node NodeDetails
}

type NotifyArgs struct {
	Node NodeDetails
}

type NotifyReply struct {
	Success bool
}

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

	// crash because n.Successor is nil
	if n.Successor == nil {
		fmt.Println("n.Successor.Id is nil")
	}

	if between(id, n.Id, n.Successor.Id) {
		return true, n.Successor
	} else {
		nprim := n.closestPrecedingNode(id)
		return false, nprim
	}
}

func (n *Node) Create() {

	//n.Id = big.NewInt(0) // temp to prevent crash, we need to actually assign the real id
	n.Predecessor = nil
	n.Successor = n
	n.next = 0
	//n.FingerTable = make([]*Node, m)

}

func (n *Node) Join(nprim *Node) {
	n.Predecessor = nil

	_, n.Successor = nprim.findSucc(n.Id)
}

func (n *Node) stabilize() {
	x := n.Successor.getPredecessor()

	if x != nil && between(x.Id, n.Id, n.Successor.Id) { // if x.id is between n.Id and n.successor.ID
		n.Successor = x // update our succesor to the one that was between
	}
	n.Successor.notifyRemote(n)
}

func (n *Node) fixFingers() {

	two := big.NewInt(2)
	exponent := big.NewInt(int64(n.next))
	offset := new(big.Int).Exp(two, exponent, nil) // 2^(next - 1)
	target := new(big.Int).Add(n.Id, offset)       // n + 2^(next - 1)

	_, nextsuccessor := n.findSucc(target)

	n.FingerTable[n.next] = nextsuccessor

	n.next = n.next + 1
	if n.next >= m {
		n.next = 0
	}

}

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

func (n *Node) closestPrecedingNode(id *big.Int) *Node {
	for i := m - 1; i >= 0; i-- {
		if n.FingerTable[i] != nil {
			if between(n.FingerTable[i].Id, n.Id, id) {
				return n.FingerTable[i]
			}
		}
	}
	return n
}

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

func (n *Node) notify(nprim *Node) {
	if n.Predecessor == nil || between(nprim.Id, n.Predecessor.Id, n.Id) {
		n.Predecessor = nprim
	}
}

func find(Id *big.Int, start *Node) *Node {
	found, nextNode := false, start
	i := 0
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

func (n *Node) Put(args *StoreArg, reply *StoreReply) error {
	n.Store(args.Key, args.FileContent)

	reply.Success = true
	return nil
}

func (n *Node) Store(key *big.Int, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.bucket == nil {
		n.bucket = make(map[string][]byte)
	}

	str := fmt.Sprintf("%d", key)
	n.bucket[str] = data
}

func (n *Node) GetPredecessorRPC(args *GetPredArgs, reply *GetPredReply) error {
	// Lock before accessing state
	n.mu.Lock()
	defer n.mu.Unlock()

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
	if n.Address == node.Address && n.Port == node.Port {
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

	address := fmt.Sprintf("%s:%d", n.Address, n.Port)
	ok := call("Node.NotifyRPC", address, &args, &reply)
	if !ok {
		fmt.Println("Unable to notify RPC")
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
