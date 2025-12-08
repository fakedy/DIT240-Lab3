package main

import (
	"math/big"
	"net"
)

const m = 3

type Node struct {
	Address     string
	Port        int
	Id          *big.Int
	Successor   *Node
	Predecessor *Node
	FingerTable []*Node

	next int
}

func (n *Node) findSuccessor(id *big.Int) *Node {
	if between(id, n.Id, n.Successor.Id) {
		return n.Successor
	} else {
		nprim := n.closestPrecedingNode(id)
		return nprim.findSuccessor(id)
	}
}

func (n *Node) Create() {
	n.Predecessor = nil
	n.Successor = n

}

func (n *Node) Join(nprim *Node) {
	n.Predecessor = nil
	n.Successor = nprim.findSuccessor(n.Id)
}

func (n *Node) stabilize() {
	x := n.Successor.Predecessor

	if between(x.Id, n.Id, n.Successor.Id) {
		n.Successor = x
	}
	n.Successor.notify(n)
}

func (n *Node) fixFingers() {
	n.next = n.next + 1
	if n.next > m {
		n.next = 1
	}

	two := big.NewInt(2)
	exponent := big.NewInt(int64(n.next - 1))
	offset := new(big.Int).Exp(two, exponent, nil) // 2^(next - 1)
	target := new(big.Int).Add(n.Id, offset)       // n + 2^(next - 1)

	n.FingerTable[n.next] = n.findSuccessor(target)

}

func (n *Node) checkPredecessor() {
	c, err := net.Dial("tcp", n.Predecessor.Address)
	if err != nil {
		n.Predecessor = nil
	}
	defer c.Close()
}

func (n *Node) closestPrecedingNode(id *big.Int) *Node {
	for i := m; i > 1; i-- {
		if between(n.FingerTable[i].Id, n.Id, id) {
			return n.FingerTable[i]
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
	found, nextNode = false, start
	 i = 0
	 for !found && i < 5 {
	 	found, nextNode = nextNode.findSuccessor(Id)
		i += 1
	 }
	if found{
		return nextNode
	} else {
		return Node{}
	}
}