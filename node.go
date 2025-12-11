package main

import (
	"fmt"
	"math/big"
	"net"
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

	bucket map[*big.Int]string

	next int
}

func (n *Node) findSuccessor(id *big.Int) (bool, *Node) {
	if between(id, n.Id, n.Successor.Id) {
		return true, n.Successor
	} else {
		nprim := n.closestPrecedingNode(id)
		return false, nprim
	}
}

func (n *Node) Create() {
	n.Id = big.NewInt(0) // temp to prevent crash, we need to actually assign the real id
	n.Predecessor = nil
	n.Successor = n
	n.next = 0
	n.FingerTable = make([]*Node, m)

}

func (n *Node) Join(nprim *Node) {
	n.Predecessor = nil
	_, n.Successor = nprim.findSuccessor(n.Id)
}

func (n *Node) stabilize() {
	x := n.Successor.Predecessor

	if x != nil && between(x.Id, n.Id, n.Successor.Id) { // if x.id is between n.Id and n.successor.ID
		n.Successor = x // update our succesor to the one that was between
	}
	n.Successor.notify(n)
}

func (n *Node) fixFingers() {

	two := big.NewInt(2)
	exponent := big.NewInt(int64(n.next))
	offset := new(big.Int).Exp(two, exponent, nil) // 2^(next - 1)
	target := new(big.Int).Add(n.Id, offset)       // n + 2^(next - 1)

	_, nextsuccessor := n.findSuccessor(target)

	n.FingerTable[n.next] = nextsuccessor

	n.next = n.next + 1
	if n.next >= m {
		n.next = 0
	}

}

func (n *Node) checkPredecessor() {
	if n.Predecessor != nil {
		c, err := net.DialTimeout("tcp", n.Predecessor.Address, 5*time.Second)
		if err != nil {
			n.Predecessor = nil
			return
		}
		c.Close()
	}
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
	found, nextNode := false, start
	i := 0
	for !found && i < 5 {
		found, nextNode = nextNode.findSuccessor(Id)
		i += 1
	}
	if found {
		return nextNode
	} else {
		fmt.Println("could not find node!!!")
		return nil
	}
}
