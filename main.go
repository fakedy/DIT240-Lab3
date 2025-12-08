package main

import (
	"crypto/sha1"
	"flag" // https://pkg.go.dev/flag
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"path/filepath"
	"time"
)

var node *Node

func main() {
	// somehow take in arguments
	var (
		IP       string
		port     int
		joinIP   string
		joinPort int
		ts       int
		tff      int
		tcp      int
		r        int
	)

	flag.StringVar(&IP, "a", "127.0.0.1", "Chord IP Address")
	flag.IntVar(&port, "p", 8080, "Chord Port")
	flag.StringVar(&joinIP, "ja", "", "IP Address to join")
	flag.IntVar(&joinPort, "jp", 0, "Port to join")
	flag.IntVar(&ts, "ts", 3000, "Stabilization Time (ms)")
	flag.IntVar(&tff, "tff", 1000, "Fix fingers time (ms)")
	flag.IntVar(&tcp, "tcp", 3000, "Check predecessor time (ms)")
	flag.IntVar(&r, "r", 4, "number of successors maintained by chord client")

	flag.Parse()

	// create a new chord client
	if joinIP == "" || joinPort == 0 {
		node = server(IP, port)
		node.Create()
	} else {
		// join an existing chord
		node = server(IP, port)
		joinNode := Node{
			Address: joinIP,
			Port:    joinPort,
		}
		node.Join(&joinNode)
	}

	go StabilizeRoutine(ts)
	go FixFingersRoutine(tff)
	go CheckPredecessorRoutine(tcp)

}

func server(IP string, port int) *Node {
	node := Node{
		Address: IP,
		Port:    port,
	}

	rpc.Register(node)
	rpc.HandleHTTP()

	address := fmt.Sprintf("%s:%d", IP, port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("net.Listen failed: ", err)
	}
	go http.Serve(l, nil)
	return &node
}

func LookUp(fileName string) {
	hash := hashString(fileName)
	found := find(hash, node)

	if found != nil {
		fmt.Printf("Node ID: %d", node.Id)
		fmt.Printf("Node IP: %s", node.Address)
		fmt.Printf("Node Port: %d", node.Port)
	} else {
		fmt.Printf("Couldn't find file")
	}

}

func StoreFile(filePath string) {
	filename := filepath.Base(filePath)

}

func PrintState() {

}

func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func StabilizeRoutine(duration int) {

	for {
		time.Sleep(time.Millisecond * time.Duration(duration))
		node.stabilize()
	}
}

func FixFingersRoutine(duration int) {

	for {
		time.Sleep(time.Millisecond * time.Duration(duration))
		node.fixFingers()
	}

}

func CheckPredecessorRoutine(duration int) {

	for {
		time.Sleep(time.Millisecond * time.Duration(duration))
		node.checkPredecessor()
	}

}
