package main

import (
	"bufio"
	"crypto/sha1"
	"flag" // https://pkg.go.dev/flag
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var node *Node

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
	i		 string
)

func main() {


	flag.StringVar(&IP, "a", "127.0.0.1", "Chord IP Address")
	flag.IntVar(&port, "p", 8080, "Chord Port")
	flag.StringVar(&joinIP, "ja", "", "IP Address to join")
	flag.IntVar(&joinPort, "jp", 0, "Port to join")
	flag.IntVar(&ts, "ts", 3000, "Stabilization Time (ms)")
	flag.IntVar(&tff, "tff", 1000, "Fix fingers time (ms)")
	flag.IntVar(&tcp, "tcp", 3000, "Check predecessor time (ms)")
	flag.IntVar(&r, "r", 4, "number of successors maintained by chord client")
	flag.StringVar(&i, "i", "", "optional hash")

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

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {

		text := scanner.Text()
		args := strings.Fields(text)

		switch args[0] {
		case "lookup":
			if len(args) < 2 {
				fmt.Println("Usage: lookup <filename>")
			} else {
				LookUp(args[1])
			}
		case "storefile":
			if len(args) < 2 {
				fmt.Println("Usage: storefile <filename>")
			} else {
				StoreFile(args[1])
			}

		case "printstate":
			PrintState()
		case "help":
			fmt.Println("Commands:")
			fmt.Println("lookup <filename>")
			fmt.Println("storefile <filename>")
			fmt.Println("printstate")
			fmt.Println("help")
			fmt.Println("exit")

		case "exit":
			os.Exit(0)
		default:
			fmt.Println("Unknown command.")
		}

		fmt.Print("> ")
	}

}

func server(IP string, port int) *Node {
	ipPortHash := hashString(fmt.Sprintf("%s:%d", IP, port))
	if(i != ""){
		ipPortHash = hashString(i)
	}
	node := Node{
		Address: IP,
		Port:    port,
		Id:      ipPortHash,
		bucket:  make(map[string][]byte),
		Successor: nil,
		Predecessor: nil,
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
		fmt.Printf("Node ID: %d\n", node.Id)
		fmt.Printf("Node IP: %s\n", node.Address)
		fmt.Printf("Node Port: %d\n", node.Port)
	} else {
		fmt.Printf("Couldn't find file\n")
	}

}

func StoreFile(filePath string) {
	// Read file content
	data, err := os.ReadFile(filePath)

	// hash the filename to get the key
	filename := filepath.Base(filePath)
	key := hashString(filename)

	// Find the responsible node for the key
	_, successor := node.findSuccessor(key)
	if err != nil {
		fmt.Printf("Cannot find successor\n")
	}

	// if the successor is self
	if successor.Address == node.Address && successor.Port == node.Port {
		// storage method
		node.Store(key, data)
	} else {

		// Prepare RPC args and reply
		args := StoreArg{
			Key:         key,
			FileContent: data,
		}
		var reply StoreReply

		// RPC calls
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", successor.Address, successor.Port))
		if err != nil {
			fmt.Printf("Dial failed\n")
		}

		err = client.Call("node.Put", &args, &reply)
		if err != nil {
			fmt.Printf("RPC call failed\n")
		} else if !reply.Success {
			fmt.Printf("Store failed\n")
		}

	}

}

func PrintState() {
	// Own node information
	fmt.Println("---Local Node---")
	fmt.Println("ID: 	%s\n", node.Id)
	fmt.Println("Addr:  %s:%d", node.Address, node.Port)

	// Successors list information
	fmt.Println("\n---Successor List---")
	if node.Successor != nil{
		fmt.Printf("[0] ID: %s | %s:%d\n", node.Successor.Id, node.Successor.Address, node.Successor.Port)
	} else {
		fmt.Println("[0] nil")
	}

	// Finger table information
	fmt.Println("\n---Finger Table---")
	for i, finger := range node.FingerTable{
		if finger != nil{
			fmt.Printf("[%d] ID: %s | %s:%d", i, finger.Id, finger.Address, finger.Port)
		} else {
			fmt.Printf("[%d] nil", i)
		}
	}

	// Stored files
	fmt.Println("\n---Stored Files---")
	if len(node.bucket) == 0{
		fmt.Println("Empty")
	} else {
		for key, content := range node.bucket {
			fmt.Printf("Key: %s | Size: %d bytes\n", key, len(content))
		}
	}
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

func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
