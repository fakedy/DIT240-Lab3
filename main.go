package main

import (
	"flag" // https://pkg.go.dev/flag
)

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

}
