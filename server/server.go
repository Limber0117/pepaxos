package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/tarcisiocjr/pepaxos/epaxos"
	"github.com/tarcisiocjr/pepaxos/masterproto"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "localhost", "Server address (this machine). Defaults to localhost.")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")

func main() {
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("hostname error", err)
	}

	reply := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort), hostname)

	// go showStatus(reply)

	runtime.GOMAXPROCS(reply.Procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	log.Println("Starting Egalitarian Paxos replica...")
	rep := epaxos.NewReplica(reply.ReplicaId, reply.NodeList, reply.Thrifty, reply.Exec, reply.ReplyAfterExecute, reply.Beacon, *durable, reply.BatchWait, reply.ExecThreads, reply.ListSize, reply.ExecParallel)
	rpc.Register(rep)

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

// func registerWithMaster(masterAddr string) (int, []string, int) {
func registerWithMaster(masterAddr string, hostname string) *masterproto.RegisterReply {
	args := &masterproto.RegisterArgs{
		Addr: *myAddr,
		Port: *portnum}
	var reply *masterproto.RegisterReply

	for done := false; !done; {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true

				err = mcli.Call("Master.StoreReplicaInformation", masterproto.StoreReplicaInformation{
					Hostname:   hostname,
					Experiment: reply.Experiment,
				}, nil)
				if err != nil {
					log.Fatalf("Error updating replica information")
				}
				break
			}
		}
		time.Sleep(1e9)
	}

	return reply
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}

func showStatus(reply *masterproto.RegisterReply) {
	for {
		fmt.Println("Replica ID:", reply.ReplicaId, "// Experiment:", reply.Experiment, "// Egalitarian Paxos:", reply.DoEpaxos, "// Thrifty:", reply.Thrifty, "// Beacon:", reply.Beacon, "// Execute:", reply.Exec, "// Threads:", reply.ExecThreads, "// Batch Wait:", reply.BatchWait, "// Reply after execute:", reply.ReplyAfterExecute, "// Size of list:", reply.ListSize, "// GOMAXPROCS:", reply.Procs)
		time.Sleep(10 * time.Second)
	}
}
