package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/tarcisiocjr/pepaxos/genericsmrproto"
	"github.com/tarcisiocjr/pepaxos/helper"
	"github.com/tarcisiocjr/pepaxos/masterproto"
	"github.com/tarcisiocjr/pepaxos/stats"
	stats1 "github.com/tarcisiocjr/stats"
)

// master flags
var portnum = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var numNodes = flag.Int("N", 3, "Number of replicas. Defaults to 3.")
var numClients = flag.Int("NC", 10, "Number of clients. Defaults to 10.")
var databaseFile = flag.String("database", "./results.db", "Location of SQLite database.")
var writeLatencyToLog *bool = flag.Bool("writelatencytolog", false, "Write latency of each request in a file. Defaults to false.")
var writeLatencyToDB *bool = flag.Bool("writelatencytodb", false, "Write latency of each request in the database. Defaults to false.")
var latencyLogPath = flag.String("latencylogpath", "/tmp/", "Location of latency logs files.")

// replicas flags
var doMencius *bool = flag.Bool("m", false, "Use Mencius as the replication protocol. Defaults to false.")
var doGpaxos *bool = flag.Bool("g", false, "Use Generalized Paxos as the replication protocol. Defaults to false.")
var doEpaxos *bool = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")

var exec = flag.Bool("exec", false, "Execute commands.")
var execParallel = flag.Bool("execParallel", false, "Execute commands in Parallel. Defaults to false (original Epaxos Execution).")
var execWaitFree = flag.Bool("execWaitFree", false, "Execute commands in wait free mode.")
var execThreads *int = flag.Int("execThreads", 1, "Number of Workers to execute commands. Defaults to 1 (sequential).")

var batchWait *int = flag.Int("batchwait", 0, "Milliseconds to wait before sending a batch. If set to 0, batching is disabled. Defaults to 0.")
var replyAfterExecute = flag.Bool("dreply", false, "Reply to client only after command has been executed.")
var listSize *int64 = flag.Int64("listSize", 0, "Size of the pre-populate list. Defaults to 0 (No populate).")
var rprocs *int = flag.Int("rp", 64, "Replica GOMAXPROCS. Defaults to 64")

// client flags
var cprocs *int = flag.Int("cp", 8, "Client GOMAXPROCS. Defaults to 8")

type Master struct {
	N          int
	NC         int
	experiment int64
	nodeList   []string
	addrList   []string
	portList   []int
	lock       *sync.Mutex
	nodes      []*rpc.Client
	leader     []bool
	alive      []bool
}

var latencyArr []int64
var nClientResponse int

func main() {
	flag.Parse()

	fkProtocol := helper.GetConsensusName(*doEpaxos, *doMencius, *doGpaxos)

	// fmt.Println(*databaseFile, fkProtocol, *thrifty, *beacon, *exec, *execThreads, *replyAfterExecute, *listSize, *rprocs, *cprocs)

	experimentID, err := stats.StoreExperimentInformation(*databaseFile, fkProtocol, *thrifty, *beacon, *exec, *execThreads, *replyAfterExecute, *listSize, *rprocs, *cprocs, *batchWait)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("New Experiment: %d\n", experimentID)
	log.Printf("...waiting for %d replicas\n", *numNodes)

	master := &Master{*numNodes,
		*numClients,
		experimentID,
		make([]string, 0, *numNodes),
		make([]string, 0, *numNodes),
		make([]int, 0, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes)}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	go master.run()

	go ShowStatus()

	logFile := *latencyLogPath + strconv.Itoa(int(experimentID))

	if *writeLatencyToLog {
		CreateFile(logFile)
	}

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if len(master.nodeList) == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d\n", i)
		}
		master.leader[i] = false
	}
	master.leader[0] = true

	for true {
		time.Sleep(3000 * 1000 * 1000)
		new_leader := false
		for i, node := range master.nodes {
			err := node.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
			if err != nil {
				//log.Printf("Replica %d has failed to reply\n", i)
				master.alive[i] = false
				if master.leader[i] {
					// neet to choose a new leader
					new_leader = true
					master.leader[i] = false
				}
			} else {
				master.alive[i] = true
			}
		}
		if !new_leader {
			continue
		}
		for i, new_master := range master.nodes {
			if master.alive[i] {
				err := new_master.Call("Replica.BeTheLeader", new(genericsmrproto.BeTheLeaderArgs), new(genericsmrproto.BeTheLeaderReply))
				if err == nil {
					master.leader[i] = true
					log.Printf("Replica %d is the new leader.", i)
					break
				}
			}
		}
	}
}

func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {

	master.lock.Lock()
	defer master.lock.Unlock()

	nlen := len(master.nodeList)
	index := nlen

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	fmt.Println("Replica connected: ", args.Addr)

	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}

	if index == nlen {
		master.nodeList = master.nodeList[0 : nlen+1]
		master.nodeList[nlen] = addrPort
		master.addrList = master.addrList[0 : nlen+1]
		master.addrList[nlen] = args.Addr
		master.portList = master.portList[0 : nlen+1]
		master.portList[nlen] = args.Port
		nlen++
	}

	if nlen == master.N {
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
		reply.Experiment = master.experiment
		reply.DoMencius = *doMencius
		reply.DoEpaxos = *doEpaxos
		reply.Thrifty = *thrifty
		reply.Beacon = *beacon
		reply.Exec = *exec
		reply.ExecThreads = *execThreads
		reply.BatchWait = *batchWait
		reply.ReplyAfterExecute = *replyAfterExecute
		reply.ListSize = *listSize
		reply.Procs = *rprocs
		reply.ExecParallel = *execParallel
		reply.ExecWaitFree = *execWaitFree
	} else {
		reply.Ready = false
	}

	return nil
}

// GetLeader returns the current Leader of the system
func (master *Master) GetLeader(args *masterproto.GetLeaderArgs, reply *masterproto.GetLeaderReply) error {
	time.Sleep(4 * 1000 * 1000)
	for i, l := range master.leader {
		if l {
			*reply = masterproto.GetLeaderReply{i}
			break
		}
	}
	return nil
}

// GetReplicaList returns to Replicas and Clients the list of connected Replicas
func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	if len(master.nodeList) == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
		reply.Experiment = master.experiment
		reply.ListSize = *listSize
	} else {
		reply.Ready = false
	}
	return nil
}

// StoreReplicaInformation store replica information
func (master *Master) StoreReplicaInformation(args *masterproto.StoreReplicaInformation, reply *masterproto.StoreReplicaInformationReply) error {
	// fmt.Println(args)
	master.lock.Lock()
	defer master.lock.Unlock()

	error := stats.StoreReplicaInformation(*databaseFile, args.Hostname, args.Experiment)
	if error != nil {
		fmt.Println(error)
	}

	return nil
}

var nClient int = 0

// StoreClientInformation receive all results from clients and store in database
func (master *Master) StoreClientInformation(args *masterproto.StoreClientInformation, reply *masterproto.StoreClientInformationReply) error {
	master.lock.Lock()
	// defer master.lock.Unlock()

	nClient++

	// write the results from each client in the database
	fkClient, err := stats.StoreClientInformation(args.Hostname, args.ClientID, args.Consensus, args.Gomaxprocs, args.Experiment, *databaseFile, args.Conflicts, args.Writes, args.TestDuration, args.Successful, args.Throughput, args.LatencyAvg)
	if err != nil {
		fmt.Println(err)
	}

	// since we are having problems saving the latency information into the
	// database, lets write to file
	if *writeLatencyToLog {
		logFile := (*latencyLogPath + strconv.Itoa(int(args.Experiment)))
		WriteFile(logFile, args.Latency)
	}

	// write the latency of each request in the database
	if *writeLatencyToDB {
		error := stats.StoreClientLatency(*databaseFile, fkClient, args.Latency, args.Experiment)
		if error != nil {
			fmt.Println(error)
		}
	}

	for _, l := range args.Latency {
		// fmt.Println(l)
		latencyArr = append(latencyArr, l)
	}

	// fmt.Println(latencyArr)
	if nClient == *numClients {
		data := stats1.LoadRawData(latencyArr)
		mean, _ := stats1.Mean(data)
		median, _ := stats1.Median(data)
		percentile5, _ := stats1.Percentile(data, 5)
		percentile25, _ := stats1.Percentile(data, 25)
		percentile50, _ := stats1.Percentile(data, 50)
		percentile75, _ := stats1.Percentile(data, 75)
		percentile90, _ := stats1.Percentile(data, 90)
		percentile95, _ := stats1.Percentile(data, 95)
		percentile99, _ := stats1.Percentile(data, 99)

		error := stats.UpdateExperimentLatency(*databaseFile, args.Experiment, mean, median, percentile5, percentile25, percentile50, percentile75, percentile90, percentile95, percentile99)
		if error != nil {
			fmt.Println(error)
		}

	}

	log.Println("Received results from:", fkClient)

	master.lock.Unlock()

	return nil
}

func ShowStatus() {
	// p := perf.Perf{}
	for {
		time.Sleep(20 * time.Second)
		fmt.Println("Master", time.Now())
		// fmt.Println(p)
	}

}

// CreateFile create the log fil
func CreateFile(logFile string) {
	// detect if file exists
	var _, err = os.Stat(logFile)

	// create the log file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(logFile)
		if err != nil {
			panic(err)
		}
		defer file.Close()
	}

	log.Println("Done creating latency file.")
}

// WriteFile writes the latency array into the experiment file
func WriteFile(logFile string, latency []int64) {
	// open
	var file, err = os.OpenFile(logFile, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// write the latency array to the file
	for _, l := range latency {
		_, err = file.WriteString(strconv.Itoa(int(l)) + "\n")
		if err != nil {
			panic(err)
		}
	}

	// sync
	err = file.Sync()
	if err != nil {
		panic(err)
	}

	log.Println("Done saving to file.")
}

// to_ms convert Unixnano to Milliseconds
func to_ms(nano int64) int64 {
	return nano / 1e6
}
