package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/tarcisiocjr/pepaxos/clientproto"
	"github.com/tarcisiocjr/pepaxos/dlog"
	"github.com/tarcisiocjr/pepaxos/genericsmrproto"
	"github.com/tarcisiocjr/pepaxos/helper"
	"github.com/tarcisiocjr/pepaxos/masterproto"
	"github.com/tarcisiocjr/pepaxos/state"
	stats1 "github.com/tarcisiocjr/stats"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var reqsNb *int = flag.Int("q", 10000000, "Total number of requests. Defaults to 10000000.")
var noLeader *bool = flag.Bool("e", true, "Egalitarian (no leader). Defaults to true.")
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", -1, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")
var barOne = flag.Bool("barOne", false, "Sent commands to all replicas except the last one.")
var waitLess = flag.Bool("waitLess", false, "Wait for only N - 1 replicas to finish.")
var cluster = flag.Bool("cluster", false, "Used for workload generate (local development only)")
var timeout = flag.Int64("timeout", 10, "Timeout.")
var ongoing = flag.Int("ongoing", 200, "Ongoing requests without reply.")
var logs *string = flag.String("logs", "/tmp/", "Logs")

var N int
var succ int
var latency []clientproto.Latency
var succLock = new(sync.Mutex)
var rarray []int
var rsp []bool

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*procs)
	hostname, err := os.Hostname()
	consensusID := helper.GetConsensusName(*noLeader, false, false)

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	N = len(rlReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	rarray = make([]int, *reqsNb / *rounds + *eps)
	karray := make([]int64, *reqsNb / *rounds + *eps)
	put := make([]bool, *reqsNb)
	perReplicaCount := make([]int, N)
	M := N
	if *barOne {
		M = N - 1
	}

	// It is time to generate our workload.
	// The thing is, we want a unique workload when there is no conflict,
	// for that we are using a prefix for each node and for each thread
	hostnamePrefix := 1

	// Check if we are a member of the the cluster
	if *cluster {
		re := regexp.MustCompile("[0-9]+")
		hostnamePrefix, _ = strconv.Atoi(re.FindString(hostname))
		// fmt.Println("---------->", hostnamePrefix)
	}

	prefix := fmt.Sprintf("%02d", hostnamePrefix+10)
	prefixForSeed, _ := strconv.Atoi(prefix)
	rand.Seed(42 + int64(prefixForSeed))

	for i := 0; i < len(rarray); i++ {
		r := rand.Intn(M)

		rarray[i] = r
		if i < *reqsNb / *rounds {
			perReplicaCount[r]++
		}

		r = rand.Intn(100)
		if r < *conflicts {
			karray[i] = 42
			put[i] = true
		} else {
			// karray[i] = int64(43 + i)
			randomKey := fmt.Sprintf("%015d", rand.Int31())
			randomPrefixed := prefix + randomKey
			karray[i], _ = strconv.ParseInt(randomPrefixed, 10, 64)
			// fmt.Println(karray[i], randomKey, randomPrefixed)
			put[i] = false
		}

	}

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
			N = N - 1
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
	}

	leader := 0

	if *noLeader == false {
		reply := new(masterproto.GetLeaderReply)
		if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
			log.Fatalf("Error making the GetLeader RPC\n")
		}
		leader = reply.LeaderId
		//log.Printf("The leader is replica %d\n", leader)
	}

	var id int32 = 0

	guard := make(chan struct{}, *ongoing)

	args := genericsmrproto.Propose{id, state.Command{state.GET, 0, 0}, 0} //make([]int64, state.VALUE_SIZE)}}

	// context package is used to control the elapsed time
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Second)
	defer cancel()

	before_total := time.Now()

	for j := 0; j < *rounds; j++ {
		n := *reqsNb / *rounds
		if *check {
			rsp = make([]bool, n)
			for j := 0; j < n; j++ {
				rsp[j] = false
			}
		}

		if *noLeader {
			for i := 0; i < N; i++ {
				go waitReplies(readers, i, perReplicaCount[i], guard)
			}
		} else {
			go waitReplies(readers, leader, n, guard)
		}

		before := time.Now()

	loop:
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				dlog.Println("Received the signal to stop.")
				break loop
			default:
				// for i := 0; i < n+*eps; i++ {
				dlog.Printf("Sending proposal %d\n", id)
				if *noLeader {
					leader = rarray[i]
					if leader >= N {
						continue
					}
				}
				args.CommandId = id
				args.Command.K = state.Key(karray[i])
				if put[i] {
					args.Command.Op = state.PUT
					args.Command.V = state.Value(time.Now().UnixNano())
				} else {
					args.Command.Op = state.GET
					args.Command.V = state.Value(0)
				}
				args.Timestamp = time.Now().UnixNano()
				writers[leader].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(writers[leader])
				writers[leader].Flush()
				// fmt.Println("Sent", id, args.Command.Op)
				// fmt.Println("Sent", id, args.Timestamp)
				guard <- struct{}{}

				id++
				if i%100 == 0 {
					for i := 0; i < N; i++ {
						writers[i].Flush()
					}
				}
			}
			for i := 0; i < N; i++ {
				writers[i].Flush()
			}

		}
		// err := false
		// if *noLeader {
		// 	W := N
		// 	if *waitLess {
		// 		W = N - 1
		// 	}
		// 	for i := 0; i < W; i++ {
		// 		e := <-done
		// 		err = e || err
		// 	}
		// } else {
		// 	err = <-done
		// }

		after := time.Now()

		fmt.Printf("Round took %v\n", after.Sub(before))

		if *check {
			for j := 0; j < n; j++ {
				if !rsp[j] {
					fmt.Println("Didn't receive", j)
				}
			}
		}
	}

	after_total := time.Now()
	fmt.Printf("Test took %v\n", after_total.Sub(before_total))
	fmt.Printf("%v\n", (after_total.Sub(before_total)).Seconds())

	// time.Sleep(10)

	// Let's send to Master the results of the experiment
	// All the information will be storaged in the database
	args1 := new(masterproto.StoreClientInformation)
	args1.TestDuration = after_total.Sub(before_total)
	args1.Successful = int64(succ)
	args1.Throughput = int64(float64(args1.Successful) / args1.TestDuration.Seconds())
	args1.Hostname = hostname
	args1.ClientID = *ongoing
	args1.Consensus = consensusID
	// args1.Writes = *writes
	args1.Gomaxprocs = *procs
	args1.Experiment = rlReply.Experiment
	args1.Conflicts = *conflicts

	// Time to manage all the latency stuff
	// all info we need is stored in latency []clientproto.Latency
	//
	// type Latency struct {
	// 	CommandID int32
	// 	Latency   int64
	// 	Timestamp time.Time
	// }
	//

	// since we have more than one gorouting adding reply to our struct,
	// lets start by sorting our latency struct by the timestamp
	sort.Slice(latency[:], func(i, j int) bool {
		return latency[i].Timestamp.Before(latency[j].Timestamp)
		// return latency[i].Timestamp < latency[j].Timestamp
	})

	// then ignore the start and the end of execution (warm up phase)
	firstLatency := latency[0].Timestamp.Add(time.Second * 5)
	lastLatency := latency[len(latency)-1].Timestamp.Add(time.Second * -5)

	// and then add all latency information between this two timestamps
	latencyArr := []int64{}
	for _, l := range latency {
		if l.Timestamp.After(firstLatency) && l.Timestamp.Before(lastLatency) {
			latencyArr = append(latencyArr, l.Latency)
		}
	}

	latencyData := stats1.LoadRawData(latencyArr)
	latencyMean, _ := stats1.Median(latencyData)

	args1.LatencyAvg = latencyMean
	args1.Latency = latencyArr

	// it is time to dial to master and send all information
	master1, err1 := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err1 != nil {
		log.Fatalf("Error connecting to master\n")
	}

	err2 := master1.Call("Master.StoreClientInformation", args1, nil)
	if err2 != nil {
		fmt.Println(err2)
		log.Fatalf("Error updating client information")
	}

	fmt.Printf("Successful: %d\n", succ)
	fmt.Printf("%v\n", float64(succ)/(after_total.Sub(before_total)).Seconds())

	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	master.Close()
	master1.Close()
}

func waitReplies(readers []*bufio.Reader, leader int, n int, guard chan struct{}) {

	reply := new(genericsmrproto.ProposeReplyTS)
	for i := 0; ; i++ {
		// if *noLeader {
		// 	leader = rarray[i]
		// }
		if err := reply.Unmarshal(readers[leader]); err != nil {
			fmt.Println("Error when reading:", err)
			break
		}
		if *check {
			if rsp[reply.CommandId] {
				fmt.Println("Duplicate reply", reply.CommandId)
			}
			rsp[reply.CommandId] = true
		}
		// fmt.Println(reply)
		if reply.OK != 0 {
			succLock.Lock()
			succ++
			replyLatency := time.Now().UnixNano() - reply.Timestamp

			latency = append(latency, clientproto.Latency{
				CommandID: reply.CommandId,
				Latency:   replyLatency,
				Timestamp: time.Unix(0, reply.Timestamp),
			})

			succLock.Unlock()
		}
		<-guard
	}
}

// to_ms convert Unixnano to Milliseconds
func to_ms(nano int64) int64 {
	return nano / 1e6
}
