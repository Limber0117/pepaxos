package masterproto

import (
	"time"
)

type RegisterArgs struct {
	Addr string
	Port int
}

type RegisterReply struct {
	ReplicaId         int
	NodeList          []string
	Ready             bool
	Experiment        int64
	DoMencius         bool
	DoEpaxos          bool
	Thrifty           bool
	Beacon            bool
	Exec              bool
	ExecThreads       int
	BatchWait         int
	ReplyAfterExecute bool
	ListSize          int64
	Procs             int
	ExecParallel      bool
	ExecWaitFree      bool
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
	LeaderId int
}

type GetReplicaListArgs struct {
}

type GetReplicaListReply struct {
	ReplicaList []string
	Ready       bool
	Experiment  int64
	ListSize    int64
}

type StoreClientInformationReply struct {
}

// StoreClientInformation represents an StoreClientInformation function
// Used by clients to store the results in database
type StoreClientInformation struct {
	Hostname     string
	ClientID     int
	Consensus    int
	Gomaxprocs   int
	Experiment   int64
	Conflicts    int
	Writes       int
	Latency      []int64
	TestDuration time.Duration
	Successful   int64
	Throughput   int64
	LatencyAvg   float64
}

type StoreReplicaInformationReply struct {
}

// StoreReplicaInformation represents an StoreRepicaInformation function
// Used by replicas to store their parametters in database
type StoreReplicaInformation struct {
	Hostname   string
	Experiment int64
}
