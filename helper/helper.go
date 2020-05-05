package helper

// GetConsensusName return the consensus name of protocol
func GetConsensusName(doEpaxos bool, doMencius bool, doGpaxos bool) int {
	if doEpaxos {
		return 1
	} else if doMencius {
		return 2
	} else if doGpaxos {
		return 3
	} else {
		return 4
	}
}

// GetExecuteMode returns the execution mode
func GetExecuteMode(exec bool, execParallel bool) string {
	if !exec {
		return "noExecute"
	}

	if execParallel {
		return "parallel"
	}
	return "sequential"
}
