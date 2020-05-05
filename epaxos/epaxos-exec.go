package epaxos

import (
	"sort"
	"time"

	"github.com/tarcisiocjr/pepaxos/epaxosproto"
	"github.com/tarcisiocjr/pepaxos/genericsmrproto"
)

func (e *Exec) prepAndExecute(list []*Instance) {
	// execute commands in the increasing order of the Seq field
	sort.Sort(nodeArray(list))

	for _, w := range list {
		// keeped from original implementation
		for w.Cmds == nil {
			time.Sleep(1000 * 1000)
		}

		// set the status of the instance to executing before the execution
		// to prevent an go routine execute a instance with dependencies
		// unexecuted
		w.Status = epaxosproto.EXECUTING
	}

	if e.r.execParallel {
		// copy the just arrived strong connected componnent to a new slice since
		// now we are using Go routines to paralelize the work
		newList := make([]*Instance, len(list))
		copy(newList, list)
		go e.executeInstance(newList)
	} else {
		e.executeInstance(list)
	}
}

func (e *Exec) executeInstance(list []*Instance) {
	if e.r.execParallel {
		// limit the concurrency with a buffered channel. That is, execThreads = 1
		// results in a sequencial instance (command) execution
		e.r.execThreads <- struct{}{}
	}

	for _, w := range list {

		// fmt.Println(len(w.Cmds), len(e.r.execThreads))

		for idx := 0; idx < len(w.Cmds); idx++ {
			// fmt.Printf("Operation: %d, Key: %d, Value: %d\n", w.Cmds[idx].Op, w.Cmds[idx].K, w.Cmds[idx].V)

			// fmt.Println(w.Deps)
			// for _, d := range w.Deps {
			// 	if d >= 0 {
			// 		fmt.Println(d)
			// 	}
			// }

			// execute the command in the key-value store
			val := w.Cmds[idx].Execute(e.r.State)

			if e.r.Dreply && w.lb != nil && w.lb.clientProposals != nil {
				e.r.ReplyProposeTS(
					&genericsmrproto.ProposeReplyTS{
						OK:        TRUE,
						CommandId: w.lb.clientProposals[idx].CommandId,
						Value:     val,
						// Value: w.lb.clientProposals[idx].Command.V,
						Timestamp: w.lb.clientProposals[idx].Timestamp},
					w.lb.clientProposals[idx].Reply,
					w.lb.clientProposals[idx].Mutex)
			}
		}
		w.Status = epaxosproto.EXECUTED
	}

	if e.r.execParallel {
		<-e.r.execThreads
	}
}
