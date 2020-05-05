package state

import (
	"container/list"
	"log"
	"sync"
	"time"
)

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	DELETE
	RLOCK
	WLOCK
	CONTAINS
)

const NIL Value = 0
const OK Value = 1

type Key int64
type Value int64

type Command struct {
	Op Operation
	K  Key
	V  Value
}

// State holds the elements, where each element points to the next element
type State struct {
	ll           *list.List
	mutex        *sync.RWMutex
	middleOfList int64
}

// InitState instantiates a new state and seed
func InitState(listSize int64) *State {
	state := &State{list.New(), new(sync.RWMutex), int64(listSize / 2)}
	// seed the list
	for i := 0; int64(i) < listSize; i++ {
		state.ll.PushBack(i)
	}
	// dispach a goroutine to show status
	// go state.ShowStatus()
	return state
}

// Add appends a value (one or more) at the end of the list
func (state *State) Add(value Key) error {
	state.mutex.Lock()
	state.ll.PushBack(value)
	state.mutex.Unlock()
	return nil
}

// Replace a element on the list
func (state *State) Replace(value Key) (Value, bool) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	found := false
	i := 0
	for element := state.ll.Front(); element != nil; element = element.Next() {
		if int64(i) == state.middleOfList {
			element.Value = value
			found = true
			break
		}
		i++
	}
	if !found {
		return NIL, false
	}
	return Value(value), true
}

// Contains checks if value are present in the set.
func (state *State) Contains(value Key) (Value, bool) {
	state.mutex.RLock()
	defer state.mutex.RUnlock()

	found := false
	var result Value = 0
	for element := state.ll.Front(); element != nil; element = element.Next() {
		// fmt.Println(element, state.size, element.value)
		if element.Value == Value(value) {
			found = true
			result = element.Value.(Value)
			break
		}
	}
	if !found {
		return NIL, false
	}
	return result, true
}

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

// IsRead returns true if a operation is read-only
func IsRead(command *Command) bool {
	return command.Op == GET
}

// Execute is the function that execute the operation in the key value or list
func (c *Command) Execute(st *State) Value {
	switch c.Op {

	case PUT:
		value, present := st.Replace(c.K)
		if present {
			return value
		}

	// case PUT:
	// 	// fmt.Printf("Executing PUT (%d, %d)\n", c.K, c.V)
	// 	error := st.Add(c.K)
	// 	if error != nil {
	// 		return OK
	// 	}
	// 	return NIL
	case GET:
		// value, present := st.Get(c.K)
		value, present := st.Contains(c.K)
		// fmt.Printf("Executing GET (%d, %d)\n", c.K, c.V)
		if present {
			// fmt.Println(present)
			return value
		}
	}
	return NIL
}

// ShowStatus dumps on-screem some statistics
func (state *State) ShowStatus() {
	for {
		time.Sleep(1 * time.Second)
		// state.Display()
		log.Println("Replica:", time.Now(), "// List size:", state.ll.Len(), "// First element of list:", state.ll.Front().Value, "// Last element of list:", state.ll.Back().Value, "// Element in the middle of list", state.middleOfList)
	}
}
