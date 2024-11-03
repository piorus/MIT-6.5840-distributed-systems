package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientState struct {
	PrevValue       string
	LastProcessedId int
}

type KVServer struct {
	mu     sync.Mutex
	values map[string]string
	states map[int]ClientState
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, _ := kv.values[args.Key]
	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	state, ok := kv.states[args.ClientId]

	// bail early, avoid duplicates
	if ok && state.LastProcessedId == args.Id {
		reply.Value = state.PrevValue
		return
	}

	prevValue, _ := kv.values[args.Key]
	reply.Value = prevValue

	kv.states[args.ClientId] = ClientState{PrevValue: prevValue, LastProcessedId: args.Id}
	kv.values[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	state, ok := kv.states[args.ClientId]

	// bail early, avoid duplicates
	if ok && state.LastProcessedId == args.Id {
		reply.Value = state.PrevValue
		return
	}

	prevValue, _ := kv.values[args.Key]
	reply.Value = prevValue

	kv.states[args.ClientId] = ClientState{PrevValue: prevValue, LastProcessedId: args.Id}
	kv.values[args.Key] = reply.Value + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.values = make(map[string]string)
	kv.states = make(map[int]ClientState)

	return kv
}
