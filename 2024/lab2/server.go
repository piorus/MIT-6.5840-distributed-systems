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

type ClientTransaction struct {
	id       int
	clientId int
	key      string
	value    string
	op       string
}

func (transaction *ClientTransaction) Commit(value *string) {
	if transaction.op == "put" {
		*value = transaction.value
		return
	}

	if transaction.op == "append" {
		*value = *value + transaction.value
		return
	}
}

type KVServer struct {
	mu           sync.Mutex
	values       map[string]string
	transactions map[int]ClientTransaction
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, _ := kv.values[args.Key]

	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Value = PutAppend(kv, &ClientTransaction{
		op:       "put",
		key:      args.Key,
		value:    args.Value,
		clientId: args.ClientId,
	})
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Value = PutAppend(kv, &ClientTransaction{
		op:       "append",
		key:      args.Key,
		value:    args.Value,
		clientId: args.ClientId,
	})
}

func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	transaction, ok := kv.transactions[args.ClientId]
	if ok {
		value, _ := kv.values[transaction.key]
		transaction.Commit(&value)
		kv.values[transaction.key] = value
		kv.transactions[args.ClientId] = ClientTransaction{}
	}
}

func PutAppend(kv *KVServer, transaction *ClientTransaction) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, _ := kv.values[transaction.key]
	kv.transactions[transaction.clientId] = *transaction

	return value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.values = make(map[string]string)
	kv.transactions = make(map[int]ClientTransaction)

	return kv
}
