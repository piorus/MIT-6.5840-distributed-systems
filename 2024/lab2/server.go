package kvsrv

import (
	"log"
	"strings"
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
	value    *string
	op       string
}

func (transaction *ClientTransaction) Commit(value *string) string {
	if transaction.op == "put" {
		return *transaction.value
	}

	if transaction.op == "append" {
		return *value + *transaction.value
	}

	return ""
}

type KVServer struct {
	mu                 sync.Mutex
	values             map[string]string
	transactionHistory map[int]int
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
		value:    &args.Value,
		id:       args.Id,
		clientId: args.ClientId,
	})
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Value = PutAppend(kv, &ClientTransaction{
		op:       "append",
		key:      args.Key,
		value:    &args.Value,
		id:       args.Id,
		clientId: args.ClientId,
	})
}

func PutAppend(kv *KVServer, transaction *ClientTransaction) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	prevTransactionId, _ := kv.transactionHistory[transaction.clientId]
	value, _ := kv.values[transaction.key]

	if prevTransactionId == transaction.id {
		return strings.Replace(value, *transaction.value, "", 1)
	}

	kv.transactionHistory[transaction.clientId] = transaction.id
	kv.values[transaction.key] = transaction.Commit(&value)

	return value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.values = make(map[string]string)
	kv.transactionHistory = make(map[int]int)

	return kv
}
