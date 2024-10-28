package mr

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	gob.Register(IdleTask{})

	for {
		if task, err := RpcGetTask(); err == nil {
			if task.Is(Map) {
				if v, ok := task.(*MapTask); ok {
					HandleMap(v, mapf)
				}
			}

			if task.Is(Reduce) {
				if v, ok := task.(*ReduceTask); ok {
					HandleReduce(v, reducef)
				}
			}

			if task.Is(Idle) {
				time.Sleep(time.Second)
			}

			RpcCompleteTask(task)
		} else {
			break
		}
	}
}

func RpcGetTask() (ITask, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	if !call("Coordinator.GetTask", &args, &reply) {
		return reply.Task, errors.New("no more tasks")
	}

	return reply.Task, nil
}

func RpcCompleteTask(task ITask) {
	args := CompleteTaskArgs{Task: task}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)

	if !ok {
		fmt.Println("Something went wrong during ColeteTask")
	}
}

func HandleMap(task *MapTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	kvaMap := make(map[int][]KeyValue)

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % task.NReduce
		kvaMap[reduceId] = append(kvaMap[reduceId], kv)
	}

	for reduceId, kva := range kvaMap {
		out, _ := os.Create(fmt.Sprintf("mr-%d-%d", task.Id, reduceId))
		enc := json.NewEncoder(out)

		sort.Sort(ByKey(kva))

		for _, kv := range kva {
			enc.Encode(&kv)
		}
	}
}

func HandleReduce(task *ReduceTask, reducef func(string, []string) string) {
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
