package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task, err := CallGetTask()

		if err != nil {
			fmt.Println("no more tasks")
			break
		}

		ProcessTask(task, mapf, reducef)

		CallNotifyAboutTaskCompletion(task)
	}
}

func CallGetTask() (Task, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	var err error

	ok := call("Coordinator.GetTask", &args, &reply)

	if !ok {
		err = errors.New("no more tasks")
	}

	return reply.Task, err
}

func CallNotifyAboutTaskCompletion(task Task) {
	args := NotifyAboutTaskCompletionArgs{Task: task}
	reply := NotifyAboutTaskCompletionReply{}
	ok := call("Coordinator.NotifyAboutTaskCompletion", &args, &reply)

	if !ok {
		fmt.Println("Something went wrong during NotifyAboutTaskCompletion")
	}
}

func ProcessTask(task Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for _, filename := range task.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
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
