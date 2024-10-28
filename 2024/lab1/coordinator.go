package mr

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	tasks []ITask
	mutex sync.Mutex
}

type TaskType string

const Map, Reduce, Idle TaskType = "map", "reduce", "idle"

type ITask interface {
	Schedule()
	Reschedule()
	Complete()
	GetId() int
	Is(taskType TaskType) bool
	Equals(task ITask) bool
	IsScheduled() bool
	IsCompleted() bool
}

type Task struct {
	Id        int
	Type      TaskType
	Scheduled bool
	Completed bool
}

type MapTask struct {
	Task
	Filename string
	NReduce  int
}

type IdleTask struct {
	Task
}

type ReduceTask struct {
	Task
}

func (t *Task) Schedule() {
	t.Scheduled = true
}

func (t *Task) Reschedule() {
	t.Scheduled = false
}

func (t *Task) Complete() {
	t.Completed = true
}

func (t *Task) GetId() int {
	return t.Id
}

func (t *Task) Is(taskType TaskType) bool {
	return t.Type == taskType
}

func (t *Task) Equals(task ITask) bool {
	return task.Is(t.Type) && task.GetId() == t.Id
}

func (t *Task) IsScheduled() bool {
	return t.Scheduled
}

func (t *Task) IsCompleted() bool {
	return t.Completed
}

func IsMappingComplete(tasks []ITask) bool {
	for _, task := range tasks {
		if task.Is(Map) && !task.IsCompleted() {
			return false
		}
	}

	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, task := range c.tasks {
		if task.Is(Map) && !task.IsCompleted() && !task.IsScheduled() {
			c.tasks[i].Schedule()
			reply.Task = task

			go func() {
				time.Sleep(10 * time.Second)

				if !task.IsCompleted() {
					c.tasks[i].Reschedule()
				}
			}()

			return nil
		}

		if task.Is(Reduce) && !task.IsCompleted() && !task.IsScheduled() && !IsMappingComplete(c.tasks) {
			// wait until mapping ends
			reply.Task = &IdleTask{Task: Task{Type: Idle}}

			return nil
		} else if task.Is(Reduce) && !task.IsCompleted() && !task.IsScheduled() {
			reply.Task = task

			return nil
		}
	}

	return errors.New("no more tasks")
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	// we do not need to register end of idle tasks
	if args.Task.Is(Idle) {
		reply.Ack = true
		return nil
	}

	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].Equals(args.Task) {
			c.tasks[i].Complete()
			//fmt.Printf("Task completed: %+v\n", c.tasks[i])
			reply.Ack = true

			return nil
		}
	}

	return fmt.Errorf("task not found: %+v", args.Task)
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, task := range c.tasks {
		if !task.IsCompleted() {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	gob.Register(&IdleTask{})

	if nReduce < 1 {
		panic("nReduce must be greater than 0")
	}

	var tasks []ITask

	for i, filename := range files {
		tasks = append(tasks, &MapTask{Task: Task{Id: i, Type: Map}, Filename: filename, NReduce: nReduce})
	}

	for i := 0; i < nReduce; i++ {
		tasks = append(tasks, &ReduceTask{Task: Task{Id: i, Type: Reduce}})
	}

	c := Coordinator{tasks: tasks}
	c.server()
	return &c
}
