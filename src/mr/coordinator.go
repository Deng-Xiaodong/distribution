package mr

import (
	"container/heap"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAP      = "MAP"
	REDUCE   = "REDUCE"
	FINISHED = "FINISHED"
)

type Task struct {
	WorkerID   int
	Index      int
	Type       string
	HandleFile string
	MNum, RNum int
	Expired    int64
}
type TaskExpired struct {
	taskId      string
	taskExpired int64
}
type TaskHeap []TaskExpired

func (t TaskHeap) Len() int {
	return len(t)
}

func (t TaskHeap) Less(i, j int) bool {
	return t[i].taskExpired < t[j].taskExpired
}

func (t *TaskHeap) Swap(i, j int) {
	(*t)[i], (*t)[j] = (*t)[j], (*t)[i]
}

func (t *TaskHeap) Push(x any) {
	*t = append(*t, x.(TaskExpired))
}

func (t *TaskHeap) Pop() any {
	x := (*t)[t.Len()-1]
	*t = (*t)[:t.Len()-1]
	return x
}

type Coordinator struct {
	// Your definitions here.
	//元数据（map任务数、reduce worker 数）
	//待处理任务队列（channel）
	//正在执行的任务队列（字典）
	//所处阶段(MAP REDUCE FINISHED)
	mu, hmu    sync.Mutex
	mNum, rNum int
	tasksChan  chan Task
	doingTasks map[string]Task
	state      string
	taskHeap   *TaskHeap
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
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
	ret := false

	// Your code here.
	ret = c.state == FINISHED

	return ret
}

func (c *Coordinator) transit() {
	if c.state == MAP {
		c.mu.Lock()
		log.Printf("coordinator 获得mu锁\n")
		log.Printf("All MAP tasks finished. Transit to REDUCE stage\n")
		//转入到reduce阶段
		c.state = REDUCE
		for i := 0; i < c.rNum; i++ {
			task := Task{Type: REDUCE, Index: i, MNum: c.mNum, RNum: c.rNum}
			c.doingTasks[GenTaskID(task.Index, task.Type)] = task
			c.tasksChan <- task
		}
		c.mu.Unlock()
		log.Printf("coordinator 释放mu锁\n")
	} else if c.state == REDUCE {
		log.Printf("All REDUCE tasks finished. Prepare to exit\n")
		c.state = FINISHED
		close(c.tasksChan)
	}
}

func (c *Coordinator) checkPoint() {
	for true {
		time.Sleep(time.Second)
		c.hmu.Lock()
		log.Printf("coordinator 获得hmu锁\n")
		c.mu.Lock()
		log.Printf("coordinator 获得mu锁\n")
		for c.taskHeap.Len() > 0 {
			top := heap.Pop(c.taskHeap).(TaskExpired)
			if top.taskExpired > time.Now().Unix() {
				heap.Push(c.taskHeap, top)
				break
			}
			task := c.doingTasks[top.taskId]
			log.Printf(
				"Found timed-out %s task %d previously running on worker %d. Prepare to re-assign",
				task.Type, task.Index, task.WorkerID)
			c.tasksChan <- task
		}
		c.mu.Unlock()
		log.Printf("coordinator 释放mu锁\n")
		c.hmu.Unlock()
		log.Printf("coordinator 释放hmu锁\n")
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.rNum = nReduce
	c.mNum = len(files)
	c.state = MAP
	c.tasksChan = make(chan Task, len(files))
	c.doingTasks = make(map[string]Task)
	c.taskHeap = new(TaskHeap)
	heap.Init(c.taskHeap)
	for i, file := range files {
		task := Task{Type: MAP, Index: i, HandleFile: file, MNum: c.mNum, RNum: c.rNum}
		c.doingTasks[GenTaskID(task.Index, task.Type)] = task
		c.tasksChan <- task
	}

	//开启后台线程周期性检查 任务是否处理超时，及时将任务送回待处理队列
	log.Printf("Coordinator start\n")
	c.server()
	go c.checkPoint()
	return &c
}
