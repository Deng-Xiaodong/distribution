package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"container/heap"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func (c *Coordinator) ApplyTask(doneTask DoneTaskArgs, addedTask *AddedTaskRly) error {

	//1 处理worker任务完成响应
	/*
		1 判断任务类型
		2 判断任务合法性
		8 检查是否应该切换到下一阶段
	*/
	if doneTask.TaskType != "" {
		c.hmu.Lock()
		index := -1
		for i := 0; i < c.taskHeap.Len(); i++ {
			if (*c.taskHeap)[i].taskId == GenTaskID(doneTask.TaskIndex, doneTask.TaskType) {
				index = i
				break
			}
		}
		if index >= 0 {
			heap.Remove(c.taskHeap, index)
		} else {
			log.Printf("finished task %d from worker %d is invalid\n ", doneTask.TaskIndex, doneTask.WorkerID)
		}
		c.hmu.Unlock()
		if index != -1 {
			c.mu.Lock()
			taskID := GenTaskID(doneTask.TaskIndex, doneTask.TaskType)
			if task, exist := c.doingTasks[taskID]; exist && task.Index == doneTask.TaskIndex {
				if doneTask.TaskType == MAP {
					for ri := 0; ri < c.rNum; ri++ {
						err := os.Rename(GenMapTempFile(doneTask.WorkerID, doneTask.TaskIndex, ri),
							GenMapFinalFile(doneTask.TaskIndex, ri))
						if err != nil {
							log.Fatalf(
								"Failed to mark map output file `%s` as final: %e",
								GenMapTempFile(doneTask.WorkerID, doneTask.TaskIndex, ri), err)
						}
					}

				} else if doneTask.TaskType == REDUCE {
					err := os.Rename(GenReduceTempFile(doneTask.WorkerID, doneTask.TaskIndex),
						GenReduceFinalFile(doneTask.TaskIndex))
					if err != nil {
						log.Fatalf("Failed to mark reduce output file `%s` as final %e",
							GenReduceFinalFile(doneTask.TaskIndex), err)
					}

				}
				delete(c.doingTasks, taskID)
				if len(c.doingTasks) == 0 {
					c.transit()
				}

			}
			c.mu.Unlock()
		}

	}

	//2 封装新任务，并加入最小堆
	task, ok := <-c.tasksChan
	if !ok {
		return nil
	}
	log.Printf("Assign %s task %d to worker %d\n", task.Type, task.Index, doneTask.WorkerID)
	c.mu.Lock()
	log.Println("获得新派任务锁")
	defer c.mu.Unlock()
	task.WorkerID = doneTask.WorkerID
	expired := time.Now().Add(10 * time.Second).Unix()
	task.Expired = expired
	c.doingTasks[GenTaskID(task.Index, task.Type)] = task
	addedTask.TaskIndex = task.Index
	addedTask.TaskType = task.Type
	addedTask.OpenFile = task.HandleFile
	addedTask.MNum = c.mNum
	addedTask.RNum = c.rNum
	c.hmu.Lock()
	heap.Push(c.taskHeap, TaskExpired{taskId: GenTaskID(task.Index, task.Type), taskExpired: expired})
	c.hmu.Unlock()
	return nil

}
func GenTaskID(index int, ty string) string {
	return fmt.Sprintf("%s_%d", ty, index)
}

// Add your RPC definitions here.
type DoneTaskArgs struct {
	WorkerID  int
	TaskIndex int
	TaskType  string
}
type AddedTaskRly struct {
	TaskIndex  int
	TaskType   string
	OpenFile   string
	MNum, RNum int
	//Expired   int64
}

func GenMapTempFile(workId, taskIndex, reduceOrder int) string {
	return fmt.Sprintf("/var/tmp/mapDict/temp/%d_%d_%d", workId, taskIndex, reduceOrder)
}
func GenMapFinalFile(taskIndex, reduceOrder int) string {
	return fmt.Sprintf("/var/tmp/mapDict/final/%d_%d", taskIndex, reduceOrder)
}
func GenReduceTempFile(workId, taskIndex int) string {
	return fmt.Sprintf("/var/tmp/reduceDict/temp/%d_%d", workId, taskIndex)
}
func GenReduceFinalFile(taskIndex int) string {
	return fmt.Sprintf("/var/tmp/reduceDict/final/%d", taskIndex)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
