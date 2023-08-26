package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
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
		c.mu.Lock()
		//log.Printf("coordinator 获得mu锁\n")
		c.hmu.Lock()
		//log.Printf("coordinator 获得hmu锁\n")
		//index := -1
		taskID := GenTaskID(doneTask.TaskIndex, doneTask.TaskType)
		if task, exist := c.doingTasks[taskID]; exist && task.WorkerID == doneTask.WorkerID {
			//for i := 0; i < c.taskHeap.Len(); i++ {
			//	if (*c.taskHeap)[i].taskId == taskID {
			//		index = i
			//		break
			//	}
			//}
			//if index >= 0 {
			//	heap.Remove(c.taskHeap, index)
			//}
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
		c.hmu.Unlock()
		//log.Printf("coordinator 释放hmu锁\n")
		c.mu.Unlock()
		//log.Printf("coordinator 释放mu锁\n")
	}

	//2 封装新任务，并加入最小堆
	task, ok := <-c.tasksChan
	if !ok {
		return nil
	}
	log.Printf("Assign %s task %d to worker %s\n", task.Type, task.Index, doneTask.WorkerID)
	c.hmu.Lock()
	//log.Printf("coordinator 获得hmu锁\n")
	c.mu.Lock()
	//log.Printf("coordinator 获得mu锁\n")
	task.WorkerID = doneTask.WorkerID
	expired := time.Now().Add(15 * time.Second)
	task.Expired = expired
	c.doingTasks[GenTaskID(task.Index, task.Type)] = task
	(*addedTask).TaskIndex = task.Index
	(*addedTask).TaskType = task.Type
	(*addedTask).OpenFile = task.HandleFile
	(*addedTask).MNum = c.mNum
	(*addedTask).RNum = c.rNum
	//log.Printf("准备推入%s %d 到最小堆", addedTask.TaskType, addedTask.TaskIndex)
	//heap.Push(c.taskHeap, TaskExpired{taskId: GenTaskID(task.Index, task.Type), taskExpired: expired})
	//log.Printf("成功推入%s %d 到最小堆", addedTask.TaskType, addedTask.TaskIndex)
	c.hmu.Unlock()
	//log.Printf("coordinator 释放hmu锁\n")
	c.mu.Unlock()
	//log.Printf("coordinator 释放mu锁\n")
	return nil

}
func GenTaskID(index int, ty string) string {
	return fmt.Sprintf("%s_%d", ty, index)
}

// Add your RPC definitions here.
type DoneTaskArgs struct {
	WorkerID  string
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

func GenMapTempFile(workId string, taskIndex, reduceOrder int) string {
	return fmt.Sprintf("/var/tmp/mapDict/temp/%s_%d_%d", workId, taskIndex, reduceOrder)
}
func GenMapFinalFile(taskIndex, reduceOrder int) string {
	return fmt.Sprintf("/var/tmp/mapDict/final/%d_%d", taskIndex, reduceOrder)
}
func GenReduceTempFile(workId string, taskIndex int) string {
	return fmt.Sprintf("/var/tmp/reduceDict/temp/%s_%d", workId, taskIndex)
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
