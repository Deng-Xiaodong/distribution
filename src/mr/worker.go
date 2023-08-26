package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (b ByKey) Len() int {
	return len(b)
}

func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}

func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	/*
		1 申请任务，同时响应已完成任务
		2 如果新任务是map任务
			- map处理
		3 如果新任务是reduce任务
			- reduce处理
	*/
	workerID := os.Getpid()
	log.Printf("Worker %d started\n", workerID)
	doneTask := DoneTaskArgs{WorkerID: workerID}
	addedTask := &AddedTaskRly{}
	for true {
		if call("Coordinator.ApplyTask", doneTask, addedTask) {
			if addedTask.TaskType == FINISHED {
				log.Printf("Received job finish signal from coordinator")
				break
			}
			log.Printf("Received %s task %d from coordinator", addedTask.TaskType, addedTask.TaskIndex)
			//开始处理
			if addedTask.TaskType == MAP {
				file, err := os.Open(addedTask.OpenFile)
				if err != nil {
					log.Fatal(err)
				}
				text, errR := io.ReadAll(file)
				if errR != nil {
					log.Fatal(errR)
				}
				file.Close()
				KVs := mapf(addedTask.OpenFile, string(text))
				hashKV := make(map[int][]KeyValue)
				for _, kv := range KVs {
					ri := ihash(kv.Key) % addedTask.RNum
					hashKV[ri] = append(hashKV[ri], kv)
				}
				for ri, rkvs := range hashKV {
					outFile := GenMapTempFile(workerID, addedTask.TaskIndex, ri)
					fileOut, errC := os.Create(outFile)
					if errC != nil {
						log.Fatal(errC)
					}
					for _, rkv := range rkvs {
						fmt.Fprintf(fileOut, "%v\t%v\n", rkv.Key, rkv.Value)
					}
					fileOut.Close()

				}

			} else if addedTask.TaskType == REDUCE {
				var lines []string
				var kvsInOrder ByKey
				for i := 0; i < addedTask.MNum; i++ {
					mapFile, err := os.Open(GenMapFinalFile(i, addedTask.TaskIndex))
					if err != nil {
						log.Fatal(err)
					}
					text, errR := io.ReadAll(mapFile)
					if errR != nil {
						log.Fatal(errR)
					}
					lines = append(lines, strings.Split(string(text), "\n")...)
				}

				for _, line := range lines {
					if strings.TrimSpace(line) == "" {
						continue
					}
					split := strings.Split(line, "\t")
					if len(split) != 2 {
						continue
					}
					kvsInOrder = append(kvsInOrder, KeyValue{Key: split[0], Value: split[1]})

				}
				sort.Sort(kvsInOrder)
				outFile, err := os.Create(GenReduceTempFile(workerID, addedTask.TaskIndex))

				i := 0
				for i < len(kvsInOrder) {
					values := []string{kvsInOrder[i].Value}
					key := kvsInOrder[i].Key
					i++
					for i < len(kvsInOrder) && kvsInOrder[i] == kvsInOrder[i-1] {
						values = append(values, kvsInOrder[i].Value)
						i++
					}
					out := reducef(key, values)

					if err != nil {
						log.Fatal(err)
					}
					fmt.Fprintf(outFile, "%s\t%s\n", key, out)

				}
				outFile.Close()

			}

		}
		doneTask.TaskIndex = addedTask.TaskIndex
		doneTask.TaskType = addedTask.TaskType
		log.Printf("Finished %s task %d", addedTask.TaskType, addedTask.TaskIndex)
	}
	log.Printf("Worker %d exit\n", workerID)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
