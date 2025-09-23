package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

	// Worker循环：持续请求任务直到被告知退出
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}

		// 调用coordinator请求任务
		ok := call("Coordinator.AskTask", &args, &reply)
		if !ok {
			// RPC调用失败，coordinator可能已经完成或崩溃，退出
			fmt.Println("Failed to contact coordinator, exiting")
			return
		}

		// 处理任务
		if !dispatchTask(&reply, mapf, reducef) {
			// TaskExit情况下返回false，退出循环
			return
		}
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func dispatchTask(task *AskTaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	switch task.TaskType {
	case TaskWait:
		// 没有任务，等待一段时间再请求
		time.Sleep(time.Second)
		return true

	case TaskMap:
		doMapTask(task, mapf)
		return true

	case TaskReduce:
		doReduceTask(task, reducef)
		return true

	case TaskExit:
		// 收到退出信号
		fmt.Println("All tasks completed, worker exiting")
		return false

	default:
		fmt.Printf("Unknown task type: %v\n", task.TaskType)
		time.Sleep(time.Second)
		return true
	}
}

// 执行Map任务
func doMapTask(task *AskTaskReply, mapf func(string, string) []KeyValue) {
	mapIndex := int(task.TaskKey)
	filename := task.FileName
	nReduce := int(task.NumReduce)

	// 读取输入文件
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		reportTask(TaskMap, mapIndex, TaskFailed)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		reportTask(TaskMap, mapIndex, TaskFailed)
		return
	}

	// 执行map函数
	kva := mapf(filename, string(content))

	// 按reduce任务分桶
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// 为每个reduce任务写入中间文件
	for reduceIdx, kvs := range buckets {
		if len(kvs) == 0 {
			continue // 跳过空桶
		}

		// 中间文件名格式：mr-X-Y (X是Map任务号，Y是Reduce任务号)
		intermediateFile := fmt.Sprintf("mr-%d-%d", mapIndex, reduceIdx)

		// 使用临时文件确保原子性写入
		tempFile, err := os.CreateTemp("", "mr-temp-*")
		if err != nil {
			log.Printf("cannot create temp file: %v", err)
			reportTask(TaskMap, mapIndex, TaskFailed)
			return
		}

		// 写入JSON格式的key/value对
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				log.Printf("cannot encode kv: %v", err)
				tempFile.Close()
				os.Remove(tempFile.Name())
				reportTask(TaskMap, mapIndex, TaskFailed)
				return
			}
		}
		tempFile.Close()

		// 原子性重命名到最终文件
		if err := os.Rename(tempFile.Name(), intermediateFile); err != nil {
			log.Printf("cannot rename temp file: %v", err)
			os.Remove(tempFile.Name())
			reportTask(TaskMap, mapIndex, TaskFailed)
			return
		}
	}

	// 报告任务完成
	reportTask(TaskMap, mapIndex, TaskDone)
}

// 执行Reduce任务
func doReduceTask(task *AskTaskReply, reducef func(string, []string) string) {
	reduceIdx := int(task.TaskKey)
	mapNum := int(task.MapNum)

	// 读取所有相关的中间文件
	var allKVs []KeyValue

	// 读取所有Map任务产生的中间文件 mr-X-reduceIdx
	for mapIdx := 0; mapIdx < mapNum; mapIdx++ {
		filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
		file, err := os.Open(filename)
		if err != nil {
			// 中间文件可能不存在（对应的map任务没有输出数据到这个reduce桶）
			continue
		}

		// 读取JSON格式的key/value对
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // 文件结束或解码错误
			}
			allKVs = append(allKVs, kv)
		}
		file.Close()
	}

	// 按key排序
	sort.Sort(ByKey(allKVs))

	// 输出文件名格式：mr-out-X (X是Reduce任务号)
	outputFile := fmt.Sprintf("mr-out-%d", reduceIdx)

	// 使用临时文件确保原子性写入
	tempFile, err := os.CreateTemp("", "mr-out-temp-*")
	if err != nil {
		log.Printf("cannot create temp output file: %v", err)
		reportTask(TaskReduce, reduceIdx, TaskFailed)
		return
	}

	// 处理每个唯一的key
	i := 0
	for i < len(allKVs) {
		j := i + 1
		// 找到相同key的所有value
		for j < len(allKVs) && allKVs[j].Key == allKVs[i].Key {
			j++
		}

		// 收集所有相同key的values
		var values []string
		for k := i; k < j; k++ {
			values = append(values, allKVs[k].Value)
		}

		// 执行reduce函数
		output := reducef(allKVs[i].Key, values)

		// 按照要求的格式写入结果："%v %v"
		fmt.Fprintf(tempFile, "%v %v\n", allKVs[i].Key, output)

		i = j
	}
	tempFile.Close()

	// 原子性重命名到最终文件
	if err := os.Rename(tempFile.Name(), outputFile); err != nil {
		log.Printf("cannot rename temp output file: %v", err)
		os.Remove(tempFile.Name())
		reportTask(TaskReduce, reduceIdx, TaskFailed)
		return
	}

	// 报告任务完成
	reportTask(TaskReduce, reduceIdx, TaskDone)
}

// 报告任务情况
func reportTask(taskType TaskType, taskKey int, state TaskState) {
	args := ReportTaskArgs{
		TaskType: taskType,
		State:    state,
		TaskKey:  int32(taskKey),
	}
	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		log.Printf("Failed to report task: Type=%v, Key=%v, State=%v", taskType, taskKey, state)
	}
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

	log.Printf("RPC call error: %v", err)
	return false
}
