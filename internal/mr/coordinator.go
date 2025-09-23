package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 简化后的Coordinator - 基于你的原始结构优化
type Coordinator struct {
	mu sync.RWMutex

	MapNum       int
	MapFiles     []string // 存储文件名列表
	MapIdle      []uint64 // Map任务空闲状态bitmap
	MapRunning   []uint64 // Map任务运行状态bitmap
	MapDone      []uint64 // Map任务完成状态bitmap
	MapStartTime []int64  // Map任务开始时间数组

	// Reduce任务管理
	ReduceNum       int
	ReduceIdle      []uint64 // Reduce任务空闲状态bitmap
	ReduceRunning   []uint64 // Reduce任务运行状态bitmap
	ReduceDone      []uint64 // Reduce任务完成状态bitmap
	ReduceStartTime []int64  // Reduce任务开始时间数组

	// 配置
	TaskTimeout      int64 // 任务超时时间(秒)
	LastTimeoutCheck int64 // 上次超时检查时间
}

// bitmap工具函数
func getBitmapSize(n int) int {
	return (n + 63) / 64 // 向上取整到64的倍数
}

func setBit(bitmap []uint64, index int) {
	wordIndex := index / 64
	bitIndex := index % 64
	bitmap[wordIndex] |= (1 << bitIndex)
}

func clearBit(bitmap []uint64, index int) {
	wordIndex := index / 64
	bitIndex := index % 64
	bitmap[wordIndex] &^= (1 << bitIndex)
}

func testBit(bitmap []uint64, index int) bool {
	wordIndex := index / 64
	bitIndex := index % 64
	return (bitmap[wordIndex] & (1 << bitIndex)) != 0
}

// 找到第一个设置的bit位置，如果没有返回-1
func findFirstSetBit(bitmap []uint64, maxIndex int) int {
	for wordIndex := 0; wordIndex < len(bitmap); wordIndex++ {
		word := bitmap[wordIndex]
		if word != 0 {
			// 找到这个word中第一个设置的bit
			for bitIndex := 0; bitIndex < 64; bitIndex++ {
				if word&(1<<bitIndex) != 0 {
					globalIndex := wordIndex*64 + bitIndex
					if globalIndex < maxIndex {
						return globalIndex
					}
				}
			}
		}
	}
	return -1
}

// 统计bitmap中设置的bit数量
func countSetBits(bitmap []uint64, maxIndex int) int {
	count := 0
	for i := 0; i < maxIndex; i++ {
		if testBit(bitmap, i) {
			count++
		}
	}
	return count
}

// Your code here -- RPC handlers for the worker to call.
// RPC处理器：Worker请求任务
// RPC处理器：Worker请求任务
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()

	// 定期检查超时任务
	if now-c.LastTimeoutCheck > 5 { // 每5秒检查一次
		c.checkTimeouts(now)
		c.LastTimeoutCheck = now
	}

	// Map阶段：查找空闲的Map任务
	if !c.allMapTasksDone() {
		mapIndex := findFirstSetBit(c.MapIdle, c.MapNum)
		if mapIndex != -1 {
			// 分配这个Map任务
			clearBit(c.MapIdle, mapIndex)  // 从空闲中移除
			setBit(c.MapRunning, mapIndex) // 加入运行中
			c.MapStartTime[mapIndex] = now // 记录开始时间

			*reply = AskTaskReply{
				TaskType:  TaskMap,
				NumReduce: int32(c.ReduceNum),
				TaskKey:   int32(mapIndex),
				FileName:  c.MapFiles[mapIndex],
				MapNum:    int32(c.MapNum),
			}
			return nil
		}
		// 所有Map任务都在运行中，等待
		*reply = AskTaskReply{TaskType: TaskWait}
		return nil
	}

	// Reduce阶段：查找空闲的Reduce任务
	if !c.allReduceTasksDone() {
		reduceIndex := findFirstSetBit(c.ReduceIdle, c.ReduceNum)
		if reduceIndex != -1 {
			// 分配这个Reduce任务
			clearBit(c.ReduceIdle, reduceIndex)  // 从空闲中移除
			setBit(c.ReduceRunning, reduceIndex) // 加入运行中
			c.ReduceStartTime[reduceIndex] = now // 记录开始时间

			*reply = AskTaskReply{
				TaskType:  TaskReduce,
				NumReduce: int32(c.ReduceNum),
				TaskKey:   int32(reduceIndex),
				FileName:  "",
				MapNum:    int32(c.MapNum),
			}
			return nil
		}
		// 所有Reduce任务都在运行中，等待
		*reply = AskTaskReply{TaskType: TaskWait}
		return nil
	}

	// 所有任务完成
	*reply = AskTaskReply{TaskType: TaskExit}
	return nil
}

// 报告任务完成
// RPC处理器：Worker报告任务完成
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	if args == nil || reply == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	taskIndex := int(args.TaskKey)

	switch args.TaskType {
	case TaskMap:
		if taskIndex >= 0 && taskIndex < c.MapNum {
			// 只处理运行中的任务
			if testBit(c.MapRunning, taskIndex) {
				clearBit(c.MapRunning, taskIndex) // 从运行中移除

				if args.State == TaskDone {
					setBit(c.MapDone, taskIndex) // 标记为完成
				} else if args.State == TaskFailed {
					setBit(c.MapIdle, taskIndex) // 放回空闲队列
				}
				c.MapStartTime[taskIndex] = 0 // 清除开始时间
			}
		}

	case TaskReduce:
		if taskIndex >= 0 && taskIndex < c.ReduceNum {
			// 只处理运行中的任务
			if testBit(c.ReduceRunning, taskIndex) {
				clearBit(c.ReduceRunning, taskIndex) // 从运行中移除

				if args.State == TaskDone {
					setBit(c.ReduceDone, taskIndex) // 标记为完成
				} else if args.State == TaskFailed {
					setBit(c.ReduceIdle, taskIndex) // 放回空闲队列
				}
				c.ReduceStartTime[taskIndex] = 0 // 清除开始时间
			}
		}

	default:
		return fmt.Errorf("invalid task type: %v", args.TaskType)
	}

	return nil
}

// 检查超时任务 - 只检查运行中的任务
func (c *Coordinator) checkTimeouts(now int64) {
	// 检查运行中的Map任务超时
	for i := 0; i < c.MapNum; i++ {
		if testBit(c.MapRunning, i) && now-c.MapStartTime[i] > c.TaskTimeout {
			// 超时任务处理
			clearBit(c.MapRunning, i) // 从运行中移除
			setBit(c.MapIdle, i)      // 放回空闲队列
			c.MapStartTime[i] = 0     // 清除开始时间
		}
	}

	// 检查运行中的Reduce任务超时
	for i := 0; i < c.ReduceNum; i++ {
		if testBit(c.ReduceRunning, i) && now-c.ReduceStartTime[i] > c.TaskTimeout {
			// 超时任务处理
			clearBit(c.ReduceRunning, i) // 从运行中移除
			setBit(c.ReduceIdle, i)      // 放回空闲队列
			c.ReduceStartTime[i] = 0     // 清除开始时间
		}
	}
}

// 检查所有Map任务是否完成
func (c *Coordinator) allMapTasksDone() bool {
	return countSetBits(c.MapDone, c.MapNum) == c.MapNum
}

// 检查所有Reduce任务是否完成
func (c *Coordinator) allReduceTasksDone() bool {
	return countSetBits(c.ReduceDone, c.ReduceNum) == c.ReduceNum
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.allMapTasksDone() && c.allReduceTasksDone()
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapBitmapSize := getBitmapSize(len(files))
	reduceBitmapSize := getBitmapSize(nReduce)

	c := &Coordinator{
		// Map任务初始化
		MapNum:       len(files),
		MapFiles:     make([]string, len(files)),
		MapIdle:      make([]uint64, mapBitmapSize),
		MapRunning:   make([]uint64, mapBitmapSize),
		MapDone:      make([]uint64, mapBitmapSize),
		MapStartTime: make([]int64, len(files)),

		// Reduce任务初始化
		ReduceNum:       nReduce,
		ReduceIdle:      make([]uint64, reduceBitmapSize),
		ReduceRunning:   make([]uint64, reduceBitmapSize),
		ReduceDone:      make([]uint64, reduceBitmapSize),
		ReduceStartTime: make([]int64, nReduce),

		// 配置：10秒超时
		TaskTimeout:      10,
		LastTimeoutCheck: time.Now().Unix(),
	}

	// 初始化Map任务 - 所有任务初始状态为空闲
	copy(c.MapFiles, files)
	for i := 0; i < len(files); i++ {
		setBit(c.MapIdle, i)
	}

	// 初始化Reduce任务 - 所有任务初始状态为空闲
	for i := 0; i < nReduce; i++ {
		setBit(c.ReduceIdle, i)
	}
	c.server()
	return c
}

// 获取性能统计（可选，调试用）
func (c *Coordinator) GetStatus() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	mapIdle := countSetBits(c.MapIdle, c.MapNum)
	mapRunning := countSetBits(c.MapRunning, c.MapNum)
	mapDone := countSetBits(c.MapDone, c.MapNum)

	reduceIdle := countSetBits(c.ReduceIdle, c.ReduceNum)
	reduceRunning := countSetBits(c.ReduceRunning, c.ReduceNum)
	reduceDone := countSetBits(c.ReduceDone, c.ReduceNum)

	return fmt.Sprintf(
		"Map: Idle=%d, Running=%d, Done=%d/%d\n"+
			"Reduce: Idle=%d, Running=%d, Done=%d/%d\n"+
			"Memory: MapBitmap=%d bytes, ReduceBitmap=%d bytes",
		mapIdle, mapRunning, mapDone, c.MapNum,
		reduceIdle, reduceRunning, reduceDone, c.ReduceNum,
		len(c.MapIdle)*8, len(c.ReduceIdle)*8,
	)
}
