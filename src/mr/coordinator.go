package mr

import (
	"bytes"
	"fmt"
	"github.com/samber/lo"
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Coordinator
// map任务的数量和文件数量一致
// reduce任务的数量和nReduce数量一致
// 这两个并不是总相等
type Coordinator struct {
	nReduce  int         // map任务输出文件数量
	Status   TaskStatus  // Coordinator的状态
	TaskType TypeTask    // 标识当前任务类型
	TaskList []*BaseTask // 任务列表
	sync.RWMutex
}

func (c *Coordinator) string() string {
	buffer := bytes.Buffer{}
	buffer.WriteString("Coordinator: {")
	buffer.WriteString(fmt.Sprintf("Status:%s ", c.Status))
	buffer.WriteString(fmt.Sprintf("TaskType:%s ", c.TaskType))
	buffer.WriteString("TaskList:\n")
	lo.ForEach(c.TaskList, func(item *BaseTask, index int) {
		buffer.WriteString("\t" + item.string() + "\n")
	})
	buffer.WriteString("}")
	return buffer.String()
}

// RPC

func (c *Coordinator) GetTask(args string, reply *TaskInfo) error {
	baseTask := c.getTaskOne()
	myLog("coordinator send task info%+v\n", baseTask)
	*reply = baseTask.TaskInfo
	return nil
}

func (c *Coordinator) TaskReport(args *TaskInfo, reply *TaskInfo) error {
	myLog("coordinator get report info  %+v\n", args)
	c.handleTaskReport(args)
	return nil
}

func (c *Coordinator) initMapTask(files []string) {
	myLog("init map task start")
	for i, file := range files {
		// 任务开始时间在分配任务时再指定
		baseTask := &BaseTask{
			Status: StatusToDo,
			TaskInfo: TaskInfo{
				MapTaskFilePath: file,
				TaskType:        TaskMap,
				Number:          i,
				NReduce:         c.nReduce,
			},
		}
		c.TaskList = append(c.TaskList, baseTask)
		myLog("add BaseTask: %+v\n", baseTask)
	}
	myLog("init map task done")
}

func (c *Coordinator) initReduceTask() {
	myLog("init reduce task start")
	c.Lock()
	defer c.Unlock()

	var reduceTaskList []*BaseTask
	var mapOutputFilePath []string
	for _, baseTask := range c.TaskList {
		mapOutputFilePath = append(mapOutputFilePath, baseTask.ReduceTaskFilePath...)
	}

	for i := 0; i < c.nReduce; i++ {
		reduceTaskList = append(reduceTaskList, NewReduceTask(lo.Filter(mapOutputFilePath, func(item string, index int) bool {
			return strings.HasSuffix(item, fmt.Sprintf("-%d", i))
		}), c.nReduce, i))
	}

	c.TaskList = reduceTaskList
	myLog("init reduce task done %s", c.string())
}

func (c *Coordinator) getSleepTask() *BaseTask {
	return &BaseTask{
		StartTime: time.Time{},
		Status:    "",
		TaskInfo: TaskInfo{
			MapTaskFilePath: "",
			TaskType:        TaskSleep,
			Number:          0,
			NReduce:         0,
		},
	}
}

func (c *Coordinator) getMapTask() *BaseTask {
	c.Lock()
	defer c.Unlock()

	if c.TaskType != TaskMap {
		myLog("task type is reduce %s\n", c.string())
		return nil
	}
	taskDoneFlag := true
	// 尝试找到一个未开始或者已超时的task
	for _, task := range c.TaskList {
		if task.judgeAssigned() {
			task.Status = StatusDoing
			task.StartTime = time.Now()
			myLog("get an assigned map task %+v\n", task)
			tmpTask := *task
			return &tmpTask

		}
		taskDoneFlag = taskDoneFlag && task.judgeDone()
	}
	myLog("can't find any map task to return %s\n", c.string())
	// map任务已完成，修改任务状态，返回nil
	if taskDoneFlag {
		myLog("all map task is done %s\n", c.string())
		c.TaskType = TaskReduce
		// initReduceTask需要获取锁，这里先释放
		c.Unlock()
		c.initReduceTask()
		c.Lock()
		return nil
	} else {
		// map任务未完成，需要等待，返回sleep任务
		myLog("map task need to be waited %s\n", c.string())
		return c.getSleepTask()
	}
}

func (c *Coordinator) getReduceTask() *BaseTask {
	c.Lock()
	defer c.Unlock()

	if c.TaskType != TaskReduce {
		myLog("task type is not reduce but call getReduceTask: %s", c.string())
	}

	reduceDoneFlag := true
	for _, baseTask := range c.TaskList {
		if baseTask.judgeAssigned() {
			baseTask.Status = StatusDoing
			baseTask.StartTime = time.Now()
			return baseTask
		}
		reduceDoneFlag = reduceDoneFlag && baseTask.judgeDone()
	}

	// reduce任务已完成
	if reduceDoneFlag {
		c.Status = StatusDone
		myLog("all reduce task is done %s\n", c.string())
		return nil
	} else {
		// reduce任务未完成，需要等待，返回sleep任务
		myLog("reduce task need to be waited %s\n", c.string())
		return c.getSleepTask()
	}

}

func (c *Coordinator) getTaskOne() *BaseTask {
	task := c.getMapTask()
	if task != nil {
		tmpTask := *task
		return &tmpTask
	}
	task = c.getReduceTask()
	if task != nil {
		tmpTask := *task
		return &tmpTask
	}
	// 任务完成
	c.taskDone()
	return &BaseTask{
		Status: StatusDone,
		TaskInfo: TaskInfo{
			TaskType: TaskNone,
		},
	}
}

// 修改状态，删除所有map文件
func (c *Coordinator) taskDone() {
	c.Lock()
	defer c.Unlock()

	c.Status = StatusDone
	lo.ForEach(c.TaskList, func(baseTask *BaseTask, index int) {
		lo.ForEach(baseTask.ReduceTaskFilePath, func(filePath string, index int) {
			os.Remove(filePath)
		})
	})
}

func (c *Coordinator) handleTaskReport(taskInfo *TaskInfo) {
	switch taskInfo.TaskType {
	case TaskMap:
		c.handleMapTaskReport(taskInfo)
	case TaskReduce:
		c.handleReduceTaskReport(taskInfo)
	}
}

// 处理map任务的上报
func (c *Coordinator) handleMapTaskReport(taskReported *TaskInfo) {
	c.Lock()
	defer c.Unlock()

	for _, task := range c.TaskList {
		if task.judgeSameTask(taskReported) {
			// Map任务处理完成
			task.TaskInfo = *taskReported
			task.Status = StatusDone
		}
	}
}

// 处理reduce任务的上报
func (c *Coordinator) handleReduceTaskReport(taskReported *TaskInfo) {
	c.Lock()
	defer c.Unlock()

	for _, task := range c.TaskList {
		if task.judgeSameTask(taskReported) {
			// Map任务处理完成
			task.TaskInfo = *taskReported
			task.Status = StatusDone
		}
	}
}

func (c *Coordinator) init(files []string, nReduce int) {
	myLog("init coordinator start")
	c.nReduce = nReduce
	c.Status = StatusDoing
	c.TaskType = TaskMap
	c.RWMutex = sync.RWMutex{}
	c.initMapTask(files)
	myLog("init coordinator done %s\n", c.string())
}

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
	c.RLock()
	defer c.RUnlock()

	return c.Status == StatusDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.init(files, nReduce)
	c.server()

	return &c
}
