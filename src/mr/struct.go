package mr

import (
	"bytes"
	"fmt"
	"time"
)

type TypeTask string
type TaskStatus string

const (
	TaskMap     TypeTask   = "map"
	TaskReduce  TypeTask   = "reduce"
	TaskSleep   TypeTask   = "sleep"
	TaskNone    TypeTask   = "none"
	StatusToDo  TaskStatus = "todo"
	StatusDoing TaskStatus = "doing"
	StatusDone  TaskStatus = "done"

	TimeOutDuration     = time.Second * 10
	WorkerSleepDuration = time.Second
)

type TaskInfo struct {
	MapTaskFilePath      string   // Map任务读取文件的路径
	ReduceTaskFilePath   []string // Reduce任务读取文件的路径
	ReduceOutPutFilePath string   // Reduce任务输出文件的路径
	TaskType             TypeTask // 任务类型
	Number               int      // 任务编号
	NReduce              int      // 输出文件个数
}

type BaseTask struct {
	StartTime time.Time
	Status    TaskStatus
	TaskInfo
}

func (task *BaseTask) string() string {
	buffer := bytes.Buffer{}
	buffer.WriteString("BaseTask: {")
	if !task.StartTime.IsZero() {
		buffer.WriteString(fmt.Sprintf("StartTime:%s ", task.StartTime.Format("2006-01-02 15:04:05")))
	}
	buffer.WriteString(fmt.Sprintf("Number:%d ", task.Number))
	buffer.WriteString(fmt.Sprintf("Status:%s ", task.Status))
	buffer.WriteString(fmt.Sprintf("TaskType:%s ", task.TaskType))
	buffer.WriteString(fmt.Sprintf("MapTaskFilePath:%s ", task.MapTaskFilePath))
	buffer.WriteString(fmt.Sprintf("ReduceOutPutFilePath:%s ", task.ReduceOutPutFilePath))
	buffer.WriteString(fmt.Sprintf("ReduceTaskFilePath:%s }", task.ReduceTaskFilePath))
	return buffer.String()
}

// 判断该任务是否可以被分配
func (task *BaseTask) judgeAssigned() bool {
	switch task.Status {
	case StatusDone:
		return false
	case StatusToDo:
		return true
	case StatusDoing:
		return task.StartTime.Add(TimeOutDuration).Before(time.Now())
	}

	return false
}

// 判断该任务是否已经结束
func (task *BaseTask) judgeDone() bool {
	return task.Status == StatusDone
}

// 判断是否为一个任务
func (task *BaseTask) judgeSameTask(taskReported *TaskInfo) bool {
	return task.TaskType == taskReported.TaskType && task.Number == taskReported.Number
}

// Map任务转换为reduce任务
func (task *BaseTask) mapTaskToReduce() {
	task.StartTime = time.Time{}
	task.Status = StatusToDo
	task.TaskType = TaskReduce
}

func NewReduceTask(filePath []string, nReduce, number int) *BaseTask {
	return &BaseTask{
		StartTime: time.Time{},
		Status:    StatusToDo,
		TaskInfo: TaskInfo{
			MapTaskFilePath:      "",
			ReduceTaskFilePath:   filePath,
			ReduceOutPutFilePath: "",
			TaskType:             TaskReduce,
			Number:               number,
			NReduce:              nReduce,
		},
	}
}

// 同时支持printf和println
func myLog(args ...any) {
	return
	if len(args) == 1 {
		fmt.Println(args)
		return
	}

	format := args[0]
	fmt.Printf(format.(string), args[1:]...)
}
