package mr_mutex_version

import (
	"encoding/json"
	"fmt"
	"github.com/samber/lo"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	TempFileRootPath = "./"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % nReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := getTask()
		switch task.TaskType {
		case TaskNone:
			myLog("receive done signal, worker exist")
			return
		case TaskSleep:
			time.Sleep(WorkerSleepDuration)
		case TaskMap:
			doMapTask(task, mapf)
			taskReport(task)
		case TaskReduce:
			doReduceTask(task, reducef)
			taskReport(task)
		}
	}
}

// 读取文件，执行map函数
func doMapTask(taskInfo *TaskInfo, mapf func(string, string) []KeyValue) (res []string) {
	if taskInfo.TaskType != TaskMap {
		return
	}
	// 读取文件内容
	data, err := readFile(taskInfo, taskInfo.MapTaskFilePath)
	if err != nil {
		panic(fmt.Sprintf("read file error: %s", err))
	}
	// 调用map function 获取kv数组
	kvArr := mapf(taskInfo.MapTaskFilePath, string(data))
	// 映射为R个输出
	fileMap := map[string][]*KeyValue{}
	for _, kv := range kvArr {
		index := fmt.Sprintf("%d", ihash(kv.Key)%taskInfo.NReduce)
		fileName := taskInfo.GetFileName(index)
		tmpKv := kv
		fileMap[fileName] = append(fileMap[fileName], &tmpKv)
	}
	// 排序
	for k, arr := range fileMap {
		sort.Slice(fileMap[k], func(i, j int) bool {
			return arr[i].Key < arr[j].Key
		})
	}
	// 保存文件
	for fileName, arr := range fileMap {
		savaFile(fileName, arr, TaskMap)
		res = append(res, fileName)
	}
	sort.Strings(res)
	taskInfo.ReduceTaskFilePath = res
	return
}

// 逐个读取文件，按照key合并后，执行reduce函数
func doReduceTask(taskInfo *TaskInfo, reducef func(string, []string) string) {
	// 基于内存的缓存，用于合并相同的key
	m := map[string][]string{}
	myLog("do reduce task: %+v\n", taskInfo)

	// 合并相同key
	for _, filePath := range taskInfo.ReduceTaskFilePath {
		file, err := os.Open(filePath)
		defer file.Close()
		if err != nil {
			myLog(err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
	}

	// 逐个调用reduce方法
	reduceMap := map[string]string{}
	for k, v := range m {
		reduceMap[k] = reducef(k, v)
	}

	// 结果数组
	res := lo.MapToSlice(reduceMap, func(key string, value string) *KeyValue {
		return &KeyValue{
			Key:   key,
			Value: value,
		}
	})

	// 排序
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})

	// 保存文件
	fileName := taskInfo.GetFileName("")
	savaFile(fileName, res, TaskReduce)

	// 修改task信息
	taskInfo.ReduceOutPutFilePath = fileName
}

func readFile(taskInfo *TaskInfo, filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Sprintf("open file error: %s, task info: %+v", err, taskInfo))
		return nil, err
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			panic(fmt.Sprintf("close file error: %s", err))
		}
	}(file)
	data, err := io.ReadAll(file)
	if err != nil {
		panic(fmt.Sprintf("read file error: %s, task info: %+v", err, taskInfo))
		return nil, err
	}

	return data, nil
}

func savaFile(fileName string, kvArr []*KeyValue, taskType TypeTask) {
	tmpFile, err := os.CreateTemp(TempFileRootPath, fileName)
	if err != nil {
		panic(tmpFile)
	}
	defer func(oldpath, newpath string) {
		err = os.Rename(oldpath, newpath)
		if err != nil {
			panic(err)
		}
	}(tmpFile.Name(), fileName)

	switch taskType {
	// map任务的输出是json
	case TaskMap:
		enc := json.NewEncoder(tmpFile)
		for _, kv := range kvArr {
			err = enc.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
	// reduce任务的输出是k v
	case TaskReduce:
		for _, kv := range kvArr {
			fmt.Fprintf(tmpFile, "%v %v\n", kv.Key, kv.Value)
		}
	}
}

// RPC

func getTask() *TaskInfo {
	taskInfo := &TaskInfo{}
	ok := call("Coordinator.GetTask", "", taskInfo)
	myLog("woker get task info: %+v\n", taskInfo)
	if ok {
		return taskInfo
	} else {
		panic("get task info error")
	}
}

func taskReport(taskInfo *TaskInfo) {
	myLog("woker report task info: %+v\n", taskInfo)
	ok := call("Coordinator.TaskReport", taskInfo, taskInfo)
	if !ok {
		panic("get task info error")
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

	myLog(err)
	return false
}
