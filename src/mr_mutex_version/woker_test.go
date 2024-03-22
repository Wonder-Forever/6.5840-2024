package mr_mutex_version

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

var mapFunction = func(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

var reduceFunction = func(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}

func TestMapTask(t *testing.T) {
	taskInfo := &TaskInfo{
		MapTaskFilePath: "../main/pg-being_ernest.txt",
		TaskType:        TaskMap,
		Number:          0,
		NReduce:         10,
	}
	doMapTask(taskInfo, mapFunction)
}

func TestReduceTask(t *testing.T) {
	taskInfo := &TaskInfo{
		MapTaskFilePath: "../main/pg-being_ernest.txt",
		ReduceTaskFilePath: []string{
			"../main/mr-out-map-0-0",
			"../main/mr-out-map-0-1",
			"../main/mr-out-map-0-2",
			"../main/mr-out-map-0-3",
			"../main/mr-out-map-0-4",
			"../main/mr-out-map-0-5",
			"../main/mr-out-map-0-6",
			"../main/mr-out-map-0-7",
			"../main/mr-out-map-0-8",
			"../main/mr-out-map-0-9",
		},
		ReduceOutPutFilePath: "",
		TaskType:             TaskReduce,
		Number:               0,
		NReduce:              10,
	}

	doReduceTask(taskInfo, reduceFunction)
	fmt.Println(taskInfo)
}
