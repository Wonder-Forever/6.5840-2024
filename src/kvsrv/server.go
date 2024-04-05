package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.

	// cache
	m map[string]string
	// do once map
	tokenMap  map[string]struct{}
	appendMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.m[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.judgeDone(args.Token) {
		kv.m[args.Key] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	oldVal := kv.m[args.Key]
	if !kv.judgeDone(args.Token) {
		kv.appendMap[args.Token] = oldVal
		kv.m[args.Key] = oldVal + args.Value
	} else {
		oldVal = kv.appendMap[args.Token]
	}
	reply.Value = oldVal
}

func (kv *KVServer) judgeDone(token string) bool {
	_, done := kv.tokenMap[token]
	kv.tokenMap[token] = struct{}{}

	return done
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.m = map[string]string{}
	kv.tokenMap = map[string]struct{}{}
	kv.appendMap = map[string]string{}

	return kv
}
