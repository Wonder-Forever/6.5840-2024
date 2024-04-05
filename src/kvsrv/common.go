package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	DoOnce
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	DoOnce
}

type GetReply struct {
	Value string
}

// DoOnce 每次请求时携带
type DoOnce struct {
	// 标识本次请求
	Token string
}
