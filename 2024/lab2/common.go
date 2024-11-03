package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	ClientId int
	Id       int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key      string
	ClientId int
	Id       int
}

type GetReply struct {
	Value string
}
