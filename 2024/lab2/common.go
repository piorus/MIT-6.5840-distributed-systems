package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	ClientId int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key      string
	ClientId int
}

type GetReply struct {
	Value string
}

type AckArgs struct {
	ClientId int
}

type AckReply struct {
}
