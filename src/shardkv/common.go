package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
    ErrBehindConfig = "ErrBehindConfig"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Me    string
    Id    string
    Num   int //Config Num

}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    Me  string
    Id  string
    Num int //Config Num
}

type GetReply struct {
	Err   Err
	Value string
}

type TranDataArgs struct {
    Shard  int
    Num    int //Config Num
}

type TranDataReply struct {
    Err Err
    Data map[string] string
    Last map[string] string
}

