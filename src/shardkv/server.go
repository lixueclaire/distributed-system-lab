package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
    Type       string
    Key        string
    Value      string
    Id         string
    Client     string
    Config     shardmaster.Config //for reconfiguration
    NewData    GetDataReply //for reconfiguration
    Mark       int64
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
    now        int
    data       map[string] string
    last       map[string] string
    config     shardmaster.Config
}

func (kv *ShardKV) Wait(seq int) Op {
    to := 10 * time.Millisecond
    for {
        status, value := kv.px.Status(seq)
        if status == paxos.Decided{
            return value.(Op)
        }
        time.Sleep(to)
        if to < 10 * time.Second {
            to *= 2
        }
    }
}

func (kv *ShardKV) AddLog(op Op){
    for {
        if op.Type == "Reconfiguration" {
            if op.Config.Num <= kv.config.Num {
                return
            }
        } else if op.Type == "GetData" {
            
        } else {
            t, ok := kv.last[op.Client + op.Type]
            if (ok && t >= op.Id) || kv.config.Shards[key2shard(op.Key)] != kv.gid{
                return
            }
        }
        seq := kv.now + 1
        kv.px.Start(seq, op)
        res := kv.Wait(seq)
        if res.Type == "Put" {
            t, ok := kv.last[res.Client + res.Type]
            if !(ok && t >= res.Id) && kv.config.Shards[key2shard(res.Key)] == kv.gid {
                kv.data[res.Key] = res.Value
                kv.last[res.Client + res.Type] = res.Id
            }
        } else if res.Type == "Append" {
            t, ok := kv.last[res.Client + res.Type]
            if !(ok && t >= res.Id) && kv.config.Shards[key2shard(res.Key)] == kv.gid {
                //tt := kv.data[res.Key]
                kv.data[res.Key] += res.Value
                kv.last[res.Client + res.Type] = res.Id
                //fmt.Println(kv.me, res.Type, res.Key, res.Value, tt, kv.data[res.Key], res.Id, t, kv.last[res.Client + res.Type])
                
            }
        } else if res.Type == "Get" {
            t, ok := kv.last[res.Client + res.Type]
            if !(ok && t >= res.Id) && kv.config.Shards[key2shard(res.Key)] == kv.gid {
                kv.last[res.Client + res.Type] = res.Id
                //fmt.Println(kv.me, res.Type, res.Key, kv.data[res.Key])
            }
        } else if res.Type == "Reconfiguration" {
            if res.Config.Num > kv.config.Num {
                for key, value := range res.NewData.Data {
                    kv.data[key] = value
                }
                for key, value := range res.NewData.Last {
                    t, ok := kv.last[key]
                    if !(ok && t >= value) {
                        kv.last[key] = value
                    }
                }
                kv.config = res.Config
            }
        } else {
           //GetData
        }
        kv.px.Done(seq)
        kv.now ++
        if res.Mark == op.Mark {
            return
        }
    }
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: "Get", Key: args.Key, Id: args.Id, Client: args.Me, Mark: rand.Int63()}
    kv.AddLog(op)
    shard := key2shard(args.Key)
    if kv.gid != kv.config.Shards[shard] {
        reply.Err = ErrWrongGroup
        return nil
    }
    tmp, ok := kv.data[args.Key]
    if !ok {
        reply.Err = ErrNoKey
    } else {
        reply.Value = tmp
    }
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, Client: args.Me, Mark: rand.Int63()}
    kv.AddLog(op)
    shard := key2shard(args.Key)
    if kv.gid != kv.config.Shards[shard] {
        reply.Err = ErrWrongGroup
        return nil
    }
	return nil
}

func (kv *ShardKV) GetData(args *GetDataArgs, reply *GetDataReply) error {
    if args.Num > kv.config.Num {
        reply.Err = ErrBehindConfig
        return nil
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: "GetData", Mark: rand.Int63()}
    kv.AddLog(op)
    shard := args.Shard
    reply.Data = map[string] string{}
    reply.Last = map[string] string{}
    for key, value := range kv.data {
        if key2shard(key) == shard {
            reply.Data[key] = value
        }
    }
    for key, value := range kv.last {
        reply.Last[key] = value
    }
    return nil
}

func (kv *ShardKV) Add(res *GetDataReply, tmp *GetDataReply) {
    for key, value := range tmp.Data {
        res.Data[key] = value
    }
    for key, value := range tmp.Last {
        t, ok := res.Last[key]
        if !(ok && t >= value) {
            res.Last[key] = value
        }
    }
}

func (kv *ShardKV) Reconfiguration(new shardmaster.Config) bool {
    newdata := GetDataReply{Err: OK, Data: map[string] string{}, Last: map[string] string{}}
    old := &kv.config
    for shard, oldg := range old.Shards {
        newg := new.Shards[shard]
        if newg != oldg && newg == kv.gid {
            for _, server := range old.Groups[oldg] {
                args := GetDataArgs{Shard: shard, Num: old.Num}
                reply := GetDataReply{Err: OK}
                ok := call(server, "ShardKV.GetData", &args, &reply)
                if ok && reply.Err != OK {
                    return false
                }
                if ok && reply.Err == OK{
                    kv.Add(&newdata, &reply)
                    break
                }
            }
        }
    }
    op := Op{Type: "Reconfiguration", Config: new, NewData: newdata, Mark: rand.Int63()}
    kv.AddLog(op)
    return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    newnum := kv.sm.Query(-1).Num
    for i := kv.config.Num + 1; i <= newnum; i++ {
        config := kv.sm.Query(i)
        if kv.Reconfiguration(config) == false {
            break
        }
    }
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
    kv.now = 0
    kv.data = map[string] string {}
    kv.last = map[string] string {}
    kv.config = shardmaster.Config{Num: -1}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)


	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
