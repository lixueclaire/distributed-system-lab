package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
    now        int   // max seq number
    max        int   // max config number
}


type Op struct {
	// Your data here.
    Type string
    GID  int64
    Servers []string
    Shard int
    Num  int
    Mark   int64
}

func (sm *ShardMaster) Wait(seq int) Op {
    to := 10 * time.Millisecond
    for {
        status, value := sm.px.Status(seq)
        if status == paxos.Decided {
            return value.(Op)
        }
        time.Sleep(to)
        if to < 10 * time.Second {
            to *= 2
        }
    }
}
    
func (sm *ShardMaster) MakeConfig() *Config {
    config := Config{Num: sm.max + 1, Shards: [NShards]int64{}, Groups: map[int64][]string{}}
    for shard, gid := range sm.configs[sm.max].Shards {
        config.Shards[shard] = gid
    }
    for gid, servers := range sm.configs[sm.max].Groups {
        config.Groups[gid] = servers
    }
    sm.configs = append(sm.configs, config)
    sm.max ++
    return &sm.configs[sm.max]
    
}
    
func (sm *ShardMaster) DoJoin(op Op) {
    config := sm.MakeConfig()
    gid := op.GID
    _, ok := config.Groups[gid]
    if ok {
        return
    }
    config.Groups[gid] = op.Servers
    if len(config.Groups) == 1 {
        for i := 0; i < len(config.Shards); i++ {
            config.Shards[i] = gid
        }
        return
    }
	
	counts := map[int64] int{}
    for _, g := range config.Shards {
        counts[g] ++
    }
	for {
		var ming int64 = -1
		var maxg int64 = -1
		for g, _ := range config.Groups {
			if ming == -1 || counts[g] < counts[ming] {
				ming = g
			}
			if maxg == -1 || counts[g] > counts[maxg] {
				maxg = g
			}
		}
		if counts[maxg] - counts[ming] <= 1 {
			break
		}
		for i := 0; i < len(config.Shards); i++ {
            if config.Shards[i] == maxg {
				config.Shards[i] = ming
				break
			}
        }
		counts[maxg]--
		counts[ming]++
	}
}
    
func (sm *ShardMaster) DoLeave(op Op) {
    config := sm.MakeConfig()
    gid := op.GID
    _, ok := config.Groups[gid]
    if !ok {
        return
    }
    delete(config.Groups, gid)
    counts := map[int64] int{}
    for _, g := range config.Shards {
        counts[g] ++
    }
	for i, _ := range config.Shards {
		if (config.Shards[i] == gid) {
			var newg int64 = -1
			for g, _ := range config.Groups {
				if newg == -1 || counts[g] < counts[newg] {
					newg = g
				}
			}
			config.Shards[i] = newg
			counts[newg]++
		}
	}
	
	for {
		var ming int64 = -1
		var maxg int64 = -1
		for g, _ := range config.Groups {
			if ming == -1 || counts[g] < counts[ming] {
				ming = g
			}
			if maxg == -1 || counts[g] > counts[maxg] {
				maxg = g
			}
		}
		if counts[maxg] - counts[ming] <= 1 {
			break
		}
		for i := 0; i < len(config.Shards); i++ {
            if config.Shards[i] == maxg {
				config.Shards[i] = ming
				break
			}
        }
		counts[maxg]--
		counts[ming]++
	}
}
    
func (sm *ShardMaster) DoMove(op Op) {
    config := sm.MakeConfig()
    config.Shards[op.Shard] = op.GID
}
    
func (sm *ShardMaster) DoQuery(op Op) Config{
    if op.Num == -1 || op.Num > sm.max {
        return sm.configs[sm.max]
    } else {
        return sm.configs[op.Num]
    }
}

func (sm *ShardMaster) AddLog(op Op) {
    for {
        seq := sm.now + 1
        sm.px.Start(seq, op)
        res:= sm.Wait(seq)
        if res.Type == "Join" {
            sm.DoJoin(res)
        } else if res.Type == "Leave" {
            sm.DoLeave(res)
        } else if res.Type == "Move" {
            sm.DoMove(res)
        } else if res.Type == "Query" {
            //sm.DoQuery(res)
        } else {
            fmt.Println("unknown operation")
        }
        sm.px.Done(seq)
        sm.now ++
        if res.Mark == op.Mark {
            return
        }
    }
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
    sm.mu.Lock()
    defer sm.mu.Unlock()
    op := Op{Type: "Join", GID: args.GID, Servers: args.Servers, Mark: rand.Int63()}
    sm.AddLog(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
    sm.mu.Lock()
    defer sm.mu.Unlock()
    op := Op{Type: "Leave", GID: args.GID, Mark: rand.Int63()}
    sm.AddLog(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
    sm.mu.Lock()
    defer sm.mu.Unlock()
    op := Op{Type: "Move", GID: args.GID, Shard: args.Shard, Mark: rand.Int63()}
    sm.AddLog(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
    sm.mu.Lock()
    defer sm.mu.Unlock()
    op := Op{Type: "Query", Num: args.Num, Mark: rand.Int63()}
    sm.AddLog(op)
    reply.Config = sm.DoQuery(op)
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
    sm.max = 0
    sm.now = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
