package kvpaxos

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


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Type       string
    Key        string
    Value      string
    Id         string
    Client     string
	Mark       int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
    now        int
    data       map[string] string
    last       map[string] string
}

func (kv *KVPaxos) Wait(seq int) Op {
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

func (kv *KVPaxos) AddLog(op Op){
    for {
		//tmp, ok := kv.last[op.Client + op.Type]
		tmp, ok := kv.last[op.Client]
		if ok && tmp >= op.Id {
			return
		}
        seq := kv.now + 1
        kv.px.Start(seq, op)
        res := kv.Wait(seq)
		//fmt.Println(kv.me, op.Type, op.Client, op.Id, op.Key, op.Value)
		//fmt.Println(kv.me, res.Type, res.Client, res.Id, res.Key, res.Value)
        //kv.last[res.Client + res.Type] = res.Id
		kv.last[res.Client] = res.Id
        if res.Type == "Put" {
            kv.data[res.Key] = res.Value
        }
        if res.Type == "Append" {
			kv.data[res.Key] += res.Value
        }
        kv.px.Done(seq)
        kv.now ++
		if res.Mark == op.Mark {
		   return
        }
    }
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: "Get", Key: args.Key, Id: args.Id, Client: args.Me, Mark: rand.Int63()}
    kv.AddLog(op)
    reply.Value = kv.data[args.Key]
    return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()
    op := Op{Type: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, Client: args.Me, Mark: rand.Int63()}
    kv.AddLog(op)
    return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
    kv.now = 0
    kv.data = map[string] string{}
    kv.last = map[string] string{}

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
